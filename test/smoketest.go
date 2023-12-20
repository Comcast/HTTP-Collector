package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"crypto/tls"
	"encoding/hex"
	"io"
	"io/ioutil"
	"log"
	"math/rand"
	"net"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/IBM/sarama"
	"github.com/google/uuid"
	"google.golang.org/protobuf/proto"
)

var (
	Large     = RandomData(5000)
	Med       = RandomData(700)
	Small     = RandomData(100)
	LargeSHA  = Sha256(Large)
	MedSHA    = Sha256(Med)
	SmallSHA  = Sha256(Small)
	TestCases []Payload
)

const UserAgent = "hc_smoketest"

type Server struct {
	consumer sarama.Consumer
	hClient  *http.Client
	*log.Logger
}

func init() {
	log.Println("Topic:", KafkaTopic)
	TestCases = []Payload{
		{XkeyPrefix: "myPrefix", Xkey: "myKey", Topic: KafkaTopic, Mirror: MirrorTopic, Data: Med, Sha: MedSHA},
		{XkeyPrefix: "myPrefix", Xkey: "mac:myKey", Topic: KafkaTopic, Mirror: MirrorTopic, Data: Med, Sha: MedSHA},
		{XkeyPrefix: "myPrefix", Topic: KafkaTopic, Mirror: MirrorTopic, Data: Med, Sha: MedSHA},
		{Xkey: "myKey", Topic: KafkaTopic, Mirror: MirrorTopic, Data: Large, Sha: LargeSHA},
		{Xkey: "myKey", Topic: KafkaTopic, Mirror: MirrorTopic, Data: Med, Sha: MedSHA},
		{Xkey: "mac:myKey", Topic: KafkaTopic, Mirror: MirrorTopic, Data: Med, Sha: MedSHA},
		{Xkey: "myKey", Topic: KafkaTopic, Mirror: MirrorTopic, Data: Small, Sha: SmallSHA},
		{Topic: KafkaTopic, Mirror: MirrorTopic, Data: Med, Sha: MedSHA},
		// Test same above with report type
		{XkeyPrefix: "myPrefix", Xkey: "myKey", Topic: KafkaTopic, Mirror: MirrorTopic, Data: Med, Sha: MedSHA, Rpt: "smoketest"},
		{XkeyPrefix: "myPrefix", Xkey: "mac:myKey", Topic: KafkaTopic, Mirror: MirrorTopic, Data: Med, Sha: MedSHA, Rpt: "smoketest"},
		{XkeyPrefix: "myPrefix", Topic: KafkaTopic, Mirror: MirrorTopic, Data: Med, Sha: MedSHA, Rpt: "smoketest"},
		{Xkey: "myKey", Topic: KafkaTopic, Mirror: MirrorTopic, Data: Large, Sha: LargeSHA, Rpt: "smoketest"},
		{Xkey: "myKey", Topic: KafkaTopic, Mirror: MirrorTopic, Data: Med, Sha: MedSHA, Rpt: "smoketest"},
		{Xkey: "mac:myKey", Topic: KafkaTopic, Mirror: MirrorTopic, Data: Med, Sha: MedSHA, Rpt: "smoketest"},
		{Xkey: "myKey", Topic: KafkaTopic, Mirror: MirrorTopic, Data: Small, Sha: SmallSHA, Rpt: "smoketest"},
		{Topic: KafkaTopic, Mirror: MirrorTopic, Data: Med, Sha: MedSHA, Rpt: "smoketest"},
	}
}

func main() {
	logger := log.New(os.Stdout, "[hc-smoke] ", log.LstdFlags)
	s := &Server{hClient: newHClient(), Logger: logger}
	ctx, cancel := context.WithTimeout(context.Background(), Timeout)
	defer cancel()
	consumer, err := newKafkaConsumer()
	if err != nil {
		s.Panicln("Unable create Kafka consumer", err)
	}
	s.consumer = consumer
	defer func() {
		consumer.Close()
	}()

	fromKafka := make(chan *KafkaMsg, 100)
	s.fromKafka(ctx, KafkaTopic, fromKafka)
	for topic := range MirrorTopic {
		s.fromKafka(ctx, topic, fromKafka)
	}

	toHC := make(chan *Payload, 100)
	go s.toHC(ctx, toHC)

	s.verify(ctx, fromKafka, toHC)
}

func (s *Server) verify(ctx context.Context, fromKafka <-chan *KafkaMsg, toHC <-chan *Payload) {
	var done bool
	toVerify := make(map[string]*Payload)
	for {
		select {
		case kafkaMsg := <-fromKafka:
			mapKey := hex.EncodeToString(kafkaMsg.payload.Uuid) + ":" + kafkaMsg.kafkaTopic
			org, ok := toVerify[mapKey]
			if !ok {
				s.Println("doesn't exist", kafkaMsg.payload.Uuid)
				break
			}
			if !bytes.Equal(kafkaMsg.payload.Sha, org.Sha) {
				s.Fatalln("Msg doesn't match", kafkaMsg.payload, org)
			}
			// Topic can be different due to mirror, encode into map key instead

			xkey := strings.TrimPrefix(kafkaMsg.payload.Xkey, "mac:")
			if kafkaMsg.payload.XkeyPrefix != "" {
				if kafkaMsg.payload.Xkey == "" {
					z := strings.Split(kafkaMsg.kafkaKey, "-")
					if len(z) < 2 {
						s.Fatal("Key doesn't match ", kafkaMsg.kafkaKey, ", ", kafkaMsg.payload.Xkey)
					}
					randNum := z[len(z)-1]
					if _, err := strconv.Atoi(randNum); err != nil {
						s.Fatal("Key doesn't match ", kafkaMsg.kafkaKey, ", ", kafkaMsg.payload.Xkey)
					}
					if key := kafkaMsg.payload.XkeyPrefix + "-" + randNum; kafkaMsg.kafkaKey != key {
						s.Fatal("Key doesn't match ", kafkaMsg.kafkaKey, ", ", key)
					}
				} else if key := kafkaMsg.payload.XkeyPrefix + "-" + xkey; kafkaMsg.kafkaKey != key {
					s.Fatal("Key doesn't match ", kafkaMsg.kafkaKey, ", ", key)
				}
			} else if kafkaMsg.kafkaKey != xkey {
				s.Fatal("Key doesn't match ", kafkaMsg.kafkaKey, ", ", xkey)
			}
			if org.Rpt != "" {
				found := false
				for _, h := range kafkaMsg.kafkaHeader {
					if string(h.Key) != "rpt" {
						continue
					}
					if string(h.Value) != org.Rpt {
						break
					}
					found = true
					break
				}
				if !found {
					s.Fatal("Kafka header doesn't match ", kafkaHeaderToString(kafkaMsg.kafkaHeader), " ", org.Rpt)
				}
			}
			delete(toVerify, mapKey)
			sentUUID := uuid.UUID{}
			sentUUID.UnmarshalBinary(org.Uuid)
			sentTime := time.Unix(sentUUID.Time().UnixTime())
			s.Printf("Verified msg topic: %s, key: %s (%s, %s), %v, %v, (%v), %v",
				kafkaMsg.kafkaTopic, kafkaMsg.kafkaKey, org.XkeyPrefix, org.Xkey, kafkaMsg.kafkaTimestamp,
				sentTime, kafkaMsg.kafkaTimestamp.Sub(sentTime), kafkaHeaderToString(kafkaMsg.kafkaHeader))
		case payload, ok := <-toHC:
			if !ok {
				s.Println("Done sending")
				done = true
				toHC = make(chan *Payload, 1)
				break
			}
			toVerify[hex.EncodeToString(payload.Uuid)+":"+payload.Topic] = payload
			for mirror := range payload.Mirror {
				toVerify[hex.EncodeToString(payload.Uuid)+":"+mirror] = payload
			}
		case <-ctx.Done():
			s.Println("Context done")
			s.Println("Not verified msg: ", len(toVerify))
			for k, v := range toVerify {
				s.Println(k, v.Topic, v.XkeyPrefix, v.Xkey)
			}
			return
		}
		if done && len(toVerify) == 0 {
			s.Println("Verification passed")
			return
		}
	}
}

func kafkaHeaderToString(header []*sarama.RecordHeader) (h []string) {
	for _, v := range header {
		h = append(h, string(v.Key)+":"+string(v.Value))
	}
	return
}

func (s *Server) toHC(ctx context.Context, toHC chan<- *Payload) {
	s.Println("Sending requests to HC")
	defer close(toHC)
	var wg sync.WaitGroup
	url := URL + KafkaTopic
	for index := range TestCases {
		payload := proto.Clone(&TestCases[index]).(*Payload)
		uuid, err := uuid.NewUUID() // Version 1 with timestamp
		if err != nil {
			s.Fatalln("Unable to create UUID", err)
		}
		payload.Uuid, err = uuid.MarshalBinary()
		if err != nil {
			s.Fatalln("Unable to marshal UUID", err)
		}
		toHC <- payload
		wg.Add(1)
		go func() {
			defer wg.Done()
			data, err := proto.Marshal(payload)
			if err != nil {
				s.Fatalln("Unable to marshal", err)
			}
			req, err := http.NewRequestWithContext(ctx, "POST", url, bytes.NewBuffer(data))
			if err != nil {
				s.Fatalln("Unable to create request", err)
			}
			req.Header.Set("User-Agent", UserAgent)
			req.Header.Set("k8s-test", TestAgent)
			req.Header.Set("Content-type", "application/x-protobuf")
			if payload.XkeyPrefix != "" {
				req.Header.Set("X-key-prefix", payload.XkeyPrefix)
			}
			if payload.Xkey != "" {
				req.Header.Set("X-key", payload.Xkey)
			}
			if payload.Rpt != "" {
				req.Header.Add("X-rpt", payload.Rpt)
			}
			for mirror := range MirrorTopic {
				req.Header.Add("X-mirror", mirror)
			}
			res, err := s.hClient.Do(req)
			if err != nil || res.StatusCode != 202 {
				s.Fatalln("Failed POST to http-collector", err)
			}
			defer res.Body.Close()
			io.Copy(ioutil.Discard, res.Body)
		}()
	}
	wg.Wait()
}

func (s *Server) fromKafka(ctx context.Context, topic string, fromKafka chan<- *KafkaMsg) {
	s.Println("Start to consume from Kafka", topic)
	partitions, err := s.consumer.Partitions(topic)
	if err != nil {
		s.Fatalln("Unable to get partitions", err)
	}
	for _, part := range partitions {
		consumer, err := s.consumer.ConsumePartition(topic, part, sarama.OffsetNewest)
		if err != nil {
			s.Fatalln("Unable to consume partitions", topic, part, err)
		}
		go func(part int32) {
			for {
				select {
				case msg := <-consumer.Messages():
					payload := &Payload{}
					if proto.Unmarshal(msg.Value, payload) != nil {
						break
					}
					fromKafka <- &KafkaMsg{
						payload:        payload,
						kafkaKey:       string(msg.Key),
						kafkaTopic:     msg.Topic,
						kafkaHeader:    msg.Headers,
						kafkaTimestamp: msg.Timestamp,
						kafkaBlockTime: msg.BlockTimestamp,
					}
				case <-ctx.Done():
					return
				}
			}
		}(part)
	}
}

func newKafkaConsumer() (sarama.Consumer, error) {
	config := sarama.NewConfig()
	config.ClientID = "hc-smoketest"
	config.Version = KafkaVersion
	config.Consumer.Offsets.AutoCommit.Enable = false
	config.Consumer.Offsets.Initial = sarama.OffsetNewest

	return sarama.NewConsumer(KafkaBrokers, config)
}

func RandomData(size int) []byte {
	t := make([]byte, size)
	if _, err := rand.Read(t); err != nil {
		log.Panicln("Unable to generate data")
	}
	return t
}

func Sha256(b []byte) []byte {
	h := sha256.New()
	h.Write(b)
	return h.Sum(nil)
}

func newHClient() *http.Client {
	return &http.Client{
		Transport: &http.Transport{
			Proxy: http.ProxyFromEnvironment,
			DialContext: (&net.Dialer{
				Timeout:   30 * time.Second,
				KeepAlive: 30 * time.Second,
				DualStack: true,
			}).DialContext,
			ForceAttemptHTTP2:     true,
			DisableKeepAlives:     true, // Disable keepalive for smoketest
			MaxIdleConnsPerHost:   100,
			IdleConnTimeout:       30 * time.Second,
			TLSHandshakeTimeout:   10 * time.Second,
			ExpectContinueTimeout: 1 * time.Second,
			// add TLS
			TLSClientConfig: &tls.Config{
				InsecureSkipVerify: true,
			},
		},
		Timeout: 5 * time.Second,
	}
}

type KafkaMsg struct {
	payload        *Payload
	kafkaKey       string
	kafkaTopic     string
	kafkaHeader    []*sarama.RecordHeader
	kafkaTimestamp time.Time
	kafkaBlockTime time.Time
}
