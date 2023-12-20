package main

import (
	"bytes"
	"compress/zlib"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"http-collector/internal/mqttfederation"
	"http-collector/internal/radix"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/IBM/sarama"
	"github.com/google/uuid"
	"github.com/mochi-mqtt/server/v2/packets"
	"github.com/prometheus/client_golang/prometheus"
)

var (
	ErrTopicNotConfigured = errors.New("topic not configured")
	ErrMissingKafkaTopic  = errors.New("missing Kafka topic")
	ErrUndefinedParam     = errors.New("undefined param")
	ErrUUIDHasNoTime      = errors.New("uuid is neither v1 nor v2")
)

func (s *Server) newMqtt() {
	if !s.config.MQTT.Enable {
		return
	}

	for _, topic := range s.config.MQTT.Broker.MqttToKafka {
		metricsName := topic.Name
		if metricsName == "" {
			metricsName = "undefined"
		}
		mqttProcessor, err := NewMqttProcessor(topic, s.metrics.mqttMsg.WithLabelValues(metricsName),
			s.metrics.mqttLatency.WithLabelValues(metricsName))
		if err != nil {
			s.log.Fatal("invalid topic config", "err", err)
		}
		s.mqttToKafka.InsertTopic(topic.MqttTopic, mqttProcessor)
	}

	s.config.MQTT.Broker.MsgProcessor = s.MQTTMsgProcessor
	s.config.MQTT.Registry = prometheus.WrapRegistererWithPrefix(AppName+"_mqtt_", prometheus.DefaultRegisterer)

	m, err := mqttfederation.New(s.config.MQTT)
	if err != nil {
		s.log.Fatal("can not create MQTT Federation", "err", err)
	}
	s.mqtt = m
}

// MQTTMsgProcessor process MQTT messages, it creates Kafka message(s) and send to the Kafka asyncProducer
func (s *Server) MQTTMsgProcessor(pk packets.Packet) error {
	ctx := radix.NewTopicContext()
	_, _, mqttProcessor := s.mqttToKafka.FindTopic(ctx, pk.TopicName)
	if mqttProcessor == nil {
		s.log.Error("unable to process MQTT msg", "err", ErrTopicNotConfigured, "topic", pk.TopicName)
		s.metrics.mqttMsgErr.Inc()
		return ErrTopicNotConfigured
	}

	body, err := mqttProcessor.preparePayload(pk.Payload)
	if err != nil {
		s.log.Error("can not create payload", "err", err, "topic", pk.TopicName)
		s.metrics.mqttProcessErr.Inc()
		return err
	}

	mqttProcessor.msgCount.Inc()
	paramValues := ctx.TopicParams.Values
	for _, topic := range mqttProcessor.kafkaTopicConstructor {
		kafkaInfo, err := topic.Create(paramValues)
		if err != nil {
			s.log.Error("can not get Kafka info", "err", err, "topic", pk.TopicName)
			s.metrics.mqttProcessErr.Inc()
			return err
		}

		headers, uuid := processMqttUserProperty(pk.Properties.User)
		if uuid != "" {
			t, err := getUUIDTime(uuid)
			if err != nil {
				s.log.Error("invalid uuid in user property", "err", err, "topic", pk.TopicName, "meta", pk.Properties.User)
			} else {
				mqttProcessor.msgLatnecy.Observe(time.Since(t).Seconds())
			}
		}

		msg := createKafkaMsg(kafkaInfo.Name, kafkaInfo.Key, kafkaInfo.Rpt, headers, body)
		s.kafka.Input() <- msg
	}

	return nil
}

func (s *Server) handlePublishMQTT(pathPrefix string) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodPost, http.MethodPut:
			mqttTopic := strings.TrimPrefix(r.URL.Path, pathPrefix)
			mqttTopicParts := strings.Split(mqttTopic, "/")

			if len(mqttTopicParts) < 4 {
				s.log.Error("invalid topic", "topic", mqttTopic, "remote", r.RemoteAddr, "ua", r.UserAgent())
				http.Error(w, mqttfederation.ErrInvalidTopic.Error(), http.StatusBadRequest)
				return
			}

			body, err := io.ReadAll(r.Body)
			if err != nil {
				s.errReadBody(w, err)
				return
			}

			if mqttTopicParts[2] == "multi" { // to topic: <devicePrefix>/to/<cID>/<module>
				s.publishMQTTmultiClient(w, r, mqttTopicParts, body)
			} else {
				s.publishMQTTsingleClient(w, r, mqttTopic, body)
			}

		default:
			s.log.Info("invalid access", "method", r.Method, "path", r.URL.Path, "remote", r.RemoteAddr,
				"ua", r.UserAgent())
			http.Error(w, http.StatusText(http.StatusForbidden), http.StatusForbidden)
		}
	})
}

func (s *Server) publishMQTTmultiClient(w http.ResponseWriter, r *http.Request, parts []string, body []byte) {
	clientIDs := r.URL.Query().Get("cid")
	if clientIDs == "" {
		http.Error(w, "missing cid query param or empty clientIDs", http.StatusBadRequest)
		return
	}
	cIDs := strings.Split(clientIDs, ",")
	var issues []string
	var hasOfflineDevice bool
	for _, cID := range cIDs {
		parts[2] = cID
		mqttTopic := strings.Join(parts, "/")
		if err := s.mqtt.Publish(mqttTopic, body); err != nil {
			switch err {
			case mqttfederation.ErrNotOnline:
				hasOfflineDevice = true
				issues = append(issues, fmt.Sprintf("%s: %s", cID, err.Error()))
			case mqttfederation.ErrInvalidTopic:
				s.log.Error("invalid topic", "err", err, "topic", mqttTopic, "remote", r.RemoteAddr, "ua", r.UserAgent())
				http.Error(w, err.Error(), http.StatusBadRequest)
				return
			default:
				s.log.Error("publish error", "err", err)
				issues = append(issues, fmt.Sprintf("%s: %s", cID, err.Error()))
			}
		}
	}

	if !hasOfflineDevice && len(issues) == len(cIDs) {
		http.Error(w, http.StatusText(http.StatusInternalServerError), http.StatusInternalServerError)
		return
	} else if hasOfflineDevice || len(issues) > 0 {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusMultiStatus)
		json.NewEncoder(w).Encode(map[string]interface{}{"errors": issues})
		return
	}
	w.WriteHeader(http.StatusAccepted)
	fmt.Fprintln(w, http.StatusText(http.StatusAccepted))
}

func (s *Server) publishMQTTsingleClient(w http.ResponseWriter, r *http.Request, mqttTopic string, body []byte) {
	if err := s.mqtt.Publish(mqttTopic, body); err != nil {
		switch err {
		case mqttfederation.ErrNotOnline:
			http.Error(w, err.Error(), http.StatusNotFound)
			return
		case mqttfederation.ErrInvalidTopic:
			s.log.Error("invalid topic", "err", err, "topic", mqttTopic, "remote", r.RemoteAddr, "ua", r.UserAgent())
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		default:
			s.log.Error("publish error", "err", err)
			http.Error(w, http.StatusText(http.StatusInternalServerError), http.StatusInternalServerError)
			return
		}
	}
	w.WriteHeader(http.StatusAccepted)
	fmt.Fprintln(w, http.StatusText(http.StatusAccepted))
}

func (s Server) handleMqttRebalance() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		param := r.URL.Query().Get("max")
		max, err := strconv.Atoi(param)
		if err != nil {
			http.Error(w, "invalid parameter "+param, http.StatusBadRequest)
			return
		}

		s.mqtt.RebalanceBroker(max)
	})
}

func (s *Server) handleMqttInfo() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		info := s.mqtt.System().Clone()
		out, err := json.MarshalIndent(info, "", "\t")
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		w.WriteHeader(http.StatusOK)
		w.Write(out)
	})
}

// processMqttUserProperty Kafka header copied from MQTT user property, and uuid if exist
func processMqttUserProperty(meta []packets.UserProperty) (h []sarama.RecordHeader, uuid string) {
	for _, m := range meta {
		h = append(h, sarama.RecordHeader{
			Key:   []byte(m.Key),
			Value: []byte(m.Val),
		})

		if m.Key == "uuid" {
			uuid = m.Val
		}
	}

	return
}

// getUUIDTime returns the time from UUID v1 or v2
func getUUIDTime(v string) (t time.Time, err error) {
	id, err := uuid.Parse(v)
	if err != nil {
		return
	}

	if v := id.Version(); v != 1 && v != 2 {
		return t, ErrUUIDHasNoTime
	}

	return time.Unix(id.Time().UnixTime()), nil
}

// MqttProcess is a processor for messages in a MQTT topic.
type MqttProcessor struct {
	config                mqttfederation.MqttToKafka
	kafkaTopicConstructor KafkaTopicConstructor
	preparePayload        PreparePayload
	msgCount              prometheus.Counter
	msgLatnecy            prometheus.Observer
}

func NewMqttProcessor(config mqttfederation.MqttToKafka, msgCount prometheus.Counter, msgLatnecy prometheus.Observer) (*MqttProcessor, error) {
	kafkaTopicConstructor, err := NewKafkaTopicConstructor(config)
	if err != nil {
		return nil, err
	}

	mp := &MqttProcessor{
		config:                config,
		kafkaTopicConstructor: kafkaTopicConstructor,
		preparePayload:        SelectPreparePayload(config),
		msgCount:              msgCount,
		msgLatnecy:            msgLatnecy,
	}

	return mp, nil
}

// KafkaTopicConstructor contains all the Kafka constructs for an MQTT topic
type KafkaTopicConstructor []KafkaTopicConstruct

// KafkaTopicConstruct contains the info to covert a MQTT topic to Kafka topic name/key and header
type KafkaTopicConstruct struct {
	Name         string
	NameValueIdx []int
	Key          string
	KeyIdx       []int
	Rpt          string
}

type KafkaTopicInfo struct {
	Name string
	Key  string
	Rpt  string
}

func NewKafkaTopicConstructor(config mqttfederation.MqttToKafka) (KafkaTopicConstructor, error) {
	mqttParams, err := radix.PatParamKeys(config.MqttTopic)
	if err != nil {
		return nil, err
	}

	kafkaTopicConstructor := make([]KafkaTopicConstruct, len(config.KafkaTopics))
	for i, kafkaTopic := range config.KafkaTopics {
		name := kafkaTopic.Name
		if name == "" {
			return nil, ErrMissingKafkaTopic
		}
		var nameValueIdx []int
		kafkaParams, err := radix.PatParamKeys(kafkaTopic.Name)
		if err != nil {
			return nil, err
		}
		for _, p := range kafkaParams {
			index, ok := findParamIndex(mqttParams, p)
			if !ok {
				return nil, ErrUndefinedParam
			}
			name = strings.Replace(name, "{"+p+"}", "%v", 1)
			nameValueIdx = append(nameValueIdx, index)
		}

		key := kafkaTopic.Key
		var keyIdx []int
		keyParams, err := radix.PatParamKeys(kafkaTopic.Key)
		if err != nil {
			return nil, err
		}
		for _, p := range keyParams {
			index, ok := findParamIndex(mqttParams, p)
			if !ok {
				return nil, ErrUndefinedParam
			}
			key = strings.Replace(key, "{"+p+"}", "%v", 1)
			keyIdx = append(keyIdx, index)
		}

		var rpt string
		if !kafkaTopic.DisableHeader {
			rpt = config.Name
		}

		kafkaTopicConstruct := KafkaTopicConstruct{
			Name:         name,
			NameValueIdx: nameValueIdx,
			Key:          key,
			KeyIdx:       keyIdx,
			Rpt:          rpt,
		}
		kafkaTopicConstructor[i] = kafkaTopicConstruct
	}

	return kafkaTopicConstructor, nil
}

// Create the Kafka topic name based on params
func (k *KafkaTopicConstruct) Create(params []string) (KafkaTopicInfo, error) {
	l := len(params) - 1
	var nameValues []any
	for _, v := range k.NameValueIdx {
		if v > l { // should not happen
			return KafkaTopicInfo{}, errors.New("out of index")
		}
		nameValues = append(nameValues, params[v])
	}
	name := fmt.Sprintf(k.Name, nameValues...)
	var keyValues []any
	for _, v := range k.KeyIdx {
		if v > l { // should not happen
			return KafkaTopicInfo{}, errors.New("out of index")
		}
		keyValues = append(keyValues, params[v])
	}
	key := fmt.Sprintf(k.Key, keyValues...)
	return KafkaTopicInfo{Name: name, Key: key, Rpt: k.Rpt}, nil
}

type PreparePayload func([]byte) ([]byte, error)

func SelectPreparePayload(config mqttfederation.MqttToKafka) PreparePayload {
	switch config.Decompress {
	case true:
		if config.DisableBase64 {
			return decompressPayload
		}
		return decompressBase64Payload
	default:
		if config.DisableBase64 {
			return copyPayload
		}
		return base64Payload
	}
}

// decompressPayload returns the decompressed input using zlib, Plume payloads are zlib compressed
func decompressPayload(input []byte) ([]byte, error) {
	if input == nil {
		return nil, nil
	}
	b := bytes.NewReader(input)
	r, err := zlib.NewReader(b)
	if err != nil {
		return nil, err
	}
	defer r.Close()
	out, err := io.ReadAll(r)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func copyPayload(input []byte) ([]byte, error) {
	if input == nil {
		return nil, nil
	}
	out := make([]byte, len(input))
	copy(out, input)
	return out, nil
}

// base64Payload returns base64 encoded input, legacy Plume reason to base64 encode payload
func base64Payload(input []byte) ([]byte, error) {
	if input == nil {
		return nil, nil
	}
	body := make([]byte, base64.StdEncoding.EncodedLen(len(input)))
	base64.StdEncoding.Encode(body, input)
	return body, nil
}

// decompressBase64Payload returns decompressed and base64 encoded input, should not be used
func decompressBase64Payload(input []byte) ([]byte, error) {
	if input == nil {
		return nil, nil
	}
	data, err := decompressPayload(input)
	if err != nil {
		return nil, err
	}
	return base64Payload(data)
}

func findParamIndex(mqttParams []string, p string) (int, bool) {
	for i, src := range mqttParams {
		if p == src {
			return i, true
		}
	}
	return -1, false
}

func KafkaTopic(ctx radix.Context, topic string, index []int) string {
	var values []any
	for _, n := range index {
		values = append(values, ctx.TopicParams.Values[n])
	}
	return fmt.Sprintf(topic, values...)
}
