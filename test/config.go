package main

import (
	"flag"
	"log"
	"strings"
	"time"

	"github.com/IBM/sarama"
)

// Sarama configuration options
var (
	KafkaBrokers []string
	KafkaVersion sarama.KafkaVersion
	KafkaTopic   string
	MirrorTopic  map[string]bool
	mirrorTopic  string
	TestAgent    string
	Timeout      time.Duration
	URL          string
)

var DefaultKafkaVersion sarama.KafkaVersion = sarama.V2_1_0_0

func init() {
	var kBrokers, kVersion, timeout string
	flag.StringVar(&kBrokers, "kafkaBrokers", "", "Kafka bootstrap brokers to connect to, as a comma separated list")
	flag.StringVar(&kVersion, "kafkaVersion", "2.1.0.0", "Kafka cluster version")
	flag.StringVar(&KafkaTopic, "kafkaTopic", "", "Kafka topic to verify from")
	flag.StringVar(&mirrorTopic, "mirrorTopic", "", "Kafka mirror topic to verify from")
	flag.StringVar(&TestAgent, "testAgent", "canary-test", "Value for k8s-test header")
	flag.StringVar(&URL, "url", "", "http-collector URL")
	flag.StringVar(&timeout, "timeout", "10m", "Time to wait for verification")
	flag.Parse()

	if kBrokers == "" {
		flag.PrintDefaults()
		log.Fatalln("Missing Kafka bootstrap brokers")
	}
	KafkaBrokers = strings.Split(kBrokers, ",")

	KafkaVersion = parseKafkaVersion(kVersion)

	if KafkaTopic == "" {
		flag.PrintDefaults()
		log.Fatalln("Missing Kafka topic")
	}

	if mirrorTopic == "" {
		flag.PrintDefaults()
		log.Fatalln("Missing Kafka mirror topic")
	}

	t := strings.Split(mirrorTopic, ",")
	MirrorTopic = make(map[string]bool)
	for _, topic := range t {
		if topic == "" || topic == KafkaTopic {
			flag.PrintDefaults()
			log.Fatalln("Invalid Kafka mirror topic", mirrorTopic)
		}
		if _, exist := MirrorTopic[topic]; exist {
			flag.PrintDefaults()
			log.Fatalln("Duplicate Kafka mirror topic", mirrorTopic)
		}
		MirrorTopic[topic] = true
	}

	if URL == "" {
		flag.PrintDefaults()
		log.Fatalln("Missing http-collector URL")
	}

	if t, err := time.ParseDuration(timeout); err != nil {
		Timeout = 10 * time.Minute
	} else {
		Timeout = t
	}
}

// Helper function to ensure the version is valid
func parseKafkaVersion(s string) sarama.KafkaVersion {
	v, err := sarama.ParseKafkaVersion(s)
	if err != nil {
		return DefaultKafkaVersion
	}
	// Generic ParseKafkaVersion lacks validation, add validation here
	for _, valid := range sarama.SupportedVersions {
		if v == valid {
			return v
		}
	}
	return DefaultKafkaVersion
}
