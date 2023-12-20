package config

import (
	"errors"
	"flag"
	"os"
	"strings"

	"github.com/IBM/sarama"
)

var brokers = flag.String("brokers", os.Getenv("KAFKA_BROKERS"), "The Kafka brokers to connect to, as a comma separated list, env: KAFKA_BROKERS")

type Kafka struct {
	Brokers []string
	Sarama  *sarama.Config
	TLS     TLS
	Version string
}

func NewKafka() *Kafka {
	c := sarama.NewConfig()
	c.Version = sarama.V2_1_0_0
	c.ChannelBufferSize = 1024
	c.Consumer.Offsets.Initial = sarama.OffsetOldest
	c.Producer.Return.Successes = true

	return &Kafka{Sarama: c}
}

func (k *Kafka) Validate() error {
	if brokers != nil && *brokers != "" {
		k.Brokers = strings.Split(*brokers, ",")
	}
	if len(k.Brokers) == 0 {
		return errors.New("missing Kafka brokers")
	}
	v, err := k.ParseVersion()
	if err != nil {
		return err
	}
	k.Sarama.Version = v
	if k.Sarama.Net.TLS.Enable {
		if err := k.TLS.Validate(); err != nil {
			return err
		}
		k.Sarama.Net.TLS.Config = k.TLS.GetClientTLS()
	}
	return k.Sarama.Validate()
}

// Helper function to ensure the version is valid
func (k *Kafka) ParseVersion() (sarama.KafkaVersion, error) {
	v, err := sarama.ParseKafkaVersion(k.Version)
	if err != nil {
		return v, err
	}
	// Generic ParseKafkaVersion lacks validation, add validation here
	for _, valid := range sarama.SupportedVersions {
		if v == valid {
			return v, nil
		}
	}
	return v, errors.New("invalid Kafka version")
}
