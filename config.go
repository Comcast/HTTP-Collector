package main

import (
	"encoding/json"
	"errors"
	"flag"
	"log"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/go-redis/redis"

	"github.com/Shopify/sarama"
)

type configuration struct {
	KafkaBrokers []string
	Verbose      bool
	AggTopic     string
	Web          *http.Server
	WebTLS       TlsConfig
	Kafka        *sarama.Config
	KafkaVersion string
	KafkaTLS     TlsConfig
	Redis        RedisConfig
	Limit        int64
}

type RedisConfig struct {
	ClientConfig *redis.UniversalOptions
	Keyspace     string
	TTL          int `json:"ttl"`
	ttl          time.Duration
}

type TlsConfig struct {
	CertFile string
	KeyFile  string
	CaFile   string
	Insecure bool
}

func getConfig(logger *log.Logger) (*configuration, error) {
	var (
		addr       = flag.String("addr", os.Getenv("HC_ADDR"), "The address to bind to, ex: :8080, env: HC_ADDR")
		brokers    = flag.String("brokers", os.Getenv("KAFKA_BROKERS"), "The Kafka brokers to connect to, as a comma separated list, env: KAFKA_BROKERS")
		configFile = flag.String("config", os.Getenv("HC_CONFIG"), "The config filename, env: HC_CONFIG")
		verbose    = flag.Bool("verbose", false, "Turn on verbose logging")
	)

	flag.Parse()

	config := configuration{
		Web:   &http.Server{},
		Kafka: sarama.NewConfig(),
		Redis: RedisConfig{
			ClientConfig: &redis.UniversalOptions{},
			Keyspace:     "quota", // default keyspace to use
			TTL:          86400,   // unit in second, default 1 day TTL
		},
		Limit: 20,
	}

	if *configFile != "" {
		file, err := os.Open(*configFile)
		defer file.Close()
		if err != nil {
			return nil, err
		}

		decoder := json.NewDecoder(file)
		if err = decoder.Decode(&config); err != nil {
			return nil, err
		}
	}
	if *addr != "" {
		config.Web.Addr = *addr
	}
	if *brokers != "" {
		config.KafkaBrokers = strings.Split(*brokers, ",")
	}
	config.Verbose = *verbose

	if config.Web.Addr == "" {
		flag.PrintDefaults()
		return nil, errors.New("Missing addr")
	}
	if config.KafkaBrokers == nil {
		flag.PrintDefaults()
		return nil, errors.New("Missing Kafka brokers")
	}

	config.Redis.ttl = time.Duration(config.Redis.TTL) * time.Second

	logger.Println("Config:", config)
	logger.Println("Kafka brokers:", strings.Join(config.KafkaBrokers, ", "))
	logger.Println("WebTLS Config:", config.WebTLS)
	logger.Println("KafkaTLS Config:", config.KafkaTLS)

	return &config, nil
}
