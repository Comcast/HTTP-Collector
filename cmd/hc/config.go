package main

//lint:file-ignore ST1005 ignore cap error

import (
	"encoding/json"
	"flag"
	"fmt"
	"http-collector/internal/config"
	"http-collector/internal/logger"
	"http-collector/internal/mqttfederation"
	"os"

	"github.com/thedevop1/jsoncr"
)

type configuration struct {
	Verbose   bool
	Web       *config.Web
	Kafka     *config.Kafka
	MQTT      *mqttfederation.Config
	Redis     *config.Redis
	Tracing   *config.Tracing
	Limit     int64
}

func getConfig(logger *logger.Logger) (*configuration, error) {
	var (
		version    bool
		configFile = flag.String("config", os.Getenv("HC_CONFIG"), "The config filename, env: HC_CONFIG")
		verbose    = flag.Bool("verbose", false, "Turn on verbose logging")
	)
	flag.BoolVar(&version, "v", false, "Shows version and exit")
	flag.BoolVar(&version, "version", false, "Shows version and exit")
	flag.Parse()

	if version {
		fmt.Println(AppName, AppVersion, BuildTime)
		os.Exit(0)
	}

	config := &configuration{
		Web:       config.NewWeb(),
		Kafka:     config.NewKafka(),
		MQTT:      mqttfederation.NewConfig(),
		Redis:     config.NewRedis(),
		Limit:     20,
	}

	if *configFile != "" {
		file, err := os.Open(*configFile)
		if err != nil {
			return nil, err
		}
		defer file.Close()
		b, err := jsoncr.Remove(file)
		if err != nil {
			return nil, err
		}
		if err = json.Unmarshal(b, &config); err != nil {
			return nil, err
		}
	}

	flag.Visit(func(f *flag.Flag) {
		if f.Name == "verbose" {
			config.Verbose = *verbose
		}
	})

	if err := config.Web.Validate(); err != nil {
		return nil, err
	}
	if err := config.Kafka.Validate(); err != nil {
		return nil, err
	}
	if err := config.MQTT.Validate(); err != nil {
		return nil, err
	}
	if err := config.Redis.Validate(); err != nil {
		return nil, err
	}
	if err := config.Tracing.Validate(); err != nil {
		return nil, err
	}

	logger.Info("Kafka brokers", "addr", config.Kafka.Brokers)
	logger.Info("WebTLS config", "tls", config.Web.TLS)
	logger.Info("MQTT config", "mqtt", config.MQTT)
	logger.Info("KafkaTLS config", "kafkaTLS", config.Kafka.TLS)
	logger.Info("Redis config", "redis", config.Redis)

	return config, nil
}
