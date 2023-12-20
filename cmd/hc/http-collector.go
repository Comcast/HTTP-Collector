package main

import (
	_ "go.uber.org/automaxprocs"

	"context"
	"encoding/json"
	"errors"
	"fmt"
	"http-collector/internal/kafkapartitioner"
	"http-collector/internal/logger"
	"http-collector/internal/logger/weblog"
	"http-collector/internal/mqttfederation"
	"http-collector/internal/radix"
	"http-collector/internal/tracing"
	"io"
	"log"
	"log/slog"
	"math/rand"
	"net/http"
	"net/http/pprof"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/IBM/sarama"
	"github.com/go-redis/redis/v7"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/rcrowley/go-metrics"
)

var (
	// AppName overide the following through ldflags="-X <package>.<varName>=<value>"
	AppName    = "hc"
	AppVersion = "unknown"
	BuildTime  = "unkown"
)

const (
	apiPathV2      string = "/v2/nolim/"
	apiPathV2Limit string = "/v2/limit/"
	apiMqttPub     string = "/v2/mqtt/pub/"
)

// A Server holds all the servers and configurations
type Server struct {
	config      *configuration
	router      *http.ServeMux
	web         *http.Server
	mqtt        *mqttfederation.Server
	mqttToKafka *radix.Node[*MqttProcessor]
	kafka       sarama.AsyncProducer
	redis       redis.UniversalClient
	log     *logger.Logger
	metrics *appMetrics
}

func main() {
	logger := logger.NewLogger()
	slog.SetDefault(logger.Logger)

	logger.Info("Server is starting...")

	config, err := getConfig(logger)
	if err != nil {
		logger.Fatal("invalid config", "err", err)
	}

	if config.Verbose {
		logger.SetLevel(slog.LevelDebug)
	}

	s := &Server{
		config:      config,
		router:      http.NewServeMux(),
		mqttToKafka: radix.New[*MqttProcessor](),
		log:         logger,
		metrics:     newMetrics(),
	}
	s.newWebServer()
	s.newMqtt()

	s.kafka = s.newKafkaProducer()
	defer func() {
		if err := s.kafka.Close(); err != nil {
			s.log.Error("failed to shut down Kafka async producer cleanly", "err", err)
		}
	}()

	if len(s.config.Redis.ClientConfig.Addrs) > 0 {
		s.redis = s.newRedisClient()
		defer func() {
			if err := s.redis.Close(); err != nil {
				s.log.Error("failed to shut down Redis client cleanly", "err", err)
			}
		}()
	} else {
		s.redis = nil
	}

	s.addRoutes()

	tracing.SetTracingConfig(AppName, s.config.Tracing.AppID)

	done := make(chan bool)
	s.RegisterGracefulShutdown(done)
	s.StartMQTT()
	s.StartWebServer()
	<-done
	logger.Info("all other servers are shutting down...")
}

func (s *Server) newWebServer() {
	var t http.Handler = s.router // inner most, called last

	if s.config.Verbose {
		t = weblog.WebLog(s.log, t)
	}

	t = s.webMetrics(s.metrics, t)

	if maxBytes := s.config.Web.MaxBodySize; maxBytes > 0 { // outter most, called first
		t = http.MaxBytesHandler(t, maxBytes)
	}

	s.config.Web.Server.Handler = t
	s.web = s.config.Web.Server
}

func (s *Server) newRedisClient() redis.UniversalClient {
	return redis.NewUniversalClient(s.config.Redis.ClientConfig)
}

func (s *Server) newKafkaProducer() sarama.AsyncProducer {
	if s.config.Verbose {
		sarama.Logger = log.New(os.Stdout, "[sarama] ", log.LstdFlags)
	}

	s.config.Kafka.Sarama.MetricRegistry = metrics.NewPrefixedChildRegistry(s.metrics.saramaRegistry, "sarama.")

	if s.config.Kafka.Sarama.Net.TLS.Enable {
		s.config.Kafka.Sarama.Net.TLS.Config = s.config.Kafka.TLS.GetClientTLS()
	}

	// Mesh MQTT topics must use Murmur2 hash, its consumer is hardcoded to use this
	var murmur2Topics []string
	for _, mqttToKafka := range s.config.MQTT.Broker.MqttToKafka {
		for _, topic := range mqttToKafka.KafkaTopics {
			murmur2Topics = append(murmur2Topics, topic.Name)
		}
	}
	s.config.Kafka.Sarama.Producer.Partitioner = kafkapartitioner.NewPartitionerConstructor(murmur2Topics)

	// forcing return error/success for metrics and use of memory pool
	s.config.Kafka.Sarama.Producer.Return.Errors = true
	s.config.Kafka.Sarama.Producer.Return.Successes = true
	s.log.Info("Kafka config", "kafka", s.config.Kafka)

	producer, err := sarama.NewAsyncProducer(s.config.Kafka.Brokers, s.config.Kafka.Sarama)
	if err != nil {
		s.log.Fatal("failed to start Sarama producer", "err", err)
	}

	// Note: messages will only be returned here after all retry attempts are exhausted.
	go func() {
		for err := range producer.Errors() {
			s.metrics.pFail.Inc()
			s.log.Error("failed to write to Kafka", "err", err)
		}
	}()

	go func() {
		for range producer.Successes() {
			s.metrics.pSuc.Inc()
		}
	}()
	return producer
}

func (s *Server) addRoutes() *http.ServeMux {
	s.router.Handle(apiPathV2, s.handleMsg(apiPathV2))
	if len(s.config.Redis.ClientConfig.Addrs) > 0 {
		s.router.Handle(apiPathV2Limit, s.handleMsgLimit(apiPathV2Limit))
	}
	if s.config.MQTT.Enable {
		s.router.Handle(apiMqttPub, s.handlePublishMQTT(apiMqttPub))
		s.router.Handle("/mqttrebalance", s.handleMqttRebalance())
		s.router.Handle("/mqttinfo", s.handleMqttInfo())
	}
	s.router.Handle("/", s.handleDefault())
	s.router.Handle("/metrics", s.handleMetrics())
	s.router.Handle("/healthz", s.handleHealthz())
	s.router.Handle("/version", s.handleVersion())

	// temp turn on profile
	s.router.HandleFunc("/debug/pprof/", pprof.Index)
	s.router.HandleFunc("/debug/pprof/cmdline", pprof.Cmdline)
	s.router.HandleFunc("/debug/pprof/profile", pprof.Profile)
	s.router.HandleFunc("/debug/pprof/symbol", pprof.Symbol)
	s.router.HandleFunc("/debug/pprof/trace", pprof.Trace)
	return s.router
}

func (s *Server) RegisterGracefulShutdown(done chan bool) {
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, os.Interrupt, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		<-quit
		if s.config.MQTT.Enable {
			s.log.Info("MQTT server is shutting down...")
			s.mqtt.Shutdown()
		}

		s.log.Info("Web server is shutting down...")

		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()

		s.web.SetKeepAlivesEnabled(false)
		if err := s.web.Shutdown(ctx); err != nil {
			s.log.Fatal("can not gracefully shutdown the server", "err", err)
		}
		close(done)
	}()
}

func (s *Server) StartMQTT() {
	if s.config.MQTT.Enable {
		s.log.Info("starting MQTT")
		go s.mqtt.Start()
	}
}

func (s *Server) StartWebServer() {
	s.log.Info("server is ready to handle requests", "addr", s.web.Addr)

	if tls := s.config.Web.TLS; tls.CertFile != "" && tls.KeyFile != "" {
		if err := s.web.ListenAndServeTLS(tls.CertFile, tls.KeyFile); err != http.ErrServerClosed {
			s.log.Fatal("can not listen on", "addr", s.web.Addr, "err", err)
		}
	} else {
		if err := s.web.ListenAndServe(); err != http.ErrServerClosed {
			s.log.Fatal("can not listen on", "addr", s.web.Addr, "err", err)
		}
	}
}

func (s *Server) handleDefault() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		s.log.Info("invalid access", "method", r.Method, "path", r.URL.Path, "remote", r.RemoteAddr,
			"ua", r.UserAgent())
		http.Error(w, http.StatusText(http.StatusForbidden), http.StatusForbidden)
	})
}

func (s *Server) handleMetrics() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		s.metrics.promMutex.Lock()
		s.metrics.promClient.UpdatePrometheusMetricsOnce()
		s.metrics.promMutex.Unlock()
		promhttp.Handler().ServeHTTP(w, r)
	})
}

func (s *Server) handleHealthz() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		fmt.Fprintln(w, http.StatusText(http.StatusOK))
	})
}

func (s *Server) handleVersion() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		version := struct {
			Data struct {
				Version   string
				BuildTime string
			} `json:"data"`
		}{
			Data: struct {
				Version   string
				BuildTime string
			}{
				Version:   AppName + "-" + AppVersion,
				BuildTime: BuildTime,
			},
		}
		output, _ := json.Marshal(version)
		w.Header().Set("Content-type", "application/json")
		w.WriteHeader(http.StatusOK)
		w.Write(output)
	})
}

func (s *Server) handleMsg(pathPrefix string) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodPost, http.MethodPut:
			topic := strings.TrimPrefix(r.URL.Path, pathPrefix) // Should validate topic
			key := getKey(r)
			mirrors := r.Header.Values("x-mirror") // requires Go 1.14 or later
			body, err := io.ReadAll(r.Body)
			if err != nil {
				s.errReadBody(w, err)
				return
			}
			if l := len(body); l > s.config.Kafka.Sarama.Producer.MaxMessageBytes {
				s.log.Error("body size too large", "size", l)
				http.Error(w, http.StatusText(http.StatusRequestEntityTooLarge), http.StatusRequestEntityTooLarge)
				return
			}
			rpt := r.Header.Get("x-rpt")
			headers := tracing.GetTraceHeaders(r, false)
			s.kafka.Input() <- createKafkaMsg(topic, key, rpt, headers, body)
			for _, v := range mirrors {
				if mirror := strings.TrimSpace(v); mirror != "" {
					s.kafka.Input() <- createKafkaMsg(mirror, key, rpt, headers, body)
					s.metrics.mirror.With(prometheus.Labels{"topic": mirror}).Inc()
				}
			}
			w.WriteHeader(http.StatusAccepted)
			fmt.Fprintln(w, http.StatusText(http.StatusAccepted))
		default:
			s.log.Info("invalid access", "method", r.Method, "path", r.URL.Path, "remote", r.RemoteAddr,
				"ua", r.UserAgent())
			http.Error(w, http.StatusText(http.StatusForbidden), http.StatusForbidden)
		}
	})
}

func getKey(r *http.Request) string {
	key := r.Header.Get("X-key")
	key = strings.TrimPrefix(key, "mac:") // strip away mac: from webpa reports
	prefix := r.Header.Get("X-key-prefix")
	if prefix != "" {
		if key == "" { // generate a random number if no key, no need to seed
			key = prefix + "-" + strconv.Itoa(int(rand.Int31())) // rand at most 10 digits, mac at least 12 characters
		} else {
			key = prefix + "-" + key
		}
	}
	return key
}

func (s *Server) handleMsgLimit(pathPrefix string) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodPost, http.MethodPut:
			if s.redis == nil {
				http.Error(w, http.StatusText(http.StatusForbidden), http.StatusForbidden)
				return
			}
			key := getKey(r)
			if s.overLimit(key) {
				w.WriteHeader(http.StatusAccepted)
				fmt.Fprintln(w, http.StatusText(http.StatusAccepted))
			} else {
				topic := strings.TrimPrefix(r.URL.Path, pathPrefix) // Should validate topic
				mirrors := r.Header.Values("x-mirror") // requires Go 1.14 or later
				body, err := io.ReadAll(r.Body)
				if err != nil {
					s.errReadBody(w, err)
					return
				}
				if l := len(body); l > s.config.Kafka.Sarama.Producer.MaxMessageBytes {
					s.log.Error("body size too large", "size", l)
					http.Error(w, http.StatusText(http.StatusRequestEntityTooLarge), http.StatusRequestEntityTooLarge)
					return
				}
				rpt := r.Header.Get("x-rpt")
				headers := tracing.GetTraceHeaders(r, false)
				s.kafka.Input() <- createKafkaMsg(topic, key, rpt, headers, body)
				for _, mirror := range mirrors {
					if mirror != "" {
						s.kafka.Input() <- createKafkaMsg(mirror, key, rpt, nil, body)
						s.metrics.mirror.With(prometheus.Labels{"topic": mirror}).Inc()
					}
				}
				w.WriteHeader(http.StatusAccepted)
				fmt.Fprintln(w, http.StatusText(http.StatusAccepted))
			}
		default:
			s.log.Info("invalid access", "method", r.Method, "path", r.URL.Path, "remote", r.RemoteAddr,
				"ua", r.UserAgent())
			http.Error(w, http.StatusText(http.StatusForbidden), http.StatusForbidden)
		}
	})
}

func (s *Server) overLimit(key string) bool {
	day := strconv.FormatInt(time.Now().Unix()/3600/24, 10) // for high volume change it to centralized time
	redisKey := s.config.Redis.Keyspace + ":" + day + ":" + key
	pipe := s.redis.Pipeline()
	count := pipe.Incr(redisKey)
	pipe.Expire(redisKey, s.config.Redis.GetTTL())
	_, err := pipe.Exec()
	if err != nil {
		s.log.Error("error increment redis", "err", err)
		return false
	}
	return count.Val() > s.config.Limit
}

func (s *Server) errReadBody(w http.ResponseWriter, err error) {
	s.log.Error("error reading body", "err", err)

	var maxBytesError *http.MaxBytesError
	if errors.As(err, &maxBytesError) {
		http.Error(w, http.StatusText(http.StatusRequestEntityTooLarge), http.StatusRequestEntityTooLarge)
		return
	}

	http.Error(w, http.StatusText(http.StatusBadRequest), http.StatusBadRequest)
}

// createKafkaMsg creates a Kafka message. topic, key, headers and value correspond to the same type in Kafka.
// If rpt non-empty, its value is added to Kafka header with Key="rpt".
func createKafkaMsg(topic, key, rpt string, headers []sarama.RecordHeader, value []byte) *sarama.ProducerMessage {
	var msgKey sarama.Encoder
	if key != "" {
		msgKey = sarama.StringEncoder(key)
	}

	if rpt != "" {
		headers = append(headers, sarama.RecordHeader{
			Key:   []byte("rpt"),
			Value: []byte(rpt),
		})
	}

	return &sarama.ProducerMessage{
		Topic:     topic,
		Key:       msgKey,
		Value:     sarama.ByteEncoder(value),
		Timestamp: time.Now(),
		Headers:   headers,
	}
}
