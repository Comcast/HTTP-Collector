package main

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"io/ioutil"
	"log"
	"math/rand"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/Shopify/sarama"
	"github.com/go-redis/redis/v7"
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
)

// A Server holds all the servers and configurations
type Server struct {
	config *configuration
	router *http.ServeMux
	web    *http.Server
	kafka  sarama.AsyncProducer
	redis  redis.UniversalClient
	*log.Logger
	metrics *appMetrics
}

type appMetric struct {
	registry metrics.Registry
	pSuc     metrics.Meter
	pFail    metrics.Meter
	rptRate  metrics.Meter
}

func main() {
	logger := log.New(os.Stdout, "[hc] ", log.LstdFlags)

	logger.Println("Server is starting...")

	config, err := getConfig(logger)
	if err != nil {
		logger.Fatalln(err)
	}

	server := &Server{
		config:  config,
		router:  http.NewServeMux(),
		Logger:  logger,
		metrics: newMetrics(),
	}
	server.newWebServer()

	server.kafka = server.newKafkaProducer()
	defer func() {
		if err := server.kafka.Close(); err != nil {
			logger.Println("Failed to shut down Kafka async producer cleanly", err)
		}
	}()

	if len(server.config.Redis.ClientConfig.Addrs) > 0 {
		server.redis = server.newRedisClient()
		defer func() {
			if err := server.redis.Close(); err != nil {
				logger.Println("Failed to shut down Redis client cleanly", err)
			}
		}()
	} else {
		server.redis = nil
	}

	server.addRoutes()

	done := make(chan bool)
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, os.Interrupt, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-quit
		logger.Println("Web server is shutting down...")

		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()

		server.web.SetKeepAlivesEnabled(false)
		if err := server.web.Shutdown(ctx); err != nil {
			logger.Fatalf("Could not gracefully shutdown the server: %v\n", err)
		}
		close(done)
	}()

	logger.Println("Server is ready to handle requests at", server.web.Addr)

	if err := server.web.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		logger.Fatalf("Could not listen on %s: %v\n", server.web.Addr, err)
	}

	<-done
	logger.Println("All other servers are shutting down...")
}

func (s *Server) newWebServer() {
	var t http.Handler
	t = webMetrics(s.metrics, s.router)

	if s.config.Verbose {
		t = webLogging(s.Logger, t)
	}
	s.config.Web.Handler = t
	s.config.Web.ErrorLog = s.Logger
	s.Println("Web Config:", s.config.Web)
	s.web = s.config.Web
}

func (s *Server) newRedisClient() redis.UniversalClient {
	return redis.NewUniversalClient(s.config.Redis.ClientConfig)
}

func (s *Server) addRoutes() *http.ServeMux {
	s.router.Handle(apiPathV2, s.handleMsg(apiPathV2))
	s.router.Handle(apiPathV2Limit, s.handleMsgLimit(apiPathV2Limit))
	s.router.Handle("/", s.handleDefault())
	s.router.Handle("/metrics", s.handleMetrics())
	s.router.Handle("/healthz", s.handleHealthz())
	s.router.Handle("/version", s.handleVersion())
	return s.router
}

func (s *Server) handleDefault() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		s.Println("Invalid access: ", r.Method, r.URL.Path, r.RemoteAddr, r.UserAgent())
		http.Error(w, http.StatusText(http.StatusForbidden), http.StatusForbidden)
	})
}

func (s *Server) handleMetrics() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		s.metrics.promClient.UpdatePrometheusMetricsOnce()
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
		w.WriteHeader(http.StatusOK)
		fmt.Fprintf(w, "%v, %v, %v", AppName, AppVersion, BuildTime)
	})
}
func (s *Server) handleMsg(pathPrefix string) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodPost, http.MethodPut:
			key := getKey(r)
			topic := strings.TrimPrefix(r.URL.Path, pathPrefix)
			body, err := ioutil.ReadAll(r.Body)
			if err != nil {
				s.Printf("Error reading body: %v", err)
				http.Error(w, http.StatusText(http.StatusBadRequest), http.StatusBadRequest)
				return
			} else if l := len(body); l > s.config.Kafka.Producer.MaxMessageBytes {
				s.Printf("Body size too large: %d", l)
				http.Error(w, http.StatusText(http.StatusRequestEntityTooLarge), http.StatusRequestEntityTooLarge)
				return
			}
			s.kafka.Input() <- createMessage(topic, key, body)
			w.WriteHeader(http.StatusAccepted)
			fmt.Fprintln(w, http.StatusText(http.StatusAccepted))
		default:
			http.Error(w, http.StatusText(http.StatusForbidden), http.StatusForbidden)
		}
	})
}

func getKey(r *http.Request) string {
	key := r.Header.Get("X-key")
	prefix := r.Header.Get("X-key-prefix")
	if prefix != "" {
		if key == "" { // generate a random number if no key
			key = prefix + "-" + strconv.Itoa(int(rand.Int31()))
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
				topic := strings.TrimPrefix(r.URL.Path, pathPrefix)
				body, err := ioutil.ReadAll(r.Body)
				if err != nil {
					s.Printf("Error reading body: %v", err)
					http.Error(w, http.StatusText(http.StatusBadRequest), http.StatusBadRequest)
					return
				} else if l := len(body); l > s.config.Kafka.Producer.MaxMessageBytes {
					s.Printf("Body size too large: %d", l)
					http.Error(w, http.StatusText(http.StatusRequestEntityTooLarge), http.StatusRequestEntityTooLarge)
					return
				}
				s.kafka.Input() <- createMessage(topic, key, body)
				w.WriteHeader(http.StatusAccepted)
				fmt.Fprintln(w, http.StatusText(http.StatusAccepted))
			}
		default:
			http.Error(w, http.StatusText(http.StatusForbidden), http.StatusForbidden)
		}
	})
}

func (s *Server) overLimit(key string) bool {
	day := strconv.FormatInt(time.Now().Unix()/3600/24, 10)
	redisKey := s.config.Redis.Keyspace + ":" + day + ":" + key
	pipe := s.redis.Pipeline()
	count := pipe.Incr(redisKey)
	pipe.Expire(redisKey, s.config.Redis.ttl)
	_, err := pipe.Exec()
	if err != nil {
		s.Println("Error: ", err)
		return false
	}
	return count.Val() > s.config.Limit
}

func createMessage(topic, key string, body []byte) *sarama.ProducerMessage {
	if key == "" {
		return &sarama.ProducerMessage{
			Topic: topic,
			Value: sarama.ByteEncoder(body),
		}
	}
	return &sarama.ProducerMessage{
		Topic: topic,
		Key:   sarama.StringEncoder(key),
		Value: sarama.ByteEncoder(body),
	}
}

func (s *Server) newKafkaProducer() sarama.AsyncProducer {
	if s.config.Verbose {
		sarama.Logger = log.New(os.Stdout, "[sarama] ", log.LstdFlags)
	}

	s.config.Kafka.MetricRegistry = metrics.NewPrefixedChildRegistry(s.metrics.saramaRegistry, "sarama.")

	v, err := sarama.ParseKafkaVersion(s.config.KafkaVersion)
	var versionIsValid bool
	if err == nil {
		// Generic ParseKafkaVersion lacks validation, add validation here
		for _, valid := range sarama.SupportedVersions {
			if v == valid {
				versionIsValid = true
				s.config.Kafka.Version = v
				break
			}
		}
	}
	if !versionIsValid {
		s.Println("Not accepted version", s.config.KafkaVersion, "fall back to default")
	}

	tlsConfig := createTLSConfiguration(s.config.KafkaTLS, s.Logger)
	if tlsConfig != nil {
		s.config.Kafka.Net.TLS.Enable = true
		s.config.Kafka.Net.TLS.Config = tlsConfig
	}

	s.Println("Kafka Config:", s.config.Kafka)

	producer, err := sarama.NewAsyncProducer(s.config.KafkaBrokers, s.config.Kafka)
	if err != nil {
		s.Fatalln("Failed to start Sarama producer:", err)
	}

	go func() {
		for err := range producer.Errors() {
			s.metrics.pFail.Inc()
			s.Println("Failed to write to Kafka:", err)
		}
	}()

	if s.config.Kafka.Producer.Return.Successes {
		go func() {
			for range producer.Successes() {
				s.metrics.pSuc.Inc()
			}
		}()
	}
	return producer
}

func webLogging(logger *log.Logger, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer func() {
			logger.Println(r.Method, r.URL.Path, r.RemoteAddr, r.UserAgent())
		}()
		next.ServeHTTP(w, r)
	})
}

func createTLSConfiguration(c TlsConfig, logger *log.Logger) (t *tls.Config) {
	if c.CertFile != "" && c.KeyFile != "" && c.CaFile != "" {
		cert, err := tls.LoadX509KeyPair(c.CertFile, c.KeyFile)
		if err != nil {
			logger.Fatal(err)
		}

		caCert, err := ioutil.ReadFile(c.CaFile)
		if err != nil {
			logger.Fatal(err)
		}

		caCertPool := x509.NewCertPool()
		caCertPool.AppendCertsFromPEM(caCert)

		t = &tls.Config{
			Certificates:       []tls.Certificate{cert},
			RootCAs:            caCertPool,
			InsecureSkipVerify: c.Insecure,
		}
	}
	return t
}
