package main

import (
	"net/http"
	"runtime"
	"strings"
	"sync"
	"time"

	pmetrics "github.com/deathowl/go-metrics-prometheus"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/rcrowley/go-metrics"
)

type appMetrics struct {
	saramaRegistry metrics.Registry
	promClient     *pmetrics.PrometheusConfig
	promMutex      *sync.Mutex
	counter        *prometheus.CounterVec
	duration       *prometheus.HistogramVec
	pSuc           prometheus.Counter
	pFail          prometheus.Counter
	mirror         *prometheus.CounterVec
	mqttMsg        *prometheus.CounterVec
	mqttMsgErr     prometheus.Counter
	mqttProcessErr prometheus.Counter
	mqttLatency    *prometheus.HistogramVec
}

func newMetrics() *appMetrics {
	saramaRegistry := metrics.NewRegistry()
	metrics := &appMetrics{
		saramaRegistry: saramaRegistry,
		promClient:     pmetrics.NewPrometheusProvider(saramaRegistry, "hc", "", prometheus.DefaultRegisterer, 1*time.Second),
		promMutex:      new(sync.Mutex),
		counter: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Name: "api_requests_total",
				Help: "A counter for total number of requests.",
			},
			[]string{"app", "code", "method", "topic"}, // app name, status code, http method, request URL
		),
		duration: prometheus.NewHistogramVec(
			prometheus.HistogramOpts{
				Name:    "api_request_duration_seconds",
				Help:    "A histogram of latencies for requests.",
				Buckets: []float64{.01, .03, 0.1, 0.5, 1, 3, 10, 130},
			},
			[]string{"app"},
		),
		pSuc: prometheus.NewCounter(
			prometheus.CounterOpts{
				Namespace: "hc",
				Name:      "kafka_success",
				Help:      "A counter of successful messages produced to Kafka",
			},
		),
		pFail: prometheus.NewCounter(
			prometheus.CounterOpts{
				Namespace: "hc",
				Name:      "kafka_fail",
				Help:      "A counter of messages failed to produce to Kafka",
			},
		),
		mirror: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Namespace: "hc",
				Name:      "mirror",
				Help:      "A counter for total number of mirror messages.",
			},
			[]string{"topic"},
		),
		mqttMsg: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Namespace: "hc",
				Name:      "mqtt_msg",
				Help:      "A counter for incoming messages through mqtt by topic.",
			},
			[]string{"topic"},
		),
		mqttMsgErr: prometheus.NewCounter(
			prometheus.CounterOpts{
				Namespace: "hc",
				Name:      "mqtt_msg_error",
				Help:      "A counter of MQTT message error",
			},
		),
		mqttProcessErr: prometheus.NewCounter(
			prometheus.CounterOpts{
				Namespace: "hc",
				Name:      "mqtt_process_error",
				Help:      "A counter of MQTT message processing error",
			},
		),
		mqttLatency: prometheus.NewHistogramVec(
			prometheus.HistogramOpts{
				Namespace: "hc",
				Name:      "mqtt_latnecy",
				Help:      "A histogram of latencies for MQTT messages.",
				Buckets:   []float64{0.1, 0.2, 0.5, 1, 5, 10, 60},
			},
			[]string{"topic"},
		),
	}

	prometheus.MustRegister(metrics.counter, metrics.duration, metrics.pSuc, metrics.pFail, metrics.mirror,
		metrics.mqttMsg, metrics.mqttMsgErr, metrics.mqttProcessErr, metrics.mqttLatency)
	createBuildInfoMetrics()
	return metrics
}

func createBuildInfoMetrics() {
	buildInfo := prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: AppName,
			Name:      "build_info",
			Help:      "Build Information",
		},
		[]string{"buildtime", "goversion", "version"},
	)
	prometheus.MustRegister(buildInfo)
	buildInfo.With(prometheus.Labels{"buildtime": BuildTime, "goversion": runtime.Version(), "version": AppVersion}).Set(1)
}

func (s *Server) webMetrics(m *appMetrics, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var topic string
		switch {
		case strings.HasPrefix(r.URL.Path, apiPathV2):
			topic = strings.TrimPrefix(r.URL.Path, apiPathV2)
		case strings.HasPrefix(r.URL.Path, apiPathV2Limit):
			topic = strings.TrimPrefix(r.URL.Path, apiPathV2Limit)
		case strings.HasPrefix(r.URL.Path, apiMqttPub):
			topic = "mqttpub"
		default:
			topic = "unknown"
		}

		promhttp.InstrumentHandlerDuration(m.duration.MustCurryWith(prometheus.Labels{"app": AppName}),
			promhttp.InstrumentHandlerCounter(m.counter.MustCurryWith(prometheus.Labels{"app": AppName, "topic": topic}), next),
		).ServeHTTP(w, r)
	})
}
