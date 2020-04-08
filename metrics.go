package main

import (
	"net/http"
	"strings"
	"time"

	pmetrics "github.com/deathowl/go-metrics-prometheus"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/rcrowley/go-metrics"
)

type appMetrics struct {
	saramaRegistry metrics.Registry
	promClient     *pmetrics.PrometheusConfig
	counter        *prometheus.CounterVec
	duration       *prometheus.HistogramVec
	inFlight       prometheus.Gauge
	responseSize   *prometheus.HistogramVec
	requestSize    *prometheus.HistogramVec
	pSuc           prometheus.Counter
	pFail          prometheus.Counter
}

func newMetrics() *appMetrics {
	saramaRegistry := metrics.NewRegistry()
	metrics := &appMetrics{
		saramaRegistry: saramaRegistry,
		promClient:     pmetrics.NewPrometheusProvider(saramaRegistry, "hc", "", prometheus.DefaultRegisterer, 1*time.Second),

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
			[]string{"app", "code", "method", "topic"},
		),
		inFlight: prometheus.NewGauge(
			prometheus.GaugeOpts{
				Name: "in_flight_requests",
				Help: "A gauge of requests currently being served.",
			},
		),
		requestSize: prometheus.NewHistogramVec(
			prometheus.HistogramOpts{
				Name:    "api_request_size_bytes",
				Help:    "A histogram of request sizes for requests.",
				Buckets: []float64{200, 500, 1000, 10000, 100000},
			},
			[]string{"app"},
		),
		responseSize: prometheus.NewHistogramVec(
			prometheus.HistogramOpts{
				Name:    "api_response_size_bytes",
				Help:    "A histogram of response sizes for requests.",
				Buckets: []float64{200, 500, 1000, 10000, 100000},
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
	}
	prometheus.MustRegister(metrics.inFlight, metrics.counter, metrics.duration,
		metrics.requestSize, metrics.responseSize, metrics.pSuc, metrics.pFail)

	return metrics
}

func webMetrics(m *appMetrics, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var topic string
		if strings.HasPrefix(r.URL.Path, apiPathV2) {
			topic = strings.TrimPrefix(r.URL.Path, apiPathV2)
		} else if strings.HasPrefix(r.URL.Path, apiPathV2Limit) {
			topic = strings.TrimPrefix(r.URL.Path, apiPathV2Limit)
		}
		promhttp.InstrumentHandlerInFlight(m.inFlight,
			promhttp.InstrumentHandlerDuration(m.duration.MustCurryWith(prometheus.Labels{"app": AppName, "topic": topic}),
				promhttp.InstrumentHandlerCounter(m.counter.MustCurryWith(prometheus.Labels{"app": AppName, "topic": topic}),
					promhttp.InstrumentHandlerRequestSize(m.requestSize.MustCurryWith(prometheus.Labels{"app": AppName}),
						promhttp.InstrumentHandlerResponseSize(m.responseSize.MustCurryWith(prometheus.Labels{"app": AppName}), next),
					),
				),
			),
		).ServeHTTP(w, r)
	})
}
