package mqttfederation

import (
	"sync/atomic"

	"github.com/prometheus/client_golang/prometheus"
)

type Metrics struct {
	ClientConnect     atomic.Int64
	ClientDisconnect  atomic.Int64
	MessageFromLeader atomic.Int64
	MessageForPublish atomic.Int64
	RetainMessage     atomic.Int64

	PublishMessageSuccess            atomic.Int64 // publish succeeded
	PublishMessageErrorNotValid      atomic.Int64 // publish failed, not a valid topic
	PublishMessageErrorNotOnline     atomic.Int64 // publish failed, subscribers not online
	PublishMessageErrorRetainMessage atomic.Int64 // publish failed, publish to retain message
	PublishMessageErrorFailed atomic.Int64 // publish failed, underlying publish failed

	BridgeRestart     atomic.Int64
	BridgeConnections atomic.Int64
}

func (s *Server) registerMetrics() {
	if s.conf.Registry == nil {
		return
	}

	registry := s.conf.Registry
	type metrics struct {
		metricType  string
		name        string
		help        string
		value       *int64
		atomicValue *atomic.Int64
	}
	metricsList := []metrics{
		{"c", "client_conn", "A counter of MQTT connection accepted", nil, &fedMetrics.ClientConnect},
		{"c", "client_disconn", "A counter of MQTT connection disconnect", nil, &fedMetrics.ClientDisconnect},
		{"c", "msg_recv_total", "A counter of total incoming messages through MQTT", &s.broker.Info.MessagesReceived, nil},
		{"c", "msg_sent_total", "A counter of total messages sent through MQTT", &s.broker.Info.MessagesSent, nil},
		{"c", "msg_drop_total", "A counter of total messages droopped due to slow subscriber", &s.broker.Info.MessagesDropped, nil},
		{"c", "pkt_recv_total", "A counter of total incoming packets through MQTT", &s.broker.Info.PacketsReceived, nil},
		{"c", "pkt_sent_total", "A counter of total packets sent through MQTT", &s.broker.Info.PacketsSent, nil},
		{"c", "bytes_recv_total", "A counter of total bytes received through MQTT", &s.broker.Info.BytesReceived, nil},
		{"c", "bytes_sent_total", "A counter of total bytes sent through MQTT", &s.broker.Info.BytesSent, nil},
		{"g", "retained_msgs", "A gauge of MQTT retained messages", &s.broker.Info.Retained, nil},
		{"g", "subscriptions", "A gauge of MQTT subscriptions", &s.broker.Info.Subscriptions, nil},
	}

	if s.conf.Role == RoleMinion {
		bridgeThreads := int64(s.conf.Federation.Threads)
		minionMetrics := []metrics{
			{"c", "bridge_msg_recv_total", "A counter of message received from leader", nil, &fedMetrics.MessageFromLeader},
			{"c", "bridge_restart", "A counter of bridge restarts", nil, &fedMetrics.BridgeRestart},
			{"g", "bridge_connection", "A gauge of bridge connections", nil, &fedMetrics.BridgeConnections},

			{"g", "bridge_threads", "A gauge of bridge threads", &bridgeThreads, nil},
		}
		metricsList = append(metricsList, minionMetrics...)
	}

	for _, m := range metricsList {
		m := m
		var fn func() float64
		switch {
		case m.value != nil:
			fn = func() float64 {
				return float64(atomic.LoadInt64(m.value))
			}
		case m.atomicValue != nil:
			fn = func() float64 {
				return float64(m.atomicValue.Load())
			}
		default:
			continue
		}

		switch m.metricType {
		case "c":
			registry.MustRegister(
				prometheus.NewCounterFunc(
					prometheus.CounterOpts{
						Name:        m.name,
						Help:        m.help,
						ConstLabels: prometheus.Labels{"role": s.conf.Role.String()},
					},
					fn,
				),
			)
		case "g":
			registry.MustRegister(
				prometheus.NewGaugeFunc(
					prometheus.GaugeOpts{
						Name:        m.name,
						Help:        m.help,
						ConstLabels: prometheus.Labels{"role": s.conf.Role.String()},
					},
					fn,
				),
			)
		}
	}

	pubToMetrics := []struct {
		code        string
		atomicValue *atomic.Int64
	}{
		{"success", &fedMetrics.PublishMessageSuccess},
		{"err_notvalid", &fedMetrics.PublishMessageErrorNotValid},
		{"err_notonline", &fedMetrics.PublishMessageErrorNotOnline},
		{"err_other", &fedMetrics.PublishMessageErrorFailed},
	}

	for _, m := range pubToMetrics {
		m := m
		registry.MustRegister(
			prometheus.NewCounterFunc(
				prometheus.CounterOpts{
					Name:        "pub_to",
					Help:        "A counter of MQTT publish to",
					ConstLabels: prometheus.Labels{"role": s.conf.Role.String(), "code": m.code},
				},
				func() float64 {
					return float64(m.atomicValue.Load())
				},
			),
		)
	}

	roleMetrics := prometheus.NewGauge(
		prometheus.GaugeOpts{
			Name:        "role",
			Help:        "MQTT Federation role",
			ConstLabels: prometheus.Labels{"role": s.conf.Role.String()},
		},
	)
	registry.MustRegister(roleMetrics)
	roleMetrics.Set(1)
}
