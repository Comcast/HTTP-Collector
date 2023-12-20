package mqttfederation

import (
	"http-collector/internal/logger"
	"log/slog"
	"sync/atomic"

	mqttServer "github.com/mochi-mqtt/server/v2"
	mqttListen "github.com/mochi-mqtt/server/v2/listeners"
)

type Broker struct {
	*mqttServer.Server
}

func NewBroker(conf *Config) (*Broker, error) {
	mochiLog := logger.NewLogger().With("role", conf.Role.String(), "mod", "mochi")
	mochiLog.SetLevel(slog.LevelError)
	if level, ok := conf.LogLevel["mochi"]; ok {
		mochiLog.SetLevel(level)
	}

	b := &Broker{
		Server: mqttServer.New(&mqttServer.Options{
			Capabilities:             &conf.Broker.Capabilities,
			ClientNetWriteBufferSize: conf.Broker.ClientNetWriteBufferSize,
			ClientNetReadBufferSize:  conf.Broker.ClientNetReadBufferSize,
			Logger:                   mochiLog.Slog(),
			InlineClient:             true,
		}),
	}

	for _, l := range conf.Broker.Listeners {
		err := b.AddListener(mqttListen.NewTCP("tcp"+l.Port, l.Port, &mqttListen.Config{
			TLSConfig: l.TLS.GetServerTLS(),
		}))
		if err != nil {
			return b, err
		}
	}

	return b, nil
}

// SubscriberAnyOnline checks if any subscriber to the topic is online
func (b *Broker) SubscriberAnyOnline(topic string) bool {
	subscribers := b.Topics.Subscribers(topic)
	for id := range subscribers.Subscriptions {
		if b.clientOnline(id) {
			return true
		}
	}
	return false
}

// SubscriberOnline checks if that subscriber to the topic is online
func (b *Broker) SubscriberOnline(topic, id string) bool {
	subscribers := b.Topics.Subscribers(topic)
	if _, ok := subscribers.Subscriptions[id]; !ok {
		return false
	}

	return b.clientOnline(id)
}

func (b *Broker) unsubscribe(topic, id string) {
	if cl, ok := b.Clients.Get(id); ok {
		cl.State.Subscriptions.Delete(topic)
	}

	if b.Topics.Unsubscribe(topic, id) {
		atomic.AddInt64(&b.Info.Subscriptions, -1)
	}
}

func (b *Broker) clientOnline(id string) bool {
	cl, ok := b.Clients.Get(id)
	if ok && !cl.Closed() {
		return true
	}
	return false
}

func (b *Broker) rebalanceBroker(max int) {
	over := b.Clients.Len() - max
	if max < 1 || over < 1 {
		return
	}
	clients := b.Clients.GetAll()
	for _, cl := range clients {
		cl.Stop(ErrOverClientsLimit)
		over--
		if over < 1 {
			break
		}
	}
}
