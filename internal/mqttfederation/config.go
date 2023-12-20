package mqttfederation

import (
	"errors"
	"fmt"
	"http-collector/internal/config"
	"log/slog"

	mqttServer "github.com/mochi-mqtt/server/v2"
	"github.com/prometheus/client_golang/prometheus"
)

const (
	DefaultFedThreads     = 2
	DefaultFedChannelSize = 1024
	DefaultNetBufferSize  = 1024 * 2
	DefaultQoS            = 1
)

type Config struct {
	Enable bool
	LogLevel   map[string]slog.Level
	Role       MQTTRole
	Registry   prometheus.Registerer
	Federation struct {
		ID          string // Client ID prefix used for connections from minion to leader.
		Threads     int    // Number of threads/connections from minion to leader.
		QoS         byte   // MQTT QoS for minion to leader connections.
		ChannelSize int    // Channel size of each thread.

		Bridges []struct {
			Addr string
		}
		TLS *config.TLS

		SysTopic string
		DeviceTopicPrefix string
	}

	Broker struct {
		Capabilities             mqttServer.Capabilities
		ClientNetWriteBufferSize int
		ClientNetReadBufferSize  int
		Listeners                []Listener
		MqttToKafka              []MqttToKafka
		MsgProcessor             MsgProcessor
		InterceptPublish         bool
	}
}

type MQTTRole uint8

const (
	RoleSingle MQTTRole = iota
	RoleLeader
	RoleMinion
)

var (
	roleText = []string{
		"single",
		"leader",
		"minion",
	}

	roleCode = map[string]MQTTRole{
		"single": RoleSingle,
		"leader": RoleLeader,
		"minion": RoleMinion,
	}
)

func (m MQTTRole) String() string {
	return roleText[m]
}

func (m *MQTTRole) UnmarshalText(text []byte) error {
	role, ok := roleCode[string(text)]
	if !ok {
		return fmt.Errorf("cannot parse %q as a role", string(text))
	}
	*m = role
	return nil
}

func (m MQTTRole) MarshalText() ([]byte, error) {
	return []byte(m.String()), nil
}

type Listener struct {
	Port string
	TLS  *config.TLS
}

type MqttToKafka struct {
	Name          string
	MqttTopic     string
	Decompress    bool
	DisableBase64 bool
	KafkaTopics   []struct {
		Name          string
		Key           string
		DisableHeader bool
	}
}

func NewConfig() *Config {
	c := &Config{}

	c.Role = RoleSingle
	c.LogLevel = map[string]slog.Level{
		"server": slog.LevelError,
		"mochi":  slog.LevelError,
		"paho":   slog.LevelError,
	}
	c.Registry = prometheus.NewRegistry()
	c.Federation.Threads = DefaultFedThreads
	c.Federation.QoS = DefaultQoS
	c.Federation.ChannelSize = DefaultFedChannelSize

	c.Broker.Capabilities = *mqttServer.DefaultServerCapabilities
	c.Broker.Capabilities.MaximumSessionExpiryInterval = 60 * 60 * 8
	c.Broker.Capabilities.MaximumMessageExpiryInterval = 60 * 60 * 24
	c.Broker.Capabilities.MaximumPacketSize = 524288
	c.Broker.Capabilities.MaximumClientWritesPending = 128

	c.Broker.ClientNetReadBufferSize = DefaultNetBufferSize
	c.Broker.ClientNetWriteBufferSize = DefaultNetBufferSize
	c.Broker.Listeners = []Listener{{Port: ":1883"}}

	return c
}

func (c *Config) Validate() (err error) {
	if !c.Enable {
		return nil
	}

	switch c.Role {
	case RoleSingle:
	case RoleLeader:
	case RoleMinion:
		if c.Federation.ID == "" {
			return errors.New("missing Federation client ID")
		}
		if c.Federation.Threads < 1 {
			c.Federation.Threads = DefaultFedThreads
		}
		if c.Federation.QoS < 1 || c.Federation.QoS > 2 { // ensure high QoS between HC & FedHC
			c.Federation.QoS = DefaultQoS
		}
		if len(c.Federation.Bridges) < 1 {
			return errors.New("missing bridges config")
		}
		if c.Federation.TLS == nil {
			c.Federation.TLS = &config.TLS{}
		}
		if err = c.Federation.TLS.Validate(); err != nil {
			return
		}
	default:
		return errors.New("missing broker role")
	}

	if len(c.Broker.Listeners) < 1 {
		return errors.New("missing broker listner")
	}

	for i, l := range c.Broker.Listeners {
		if l.TLS == nil {
			c.Broker.Listeners[i].TLS = &config.TLS{}
			continue
		}
		if err = l.TLS.Validate(); err != nil {
			return
		}
	}

	for i := 0; i < len(c.Broker.MqttToKafka); i++ {
		if c.Broker.MqttToKafka[i].Name == "" {
			c.Broker.MqttToKafka[i].Name = createMetricsName(c.Broker.MqttToKafka[i].MqttTopic, c.Federation.DeviceTopicPrefix)
		}
	}

	if c.Broker.MsgProcessor == nil {
		c.Broker.MsgProcessor = NOOPMsgProcessor
	}

	return
}
