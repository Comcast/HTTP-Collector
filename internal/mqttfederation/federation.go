package mqttfederation

import (
	"context"
	"errors"
	"http-collector/internal/logger"
	"http-collector/internal/radix"
	"log/slog"
	"strings"
	"sync"
	"time"

	MQTT "github.com/eclipse/paho.mqtt.golang"
	"github.com/google/uuid"
	mqttAuth "github.com/mochi-mqtt/server/v2/hooks/auth"
	mqttPackets "github.com/mochi-mqtt/server/v2/packets"
	mqttSystem "github.com/mochi-mqtt/server/v2/system"
	"google.golang.org/protobuf/proto"
)

var (
	FED string = "fed"

	fedMetrics = Metrics{}

	ErrMissingClientID        = errors.New("missing client ID")
	ErrNotOnline              = errors.New("device not online")
	ErrPublishRetainedMessage = errors.New("cannot publish retained message")
	ErrInvalidTopic           = errors.New("cannot publish to invalid topic")
	ErrOverClientsLimit       = errors.New("over limit")
)

// MsgProcessor is used to process MQTT messages
type MsgProcessor func(pk mqttPackets.Packet) error

func NOOPMsgProcessor(pk mqttPackets.Packet) error { return nil }

type BridgeMsgHandler func(*MQTTClient, MQTT.Message, *radix.Context)

type Server struct {
	conf           *Config
	sysInfoPayload []byte
	bridges        []*Bridge
	broker         *Broker
	bridgeRouter   *radix.Node[BridgeMsgHandler]
	toDvcTopics    *TopicsValidator
	onBrokerMsg    MsgProcessor
	log            *logger.Logger
}

// New creates a new server
func New(conf *Config) (s *Server, err error) {
	if conf == nil {
		return nil, errors.New("missing config")
	}

	if !conf.Enable {
		return nil, errors.New("not enabled")
	}

	if err = conf.Validate(); err != nil {
		return
	}

	initSysTopics(conf.Federation.SysTopic)

	if conf.Registry == nil {
		return nil, errors.New("missing Prometheus registry")
	}

	logger := logger.NewLogger()
	if level, ok := conf.LogLevel["server"]; ok {
		logger.SetLevel(level)
	} else {
		logger.SetLevel(slog.LevelError)
	}
	logger = logger.With("role", conf.Role.String())

	s = &Server{conf: conf, onBrokerMsg: conf.Broker.MsgProcessor, log: logger}

	s.sysInfoPayload, err = s.GenerateSystemInfo()
	if err != nil {
		return
	}

	s.bridgeRouter, err = s.newBridgeRouter()
	if err != nil {
		return
	}

	s.toDvcTopics, err = NewToDvcTopics(conf.Federation.DeviceTopicPrefix)
	if err != nil {
		return
	}

	s.broker, err = NewBroker(conf)
	if err != nil {
		return
	}

	hookLogger := logger.With("mod", "hook")
	if logger.Level() <= slog.LevelDebug {
		id := "debug"
		logger := hookLogger.With("mod", "hook", "hook", id)
		s.broker.AddHook(&DebugLog{id: id, log: logger}, nil)
	}

	switch conf.Role {
	case RoleSingle:
		id := "single"
		logger := hookLogger.With("hook", id)
		if err = s.broker.AddHook(new(mqttAuth.AllowHook), nil); err != nil {
			return
		}
		if err = s.broker.AddHook(&SingleHook{fed: s, id: id, log: logger}, nil); err != nil {
			return
		}
	case RoleLeader:
		id := "leader"
		logger := hookLogger.With("hook", id)
		if err = s.broker.AddHook(new(mqttAuth.AllowHook), nil); err != nil {
			return
		}
		if err = s.broker.AddHook(&LeaderHook{fed: s, id: id, log: logger}, nil); err != nil {
			return
		}
	case RoleMinion:
		id := "minion-auth"
		logger := hookLogger.With("hook", id)
		if err = s.broker.AddHook(&MinionAuthHook{id: id, log: logger}, nil); err != nil {
			return
		}

		id = "minion"
		logger = hookLogger.With("hook", id)
		if err = s.broker.AddHook(&MinionHook{fed: s, id: id, log: logger}, nil); err != nil {
			return
		}

		if s.bridges, err = s.newBridges(conf); err != nil {
			return
		}
	}

	s.registerMetrics()

	return s, nil
}

func (s *Server) newBridgeRouter() (*radix.Node[BridgeMsgHandler], error) {
	r := radix.New[BridgeMsgHandler]()

	var LeaderTopicToHandler = map[string]BridgeMsgHandler{
		ToDeviceTopicPattern: s.handleRepublish,
		SysLeaderInfoTopic:   s.handleCheckLeaderSysinfo,
	}

	for k, v := range LeaderTopicToHandler {
		_, err := r.InsertTopic(k, v)
		if err != nil {
			return nil, err
		}
	}
	return r, nil
}

// newBridges creates the Bridge to leader.
func (s *Server) newBridges(conf *Config) ([]*Bridge, error) {
	bridges := make([]*Bridge, len(conf.Federation.Bridges))

	for i := range conf.Federation.Bridges {
		b, err := NewBridge(conf, conf.Federation.Bridges[i].Addr, s.routeLeaderMsg)
		if err != nil {
			return nil, err
		}
		bridges[i] = b
	}
	return bridges, nil
}

// Start the federation
func (s *Server) Start() {
	if !s.conf.Enable {
		return
	}

	s.log.Debug("start MQTT")

	if s.conf.Role == RoleMinion {
		for _, b := range s.bridges {
			b.start()
		}
	}

	s.broker.Serve()
	if s.conf.Role == RoleLeader { // Publish retained SysInfo
		s.log.Debug("publish", "mod", FED, "topic", SysLeaderInfoTopic, "payload", s.sysInfoPayload)
		s.broker.Publish(SysLeaderInfoTopic, s.sysInfoPayload, true, 1)
	}
}

func (s *Server) Shutdown() {
	ctx, cancel := context.WithTimeout(context.TODO(), 30*time.Second)
	defer cancel()
	if s.conf.Role == RoleMinion {
		wg := &sync.WaitGroup{}
		wg.Add(len(s.bridges))
		for _, b := range s.bridges {
			b := b
			s.log.Info("shutting down bridge", "mod", FED, "broker", b.broker)
			go func() {
				defer wg.Done()
				b.shutdown(ctx)
			}()
		}
		wg.Wait()
	}
	s.log.Info("shutting down broker", "mod", FED)
	s.broker.Close()
}

// AddSubscriber re-sub to all Federation leaders
func (s *Server) AddSubscriber(id string) {
	for _, b := range s.bridges {
		b.addSubscriber(id)
	}
}

// DelSubscriber remove subsubscription from all Federation leaders
func (s *Server) DelSubscriber(id string) {
	for _, b := range s.bridges {
		b.delSubscriber(id)
	}
}

func (s *Server) RebalanceBroker(max int) {
	s.broker.rebalanceBroker(max)
}

// Publish publishes to a topic when the subscriber is online
func (s *Server) Publish(topic string, payload []byte) error {
	const QoS1 = 1
	const Retain, NoRetain = true, false

	_, ok := s.toDvcTopics.Validate(topic)
	if !ok {
		s.log.Error("publish failed", "topic", topic, "err", ErrInvalidTopic)
		fedMetrics.PublishMessageErrorNotValid.Add(1)
		return ErrInvalidTopic
	}

	if !s.broker.SubscriberAnyOnline(topic) {
		s.log.Debug("client is not online, publish retained message", "topic", topic)
		if err := s.broker.Publish(topic, payload, Retain, QoS1); err != nil {
			s.log.Error("publish retained message failed", "topic", topic, "err", err)
			fedMetrics.PublishMessageErrorRetainMessage.Add(1)
			return ErrPublishRetainedMessage
		}
		return ErrNotOnline
	}

	s.log.Debug("publish message", "topic", topic)
	if err := s.broker.Publish(topic, payload, NoRetain, QoS1); err != nil {
		s.log.Error("publish failed", "topic", topic, "err", err)
		fedMetrics.PublishMessageErrorFailed.Add(1)
		return err
	}
	s.log.Debug("publish succeeded", "topic", topic)
	fedMetrics.PublishMessageSuccess.Add(1)
	return nil
}

// routeLeaderMsg handles messages from leader
func (s *Server) routeLeaderMsg(m *MQTTClient, msg MQTT.Message) {
	fedMetrics.MessageFromLeader.Add(1)
	s.log.Debug("received message", "briID", m.connOpts.ClientID, "msgID", msg.MessageID(), "topic", msg.Topic())

	ctx := radix.NewTopicContext()
	_, _, handler := s.bridgeRouter.FindTopic(ctx, msg.Topic())
	if handler == nil {
		s.log.Error("unknown topic", "topic", msg.Topic())
		return
	}

	handler(m, msg, ctx)
}

// handleRepublish republishes messages to subscriber from leader
func (s *Server) handleRepublish(m *MQTTClient, msg MQTT.Message, ctx *radix.Context) {
	if ctx == nil {
		s.log.Error("invalid republish topic", "topic", msg.Topic())
		return
	}

	s.log.Debug("republish message", "topic", msg.Topic(), "msgID", msg.MessageID(), "dup", msg.Duplicate(), "qos", msg.Qos())

	clientID := ctx.Param("cID")
	topic := msg.Topic()
	if clientID == "" {
		s.log.Error("republish failed", "topic", topic, "err", ErrMissingClientID)
		fedMetrics.PublishMessageErrorNotValid.Add(1)
		return
	}

	if !s.broker.SubscriberOnline(topic, clientID) {
		s.log.Error("republish failed", "topic", topic, "err", ErrNotOnline)
		fedMetrics.PublishMessageErrorNotOnline.Add(1)
		return
	}

	if err := s.broker.Publish(topic, msg.Payload(), false, 1); err != nil {
		s.log.Error("republish failed", "topic", topic, "err", err)
		fedMetrics.PublishMessageErrorFailed.Add(1)
		return
	}
	s.log.Debug("republish succeeded", "topic", topic)
	fedMetrics.PublishMessageSuccess.Add(1)
}

func (s *Server) handleCheckLeaderSysinfo(m *MQTTClient, msg MQTT.Message, ctx *radix.Context) {
	m.checkLeaderSysInfo(msg)
}

func (s *Server) System() *mqttSystem.Info {
	return s.broker.Info
}

// GenerateSystemInfo generates info about this server
func (s *Server) GenerateSystemInfo() ([]byte, error) {
	uuid, err := uuid.NewUUID()
	if err != nil {
		return nil, err
	}

	uuidTime := time.Unix(uuid.Time().UnixTime())
	s.log.Info("SystemInfo", "uuid", &uuid, "time", &uuidTime)

	binaryUUID, _ := uuid.MarshalBinary()
	sysinfo := SystemInfo{Uuid: binaryUUID}
	return proto.Marshal(&sysinfo)
}

func GetClientID(topic string) (string, error) {
	const LEVELS = 3
	t := strings.Split(topic, "/")
	if len(t) < LEVELS {
		return "", errors.New("not able to get clientID")
	}
	return t[2], nil
}
