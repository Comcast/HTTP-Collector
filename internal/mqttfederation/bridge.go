package mqttfederation

import (
	"bytes"
	"context"
	"errors"
	"hash/maphash"
	"http-collector/internal/logger"
	"log/slog"
	"math"
	"strconv"
	"sync"
	"time"

	MQTT "github.com/eclipse/paho.mqtt.golang"
	"github.com/google/uuid"
	"google.golang.org/protobuf/proto"
)

const MaxDisconnectWait = uint(10 * time.Second / time.Millisecond)

type Bridge struct {
	conf    *Config
	broker  string
	clients []*MQTTClient
	seed    maphash.Seed
	ctx     context.Context
	cancel  context.CancelFunc
	wg      *sync.WaitGroup
	log     *logger.Logger
}

type OnBridgeMsg func(*MQTTClient, MQTT.Message)

func NewBridge(conf *Config, broker string, onBridgeMsg OnBridgeMsg) (*Bridge, error) {
	if conf == nil || onBridgeMsg == nil {
		return nil, errors.New("missing config or message handler")
	}

	if level, ok := conf.LogLevel["paho"]; ok {
		if level <= slog.LevelError {
			logger := logger.NewLogger().With("role", conf.Role.String(), "mod", "paho")
			logger.SetLevel(slog.LevelError)
			MQTT.ERROR = logger
		}
		if level <= slog.LevelDebug {
			logger := logger.NewLogger().With("role", conf.Role.String(), "mod", "paho")
			logger.SetLevel(slog.LevelDebug)
			MQTT.DEBUG = logger
		}
	}

	logger := logger.NewLogger().With("role", conf.Role.String(), "mod", "bridge")
	seed := maphash.MakeSeed()
	b := &Bridge{
		conf:   conf,
		broker: broker,
		seed:   seed,
		wg:     &sync.WaitGroup{},
		log:    logger,
	}
	b.ctx, b.cancel = context.WithCancel(context.TODO())

	b.clients = make([]*MQTTClient, conf.Federation.Threads)
	for i := 0; i < conf.Federation.Threads; i++ {
		connOpts := MQTT.NewClientOptions().
			AddBroker(b.broker).
			// Paho module doesn't have a way to fully stop a client
			// So, instead of have the client auto reconnect with resumeSub
			//  1/ create a new client and connect with cleanSession=true,
			//  2/ which resets all sub on leader then bulk sub all existing clients
			SetAutoReconnect(false). // will create new client/bulk subscription upon disconnect
			SetCleanSession(true).
			SetClientID(conf.Federation.ID + "-" + strconv.Itoa(i)).
			SetConnectTimeout(30 * time.Second).
			SetKeepAlive(60 * time.Second).
			SetMaxReconnectInterval(10 * time.Second).
			SetResumeSubs(false). // upon disconnect, client should be released
			SetTLSConfig(b.conf.Federation.TLS.GetClientTLS()).
			SetWriteTimeout(0)

		logger := logger.With("briID", connOpts.ClientID)
		cl := &MQTTClient{
			ctx:               b.ctx,
			connOpts:          connOpts,
			topicPrefix:       ToDeviceTopicPrefix,
			qos:               b.conf.Federation.QoS,
			onBridgeMsg:       onBridgeMsg,
			subscribers:       make(map[string]byte),
			update:            make(chan SubscriberUpdate, b.conf.Federation.ChannelSize),
			newClientToLeader: make(chan MQTT.Client),
			restart:           make(chan bool),
			log:               logger,
		}
		b.clients[i] = cl
	}
	return b, nil
}

func (b *Bridge) start() {
	b.wg.Add(b.conf.Federation.Threads)
	for _, client := range b.clients {
		go client.run(b.wg)
	}
}

// addSubscriber subscribe the client with leader
func (b *Bridge) addSubscriber(id string) {
	i := maphash.String(b.seed, id) % uint64(b.conf.Federation.Threads)
	b.clients[i].addClient(id)
}

// delSubscriber unsubscribe the client with leader
func (b *Bridge) delSubscriber(id string) {
	i := maphash.String(b.seed, id) % uint64(b.conf.Federation.Threads)
	b.clients[i].delClient(id)
}

func (b *Bridge) shutdown(ctx context.Context) error {
	var cancel context.CancelFunc
	if ctx == nil {
		ctx, cancel = context.WithTimeout(context.TODO(), 30*time.Second)
		defer cancel()
	}
	b.cancel()
	done := make(chan bool)
	go func() {
		b.wg.Wait()
		close(done)
	}()

	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-done:
		return nil
	}
}

type MQTTClient struct {
	ctx               context.Context
	startTime         int64
	connOpts          *MQTT.ClientOptions
	topicPrefix       string // topic prefix for subscription
	qos               byte
	onBridgeMsg       OnBridgeMsg     // handler for message from leader
	subscribers       map[string]byte // a map connected subscribers
	update            chan SubscriberUpdate
	newClientToLeader chan MQTT.Client
	restart           chan bool
	leaderInfo        *SystemInfo
	log               *logger.Logger
}

type SubscriberUpdate struct {
	state int
	id    string
}

const (
	Add int = iota
	Del
)

func (m *MQTTClient) addClient(id string) {
	m.update <- SubscriberUpdate{Add, id}
}

func (m *MQTTClient) delClient(id string) {
	m.update <- SubscriberUpdate{Del, id}
}

func (m *MQTTClient) run(wg *sync.WaitGroup) {
	defer wg.Done()
	go m.startSession()
	m.eventLoop()
}

func (m *MQTTClient) eventLoop() {
	var cl MQTT.Client = &noopClient{}
	restartInProgress := false
	m.startTime = time.Now().UnixMilli()

	for {
		select {
		case subscriberUpdate := <-m.update:
			topic := m.topicPrefix + subscriberUpdate.id + "/#"
			switch subscriberUpdate.state {
			case Add:
				if _, ok := m.subscribers[subscriberUpdate.id]; ok {
					break
				}

				m.subscribers[subscriberUpdate.id] = m.qos
				token := cl.Subscribe(topic, m.qos, nil)
				if err := token.Error(); err != nil { // handle immediate error, let connection error to handle actual write error
					m.log.Error("subscribe error to leader", "err", err, "broker", m.connOpts.Servers)
					cl = &noopClient{}
					go func() {
						m.restart <- true
					}()
				}
			case Del:
				if _, ok := m.subscribers[subscriberUpdate.id]; !ok {
					break
				}

				delete(m.subscribers, subscriberUpdate.id)
				token := cl.Unsubscribe(topic)
				if err := token.Error(); err != nil { // handle immediate error, let connection error to handle actual write error
					m.log.Error("unsubscribe error to leader", "err", err, "broker", m.connOpts.Servers)
					cl = &noopClient{}
					go func() {
						m.restart <- true
					}()
				}
			}
		case <-m.restart:
			if restartInProgress {
				m.log.Debug("bridge restart already in progress")
				break
			}
			restartInProgress = true
			m.log.Info("restarting bridge to leader", "broker", m.connOpts.Servers)
			m.startTime = time.Now().UnixMilli()
			go m.restartSession(cl)
			cl = &noopClient{}
		case cl = <-m.newClientToLeader: // auto reconnect will not trigger this
			restartInProgress = false
			if !cl.IsConnectionOpen() {
				go func() {
					m.restart <- true
				}()
			}
			m.bulkSubscribe(cl)
		case <-m.ctx.Done():
			m.log.Info("shutting down bridge to leader", "broker", m.connOpts.Servers)
			if cl.IsConnectionOpen() {
				cl.Disconnect(MaxDisconnectWait)
			}
			return
		}
	}
}

// restartSession disconnect existing client, and establishes new connection to leader
func (m *MQTTClient) restartSession(cl MQTT.Client) {
	cl.Disconnect(MaxDisconnectWait)
	fedMetrics.BridgeRestart.Add(1)
	m.startSession()
}

// startSession retries forever until it establishes connection to leader
func (m *MQTTClient) startSession() {
	const MinBackOff = time.Second
	const MaxBackOff = 10 * time.Second
	tries := 0
	for {
		cl, err := m.newSession()
		if err == nil {
			m.newClientToLeader <- cl
			return
		}

		wait := backoff(MinBackOff, MaxBackOff, tries)
		select {
		case <-m.ctx.Done():
			return
		case <-time.After(wait):
		}
		tries++
	}
}

// newSession creates a new session to leader. It will first resetSession
func (m *MQTTClient) newSession() (MQTT.Client, error) {
	m.leaderInfo = nil

	// create actual connection to be used
	connOpts := *m.connOpts
	connOpts.SetDefaultPublishHandler(m.defaultMessageHandler)
	connOpts.OnConnect = func(c MQTT.Client) { // ensure always subscribe upon connect/reconnect
		m.log.Debug("OnConnect to leader", "broker", connOpts.Servers)
		fedMetrics.BridgeConnections.Add(1)
		topics := map[string]byte{ // minion must subscribe topics
			SysAllBrdTopic:                  m.qos,
			SysUniTopic + connOpts.ClientID: m.qos,
		}
		if token := c.SubscribeMultiple(topics, nil); token.Wait() && token.Error() != nil {
			return
		}
	}
	connOpts.OnConnectionLost = func(c MQTT.Client, err error) {
		m.log.Debug("OnConnectionLost to leader", "broker", connOpts.Servers)
		go func() {
			m.restart <- true
		}()
		fedMetrics.BridgeConnections.Add(-1)
	}

	m.log.Debug("connect to leader", "broker", connOpts.Servers)
	cl := MQTT.NewClient(&connOpts)
	if token := cl.Connect(); token.Wait() && token.Error() != nil {
		m.log.Error("error connect to leader", "broker", &connOpts.Servers[0].Host, "err", token.Error())
		return nil, token.Error()
	}
	m.log.Debug("successfully connected to leader", "broker", connOpts.Servers)

	return cl, nil
}

// bulkSubscribe will send all existing subscribers to the leader
func (m *MQTTClient) bulkSubscribe(cl MQTT.Client) {
	m.log.Debug("bulk subscribe", "count", len(m.subscribers))
	batch := make(map[string]byte)
	for id, qos := range m.subscribers {
		batch[m.topicPrefix+id] = qos
		if len(batch) > 100 {
			cl.SubscribeMultiple(batch, nil)
			batch = make(map[string]byte)
		}
	}
	if len(batch) > 0 {
		cl.SubscribeMultiple(batch, nil)
	}
}

// defaultMessageHandler handles all incoming message. Paho mod is not efficient in routing, so all
// message goto a single handler, and route traffic.
func (m *MQTTClient) defaultMessageHandler(c MQTT.Client, msg MQTT.Message) {
	m.onBridgeMsg(m, msg)
}

// checkLeaderSysInfo restarts session if leader's UUID changed.
func (m *MQTTClient) checkLeaderSysInfo(msg MQTT.Message) {
	info := &SystemInfo{}
	err := proto.Unmarshal(msg.Payload(), info)
	if err != nil {
		return
	}

	var leadUUID uuid.UUID
	err = leadUUID.UnmarshalBinary(info.Uuid)
	if err != nil {
		m.log.Error("invalid leader UUID", "err", err)
		return
	}

	if m.leaderInfo == nil { // 1st time connected to a leader
		m.leaderInfo = info
		return
	}

	// below should not happen any more as we no longer auto reconnect session, a new client is always
	// created upon disconnect

	leaderTime := time.Unix(leadUUID.Time().UnixTime())
	if time.Now().Before(leaderTime) {
		m.log.Error("leader time is in the future, check leader or local NTP is running", "broker", m.connOpts.Servers, "leaderTime", leaderTime)
	}

	if !bytes.Equal(info.Uuid, m.leaderInfo.Uuid) {
		m.log.Info("leader restarted, restarting bridge", "broker", m.connOpts.Servers)
		m.leaderInfo = info
		m.restart <- true
	}
}

func backoff(min, max time.Duration, tries int) time.Duration {
	const MaxTries = 32
	if tries > MaxTries {
		tries = MaxTries
	}
	if min <= 0 {
		min = 1 * time.Second
	}

	mult := math.Pow(2, float64(tries)) * float64(min)
	sleep := time.Duration(mult)
	if float64(sleep) != mult || sleep > max {
		sleep = max
	}
	return sleep
}

// noopClient implements MQTT.client. It is a noopClient so eventLoop is not blocked if a leader is down.
type noopClient struct{}

func (n *noopClient) IsConnected() bool { return false }

func (n *noopClient) IsConnectionOpen() bool { return false }

func (n *noopClient) Connect() MQTT.Token { return &MQTT.DummyToken{} }

func (n *noopClient) Disconnect(quiesce uint) {}

func (n *noopClient) Publish(topic string, qos byte, retained bool, payload interface{}) MQTT.Token {
	return &MQTT.DummyToken{}
}

func (n *noopClient) Subscribe(topic string, qos byte, callback MQTT.MessageHandler) MQTT.Token {
	return &MQTT.DummyToken{}
}

func (n *noopClient) SubscribeMultiple(filters map[string]byte, callback MQTT.MessageHandler) MQTT.Token {
	return &MQTT.DummyToken{}
}

func (n *noopClient) Unsubscribe(topics ...string) MQTT.Token { return &MQTT.DummyToken{} }

func (n *noopClient) AddRoute(topic string, callback MQTT.MessageHandler) {}

func (n *noopClient) OptionsReader() MQTT.ClientOptionsReader { return MQTT.ClientOptionsReader{} }
