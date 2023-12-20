package mqttfederation

import (
	"bytes"
	"errors"
	"http-collector/internal/logger"
	"strings"

	mqttServer "github.com/mochi-mqtt/server/v2"
	mqttPackets "github.com/mochi-mqtt/server/v2/packets"
)

var ErrInvalidConfig = errors.New("invalid roleHook config")

// DebugLog implements hook interface, used verbose
type DebugLog struct {
	mqttServer.HookBase
	id  string
	log *logger.Logger
}

func (d *DebugLog) ID() string {
	return d.id
}

func (d *DebugLog) Provides(b byte) bool {
	return bytes.Contains([]byte{
		mqttServer.OnConnect,
		mqttServer.OnDisconnect,
		mqttServer.OnSubscribed,
		mqttServer.OnUnsubscribed,
		mqttServer.OnPublish,
		mqttServer.OnPublishDropped,
	}, []byte{b})
}

func (d *DebugLog) OnConnect(cl *mqttServer.Client, pk mqttPackets.Packet) error {
	d.log.Debug("", "event", "OnConnect", "id", cl.ID, "user", cl.Properties.Username,
		"remote", cl.Net.Remote, "clean", cl.Properties.Clean, "protocol", cl.Properties.ProtocolVersion,
		"expiryFlag", cl.Properties.Props.SessionExpiryIntervalFlag,
		"interval", cl.Properties.Props.SessionExpiryInterval)
	return nil
}

func (d *DebugLog) OnDisconnect(cl *mqttServer.Client, err error, expire bool) {
	d.log.Debug("", "event", "OnDisconnect", "err", err, "id", cl.ID,
		"expiryFlag", cl.Properties.Props.SessionExpiryIntervalFlag,
		"interval", cl.Properties.Props.SessionExpiryInterval)
}

func (d *DebugLog) OnSubscribed(cl *mqttServer.Client, pk mqttPackets.Packet, reasonCodes []byte) {
	d.log.Debug("", "event", "OnSubscribed", "id", cl.ID, "filters", pk.Filters, "reason", reasonCodes)
}

func (d *DebugLog) OnUnsubscribed(cl *mqttServer.Client, pk mqttPackets.Packet) {
	d.log.Debug("", "event", "OnUnsubscribed", "id", cl.ID, "filters", pk.Filters)
}

func (d *DebugLog) OnPublish(cl *mqttServer.Client, pk mqttPackets.Packet) (mqttPackets.Packet, error) {
	if len(pk.Properties.User) == 0 {
		d.log.Debug("", "event", "OnPublish", "id", cl.ID, "topic", pk.TopicName, "size", len(pk.Payload))
	} else {
		d.log.Debug("", "event", "OnPublish", "id", cl.ID, "topic", pk.TopicName,
			"meta", pk.Properties.User)
	}
	return pk, nil
}

func (d *DebugLog) OnPublishDropped(cl *mqttServer.Client, pk mqttPackets.Packet) {
	d.log.Debug("", "event", "OnPublishDropped", "id", cl.ID, "topic", pk.TopicName)
}

func (d *DebugLog) OnPacketRead(cl *mqttServer.Client, pk mqttPackets.Packet) (mqttPackets.Packet, error) {
	d.log.Debug("", "event", "OnPacketRead", "id", cl.ID, "remote", cl.Net.Remote,
		"packet", mqttPackets.PacketNames[pk.FixedHeader.Type], "id", pk.PacketID, "size", len(pk.Payload))
	return pk, nil
}

func (d *DebugLog) OnPacketSent(cl *mqttServer.Client, pk mqttPackets.Packet, b []byte) {
	d.log.Debug("", "event", "OnPacketSent", "id", cl.ID, "remote", cl.Net.Remote,
		"packet", mqttPackets.PacketNames[pk.FixedHeader.Type], "id", pk.PacketID, "size", len(pk.Payload))
}

// MinionHook implements hook interface, used when role=minion
type MinionHook struct {
	fed *Server
	mqttServer.HookBase
	id  string
	log *logger.Logger
}

func (h *MinionHook) ID() string {
	return h.id
}

func (h *MinionHook) Provides(b byte) bool {
	return bytes.Contains([]byte{
		mqttServer.OnConnect,
		mqttServer.OnSubscribed,
		mqttServer.OnDisconnect,
		mqttServer.OnUnsubscribed,
		mqttServer.OnPublish,
	}, []byte{b})
}

func (h *MinionHook) OnConnect(cl *mqttServer.Client, pk mqttPackets.Packet) error {
	fedMetrics.ClientConnect.Add(1)
	return nil
}

func (h *MinionHook) OnDisconnect(cl *mqttServer.Client, err error, expire bool) {
	fedMetrics.ClientDisconnect.Add(1)
	h.fed.DelSubscriber(cl.ID)
}

func (h *MinionHook) OnSubscribed(cl *mqttServer.Client, pk mqttPackets.Packet, reasonCodes []byte) {
	for _, sub := range pk.Filters {
		param, ok := h.fed.toDvcTopics.Validate(sub.Filter)
		if !ok {
			continue
		}
		id := param.Param("cID")
		if id == cl.ID {
			h.fed.AddSubscriber(cl.ID)
			return
		}
	}
}

func (h *MinionHook) OnUnsubscribed(cl *mqttServer.Client, pk mqttPackets.Packet) {
	for _, sub := range pk.Filters {
		param, ok := h.fed.toDvcTopics.Validate(sub.Filter)
		if !ok {
			continue
		}
		id := param.Param("cID")
		if id == cl.ID && !cl.Closed() { // if client is closed, it would be captured in OnDiscoonnect
			h.fed.DelSubscriber(cl.ID)
			return
		}
	}
}

func (h *MinionHook) OnPublish(cl *mqttServer.Client, pk mqttPackets.Packet) (mqttPackets.Packet, error) {
	switch cl.Net.Inline {
	case true:
		return pk, nil
	}
	h.fed.onBrokerMsg(pk)
	return pk, nil
}

// LeadHook implements hook interface, used when role=leader
type LeaderHook struct {
	fed *Server
	mqttServer.HookBase
	id  string
	log *logger.Logger
}

func (h *LeaderHook) ID() string {
	return h.id
}

func (h *LeaderHook) Provides(b byte) bool {
	return bytes.Contains([]byte{
		mqttServer.OnConnect,
		mqttServer.OnDisconnect,
		mqttServer.OnRetainPublished,
		mqttServer.OnSubscribed,
	}, []byte{b})
}

func (h *LeaderHook) OnConnect(cl *mqttServer.Client, pk mqttPackets.Packet) error {
	fedMetrics.ClientConnect.Add(1)
	return nil
}

func (h *LeaderHook) OnDisconnect(cl *mqttServer.Client, err error, expire bool) {
	fedMetrics.ClientDisconnect.Add(1)
}

func (h *LeaderHook) OnRetainPublished(cl *mqttServer.Client, pk mqttPackets.Packet) {
	h.log.Debug("", "event", "OnRetainPublished", "id", cl.ID, "remote", cl.Net.Remote,
		"packet", mqttPackets.PacketNames[pk.FixedHeader.Type], "id", pk.PacketID, "size", len(pk.Payload),
		"topic", pk.TopicName)

	// add check for <devicePrefix>/to topic only
	if strings.HasPrefix(pk.TopicName, ToDeviceTopicPrefix) {
		h.fed.broker.Topics.Retained.Delete(pk.TopicName)
	}
}

func (h *LeaderHook) OnSubscribed(cl *mqttServer.Client, pk mqttPackets.Packet, reasonCodes []byte) {
	for _, sub := range pk.Filters {
		if strings.HasPrefix(sub.Filter, SysTopic) {
			continue
		}

		// only allow one subscriber from minion per topic
		for subID := range h.fed.broker.Topics.Subscribers(sub.Filter).Subscriptions {
			if subID != cl.ID {
				h.fed.broker.unsubscribe(sub.Filter, subID)
			}
		}
	}
}

// Single implements hook interface, used when role=single
type SingleHook struct {
	fed *Server
	mqttServer.HookBase
	id  string
	log *logger.Logger
}

func (h *SingleHook) ID() string {
	return h.id
}

func (h *SingleHook) Provides(b byte) bool {
	return bytes.Contains([]byte{
		mqttServer.OnConnect,
		mqttServer.OnDisconnect,
		mqttServer.OnPublish,
	}, []byte{b})
}

func (h *SingleHook) OnConnect(cl *mqttServer.Client, pk mqttPackets.Packet) error {
	fedMetrics.ClientConnect.Add(1)
	return nil
}

func (h *SingleHook) OnDisconnect(cl *mqttServer.Client, err error, expire bool) {
	fedMetrics.ClientDisconnect.Add(1)
}

func (h *SingleHook) OnPublish(cl *mqttServer.Client, pk mqttPackets.Packet) (mqttPackets.Packet, error) {
	switch cl.Net.Inline {
	case true:
		return pk, nil
	}
	h.fed.onBrokerMsg(pk)
	return pk, nil
}

type MinionAuthHook struct {
	mqttServer.HookBase
	id  string
	log *logger.Logger
}

// ID returns the ID of the hook.
func (h *MinionAuthHook) ID() string {
	return h.id
}

// Provides indicates which hook methods this hook provides.
func (h *MinionAuthHook) Provides(b byte) bool {
	return bytes.Contains([]byte{
		mqttServer.OnConnectAuthenticate,
		mqttServer.OnACLCheck,
	}, []byte{b})
}

// OnConnectAuthenticate returns true/allowed for all requests.
func (h *MinionAuthHook) OnConnectAuthenticate(cl *mqttServer.Client, pk mqttPackets.Packet) bool {
	return true
}

// OnACLCheck returns true/allowed for all except SysTopics.
func (h *MinionAuthHook) OnACLCheck(cl *mqttServer.Client, topic string, write bool) bool {
	if strings.HasPrefix(topic, SysTopic) && write {
		h.log.Error("write to system topic not allowed", "id", cl.ID, "user", cl.Properties.Username,
			"remote", cl.Net.Remote)
		return false
	}
	return true
}
