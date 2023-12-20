package mqttfederation

import (
	"http-collector/internal/radix"
	"strings"
)

const (
	DefaultSysTopicPrefix    = "SysTopic"
	DefaultDeviceTopicPrefix = "device"
)

var (
	SysTopic, SysBrdTopic, SysUniTopic, SysAllBrdTopic, SysLeaderInfoTopic string
	ToDeviceTopicPrefix, ToDeviceTopicPattern, ToAllDeviceTopic            string
	FrDeviceTopicPrefix, FrDeviceTopicPattern                              string
)

type TopicsValidator struct {
	validTopics *radix.Node[bool]
}

func initSysTopics(sysTopicPrefix string) {
	SysTopic = DefaultSysTopicPrefix + "$/"
	if sysTopicPrefix != "" {
		SysTopic = sysTopicPrefix + "$/"
	}
	SysBrdTopic = SysTopic + "brd/"
	SysUniTopic = SysTopic + "uni/"
	SysAllBrdTopic = SysBrdTopic + "#"
	SysLeaderInfoTopic = SysBrdTopic + "fr/leader/info"
}

func NewToDvcTopics(devTopicPrefix string) (*TopicsValidator, error) {
	dtPrefix := DefaultDeviceTopicPrefix
	if devTopicPrefix != "" {
		dtPrefix = devTopicPrefix
	}
	ToDeviceTopicPrefix = dtPrefix + "/to/"
	ToDeviceTopicPattern = ToDeviceTopicPrefix + "{cID}/{module}"
	ToAllDeviceTopic = ToDeviceTopicPrefix + "all"

	r := radix.New[bool]()
	_, err := r.InsertTopic(ToDeviceTopicPattern, true)
	if err != nil {
		return nil, err
	}
	return &TopicsValidator{validTopics: r}, nil
}

func NewFrDvcTopics(devTopicPrefix string) (*TopicsValidator, error) {
	dtPrefix := DefaultDeviceTopicPrefix
	if devTopicPrefix != "" {
		dtPrefix = devTopicPrefix
	}
	FrDeviceTopicPrefix = dtPrefix + "/fr/"
	FrDeviceTopicPattern = FrDeviceTopicPrefix + "{cID}/{lID}/{module}/{action}"

	r := radix.New[bool]()
	_, err := r.InsertTopic(FrDeviceTopicPattern, true)
	if err != nil {
		return nil, err
	}
	return &TopicsValidator{validTopics: r}, nil
}

func (t *TopicsValidator) Validate(topic string) (*radix.Context, bool) {
	param := radix.NewTopicContext()
	_, _, valid := t.validTopics.FindTopic(param, topic)
	return param, valid
}

func createMetricsName(name string, devTopicPrefix string) string {
	name = strings.Split(name, "/chi/")[0]

	frDvcTopic, _ := NewFrDvcTopics(devTopicPrefix)
	param, ok := frDvcTopic.Validate(name)
	if ok {
		module := param.Param("module")
		action := param.Param("action")
		return FrDeviceTopicPrefix + module + "/" + action
	}

	pi := strings.IndexRune(name, '{')
	if pi >= 0 {
		name = name[:pi]
	}
	return strings.TrimSuffix(name, "/")
}
