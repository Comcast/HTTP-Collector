package main

import (
	"http-collector/internal/mqttfederation"
	reflect "reflect"
	"runtime"
	"testing"
	"time"

	"github.com/IBM/sarama"
	"github.com/mochi-mqtt/server/v2/packets"
	"github.com/stretchr/testify/require"
)

func TestSelectPreparePayload(t *testing.T) {
	tests := []struct {
		topic                string
		decompress           bool
		disableBase64        bool
		preparePayloadExpect string
	}{
		{"a/b/c/d", false, false,
			runtime.FuncForPC(reflect.ValueOf(base64Payload).Pointer()).Name(),
		},
		{"a/b/c/d", false, true,
			runtime.FuncForPC(reflect.ValueOf(copyPayload).Pointer()).Name(),
		},
		{"a/b/c/d", true, false,
			runtime.FuncForPC(reflect.ValueOf(decompressBase64Payload).Pointer()).Name(),
		},
		{"a/b/c/d", true, true,
			runtime.FuncForPC(reflect.ValueOf(decompressPayload).Pointer()).Name(),
		},
	}

	for i, tt := range tests {
		config := mqttfederation.MqttToKafka{
			MqttTopic:     tt.topic,
			Decompress:    tt.decompress,
			DisableBase64: tt.disableBase64,
		}
		result := SelectPreparePayload(config)

		got := runtime.FuncForPC(reflect.ValueOf(result).Pointer()).Name()
		require.Equal(t, tt.preparePayloadExpect, got, "index %d", i)
	}
}

func TestNewKafkaTopicConstructor(t *testing.T) {
	tests := []struct {
		config    mqttfederation.MqttToKafka
		expect    KafkaTopicConstructor
		expectErr bool
	}{
		{
			config: mqttfederation.MqttToKafka{
				Name:          "t1",
				MqttTopic:     "a/b/{x}/{y}",
				Decompress:    false,
				DisableBase64: false,
				KafkaTopics: []struct {
					Name          string
					Key           string
					DisableHeader bool
				}{
					{"test-topic", "{x}", false},
					{"test-{y}", "{x}", true},
				},
			},
			expect: KafkaTopicConstructor{
				{
					Name:         "test-topic",
					NameValueIdx: nil,
					Key:          "%v",
					KeyIdx:       []int{0},
					Rpt:          "t1",
				},
				{
					Name:         "test-%v",
					NameValueIdx: []int{1},
					Key:          "%v",
					KeyIdx:       []int{0},
					Rpt:          "",
				},
			},
			expectErr: false,
		},
		{
			config: mqttfederation.MqttToKafka{
				Name:          "t2",
				MqttTopic:     "a/b/{x}/{y}",
				Decompress:    false,
				DisableBase64: false,
				KafkaTopics: []struct {
					Name          string
					Key           string
					DisableHeader bool
				}{
					{"someTopicName", "{y}", false},
				},
			},
			expect: KafkaTopicConstructor{
				{
					Name:         "someTopicName",
					NameValueIdx: nil,
					Key:          "%v",
					KeyIdx:       []int{1},
					Rpt:          "t2",
				},
			},
			expectErr: false,
		},
		{
			config: mqttfederation.MqttToKafka{
				MqttTopic:     "a/b/{x}/{y}",
				Decompress:    false,
				DisableBase64: false,
				KafkaTopics: []struct {
					Name          string
					Key           string
					DisableHeader bool
				}{
					{"someTopicName", "", true},
				},
			},
			expect: KafkaTopicConstructor{
				{
					Name:         "someTopicName",
					NameValueIdx: nil,
					Key:          "",
					KeyIdx:       nil,
					Rpt:          "",
				},
			},
			expectErr: false,
		},
		{
			config: mqttfederation.MqttToKafka{
				MqttTopic:     "a/b/{x}",
				Decompress:    false,
				DisableBase64: false,
				KafkaTopics: []struct {
					Name          string
					Key           string
					DisableHeader bool
				}{
					{"someTopicName", "{y}", true},
				},
			},
			expect: KafkaTopicConstructor{
				{
					Name:         "someTopicName",
					NameValueIdx: nil,
					Key:          "%v",
					KeyIdx:       []int{1},
					Rpt:          "",
				},
			},
			expectErr: true,
		},
	}

	for i, tt := range tests {
		got, err := NewKafkaTopicConstructor(tt.config)
		if tt.expectErr {
			require.Error(t, err)
			continue
		}

		require.NoError(t, err)
		require.Equal(t, tt.expect, got, "index %d", i)
	}
}

func TestKafkaTopicConstructGet(t *testing.T) {
	tests := []struct {
		input     KafkaTopicConstruct
		params    []string
		expect    KafkaTopicInfo
		expectErr bool
	}{
		{
			input: KafkaTopicConstruct{
				Name:         "test-Aggregated",
				NameValueIdx: nil,
				Key:          "%v",
				KeyIdx:       []int{0},
				Rpt:          "",
			},
			params: []string{"x", "y"},
			expect: KafkaTopicInfo{"test-Aggregated", "x", ""},
		},
		{
			input: KafkaTopicConstruct{
				Name:         "test-%v",
				NameValueIdx: []int{1},
				Key:          "%v",
				KeyIdx:       []int{0},
				Rpt:          "",
			},
			params: []string{"x", "y"},
			expect: KafkaTopicInfo{"test-y", "x", ""},
		},
		{
			input: KafkaTopicConstruct{
				Name:         "test-topic",
				NameValueIdx: nil,
				Key:          "%v",
				KeyIdx:       []int{0},
				Rpt:          "",
			},
			params:    []string{},
			expect:    KafkaTopicInfo{"test-topic", "", ""},
			expectErr: true,
		},
	}

	for i, tt := range tests {
		got, err := tt.input.Create(tt.params)
		if tt.expectErr {
			require.Error(t, err)
			continue
		}
		require.NoError(t, err)
		require.Equal(t, tt.expect, got, "index %d", i)
	}
}

func TestProcessMqttUserProperty(t *testing.T) {
	tests := []struct {
		input        []packets.UserProperty
		expectHeader []sarama.RecordHeader
		expectUUID   string
	}{
		{
			input:        nil,
			expectHeader: nil,
			expectUUID:   "",
		},
		{
			input:        []packets.UserProperty{},
			expectHeader: nil,
			expectUUID:   "",
		},
		{
			input: []packets.UserProperty{
				{Key: "uuid", Val: "7306dca4-05a6-11ee-be56-0242ac120002"},
				{Key: "test", Val: "abc"},
			},
			expectHeader: []sarama.RecordHeader{
				{Key: []byte("uuid"), Value: []byte("7306dca4-05a6-11ee-be56-0242ac120002")},
				{Key: []byte("test"), Value: []byte("abc")},
			},
			expectUUID: "7306dca4-05a6-11ee-be56-0242ac120002",
		},
		{
			input: []packets.UserProperty{
				{Key: "test", Val: "abc"},
			},
			expectHeader: []sarama.RecordHeader{
				{Key: []byte("test"), Value: []byte("abc")},
			},
			expectUUID: "",
		},
	}

	for i, tt := range tests {
		gotHeader, gotUUID := processMqttUserProperty(tt.input)
		require.Equal(t, tt.expectHeader, gotHeader, "index %d", i)
		require.Equal(t, tt.expectUUID, gotUUID, "index %d", i)
	}
}

func TestGetUUIDTime(t *testing.T) {
	tests := []struct {
		input      string
		expectTime time.Time
		expectErr  bool
	}{
		{
			input:      "2e4c6bf6-aa1a-4147-b34a-d93cd3b8911e", // UUID v4
			expectTime: time.Time{},
			expectErr:  true,
		},
		{
			input:      "7e526580-05c2-11ee-9207-88e9fe781439",
			expectTime: time.Unix(1686204339, 618752000),
			expectErr:  false,
		},
	}

	for i, tt := range tests {
		got, err := getUUIDTime(tt.input)
		if tt.expectErr {
			require.Error(t, err)
			continue
		}

		require.NoError(t, err)
		require.Equal(t, tt.expectTime, got, "index %d", i)
	}
}
