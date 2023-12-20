package mqttfederation

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestToDvcTopicsValidate(t *testing.T) {
	tdt, err := NewToDvcTopics("device")
	require.NoError(t, err)

	tests := []struct {
		topic string
		valid bool
	}{
		{"device/to/123/abc", true},
		{"device/to/123/abc/extra", false},
		{"non-existing-topic", false},
		{"", false},
	}

	for _, tc := range tests {
		_, valid := tdt.Validate(tc.topic)
		require.Equal(t, tc.valid, valid)
	}
}

func TestFrDvcTopicsValidate(t *testing.T) {
	tdt, err := NewFrDvcTopics("device")
	require.NoError(t, err)

	tests := []struct {
		topic string
		valid bool
	}{
		{"device/fr/a/b/c/d", true},
		{"device/fr/{a}/{b}/{c}/{d}", true},
		{"device/fr/a/b/c/d/", false},
		{"device/fr/a/b/c/d/abc", false},
		{"device/fr/a/b/c/", false},
		{"", false},
	}

	for _, tc := range tests {
		_, valid := tdt.Validate(tc.topic)
		require.Equal(t, tc.valid, valid)
	}
}

func TestCreateMetricsName(t *testing.T) {
	tests := []struct {
		topic  string
		expect string
	}{
		{"device/fr/{a}/{b}/webconfig/poke", "device/fr/webconfig/poke"},
		{"device/fr/{a}/{b}/{c}/{d}", "device/fr/{c}/{d}"},
		{"test/chi/{b}/{e}", "test"},
		{"test/abc/chi/{b}/{e}", "test/abc"},
		{"{something}", ""},
		{"", ""},
	}

	for _, tc := range tests {
		got := createMetricsName(tc.topic, "device")
		require.Equal(t, tc.expect, got)
	}
}
