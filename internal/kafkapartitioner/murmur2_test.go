package kafkapartitioner

import (
	"testing"

	"github.com/IBM/sarama"
	"github.com/aviddiviner/go-murmur"
	"github.com/stretchr/testify/require"
)

func TestNewPartitionerConstructor(t *testing.T) {
	murmur2Topics := []string{"murmur-test-topic1", "murmur-test-topic2"}
	partConstructor := NewPartitionerConstructor(murmur2Topics)
	tests := []struct {
		topic  string
		expect sarama.Partitioner
	}{
		{"murmur-test-topic1", NewMurmur2("test")},
		{"murmur-test-topic2", NewMurmur2("test")},
		{"test", sarama.NewHashPartitioner("test")},
		{"default", sarama.NewHashPartitioner("test")},
	}
	for _, tt := range tests {
		p := partConstructor(tt.topic)
		require.IsType(t, tt.expect, p)
	}
}

func TestMurmur2Hash(t *testing.T) {
	tests := []struct {
		key    string
		expect int32
	}{
		{"637757d3febb3d174adf4c3b", 1353579763},
		{"6385b31064eb220d52a86c15", -2097433329},
		{"605b498638a2d562ca2ebca3", -1035781448},
		{"6387abd07264feedc175eb9f", 744020752},
		{"637b531eabfc000d4bc42991", -1482987632},
	}

	const seed uint32 = 0x9747b28c
	for _, tt := range tests {
		hash := murmur.MurmurHash2([]byte(tt.key), seed)
		require.Equal(t, tt.expect, int32(hash))
	}
}

func TestMurmur2(t *testing.T) {
	tests := []struct {
		key     string
		numPart int32
		expect  int32
	}{
		{"637757d3febb3d174adf4c3b", 2, 1},
		{"6385b31064eb220d52a86c15", 2, 1},
		{"605b498638a2d562ca2ebca3", 2, 0},
		{"6387abd07264feedc175eb9f", 2, 0},
		{"637b531eabfc000d4bc42991", 2, 0},
		{"5fb070817ecfc804387a1047", 180, 80},
		{"60937fcb8935f5401c3cfeb9", 180, 137},
		{"5fffc56dc12c0e3984c25037", 180, 47},
		{"6074dbf606338645f5075fd4", 180, 50},
		{"6127103b22a4d12c05ad1341", 180, 54},
		{"5faa1edbe6a33d5f545535ca", 180, 96},
		{"5f5beb457ae8b45b86d38706", 180, 179},
	}

	partitioner := new(Murmur2)
	for _, tt := range tests {
		msg := &sarama.ProducerMessage{
			Key: sarama.StringEncoder(tt.key),
		}
		part, err := partitioner.Partition(msg, tt.numPart)
		require.NoError(t, err)
		require.Equal(t, tt.expect, part)
	}
}
