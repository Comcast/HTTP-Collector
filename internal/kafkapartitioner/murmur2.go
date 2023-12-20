package kafkapartitioner

import (
	"strings"

	"github.com/IBM/sarama"
	"github.com/aviddiviner/go-murmur"
)

type PartitionerConstructor struct {
	murmur2Topics []string
}

// NewPartitionerConstructor returns a PartitionerConstructor that selects Murmur2 for topic with
// prefix with any of the murmur2topics, otherwise uses default Sarama HashParitition for others.
func NewPartitionerConstructor(murmur2Topics []string) sarama.PartitionerConstructor {
	p := &PartitionerConstructor{murmur2Topics}
	return p.Select
}

func (p *PartitionerConstructor) Select(topic string) sarama.Partitioner {
	for _, t := range p.murmur2Topics {
		if strings.HasPrefix(topic, t) {
			return NewMurmur2(topic)
		}
	}

	return sarama.NewHashPartitioner(topic)
}

type Murmur2 struct {
	random sarama.Partitioner
}

// NewMurmur2 returns a partitioner that matches Java Kafka client murmur2 partitiooner.
func NewMurmur2(topic string) sarama.Partitioner {
	p := new(Murmur2)
	p.random = sarama.NewRandomPartitioner(topic)
	return p
}

func (p *Murmur2) Partition(message *sarama.ProducerMessage, numPartitions int32) (int32, error) {
	const seed uint32 = 0x9747b28c
	if message.Key == nil {
		return p.random.Partition(message, numPartitions)
	}
	bytes, err := message.Key.Encode()
	if err != nil {
		return -1, err
	}
	hash := murmur.MurmurHash2(bytes, seed)
	part := toPositive(hash) % numPartitions
	return part, nil
}

// toPositive returns positive value as in Java Kafka client:
// https://github.com/apache/kafka/blob/1.0.0/clients/src/main/java/org/apache/kafka/common/utils/Utils.java#L728
func toPositive(n uint32) int32 {
	return int32(n) & 0x7fffffff
}

func (p *Murmur2) RequiresConsistency() bool {
	return true
}
