package dskafka

import (
	"context"

	"github.com/segmentio/kafka-go"
)

// KafkaWriter abstracts kafka.Writer for dependency injection and testing.
type KafkaWriter interface {
	WriteMessages(ctx context.Context, msgs ...kafka.Message) error
	Close() error
}

// KafkaReader abstracts kafka.Reader for dependency injection and testing.
type KafkaReader interface {
	ReadMessage(ctx context.Context) (kafka.Message, error)
	CommitMessages(ctx context.Context, msgs ...kafka.Message) error
	Stats() kafka.ReaderStats
	Close() error
}

// Backward-compatible internal aliases.
type kafkaWriter = KafkaWriter
type kafkaReader = KafkaReader

// Ensure kafka.Writer implements KafkaWriter interface.
var _ KafkaWriter = (*kafka.Writer)(nil)

// Ensure kafka.Reader implements KafkaReader interface.
var _ KafkaReader = (*kafka.Reader)(nil)
