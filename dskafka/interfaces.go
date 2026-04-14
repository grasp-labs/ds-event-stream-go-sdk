package dskafka

import (
	"context"

	"github.com/segmentio/kafka-go"
)

// kafkaWriter is an internal interface that abstracts kafka.Writer for testing.
// This allows dependency injection of mock writers in tests without changing production code.
type kafkaWriter interface {
	WriteMessages(ctx context.Context, msgs ...kafka.Message) error
	Close() error
}

// kafkaReader is an internal interface that abstracts kafka.Reader for testing.
// This allows dependency injection of mock readers in tests without changing production code.
type kafkaReader interface {
	ReadMessage(ctx context.Context) (kafka.Message, error)
	CommitMessages(ctx context.Context, msgs ...kafka.Message) error
	Stats() kafka.ReaderStats
	Close() error
}

// Ensure kafka.Writer implements kafkaWriter interface
var _ kafkaWriter = (*kafka.Writer)(nil)

// Ensure kafka.Reader implements kafkaReader interface
var _ kafkaReader = (*kafka.Reader)(nil)
