package dskafka

import (
	"context"
	"encoding/json"
	"errors"
	"log"
	"time"

	"github.com/grasp-labs/ds-event-stream-go-sdk/models"
	"github.com/segmentio/kafka-go"
	"github.com/segmentio/kafka-go/sasl/scram"
)

// Producer wraps a kafka-go Writer for sending model messages to any topic.
type Producer struct {
	w      *kafka.Writer
	client *kafka.Client
}

// DefaultProducerConfig creates a Config with sensible production defaults for Kafka producers.
// The configuration includes:
//   - SASL SCRAM-SHA512 authentication
//   - Snappy compression
//   - 100 message batch size with 1MB batch bytes
//   - 50ms batch timeout
//   - RequireOne acknowledgment level
//   - Hash-based partitioning
//   - Auto topic creation enabled
//
// Parameters:
//   - clientCredentials: Username and password for Kafka authentication
//   - bootstrapServers: List of Kafka broker addresses
//
// Returns a Config suitable for creating a Producer.
func DefaultProducerConfig(clientCredentials ClientCredentials, bootstrapServers []string) Config {
	return Config{
		Brokers:                bootstrapServers,
		ClientCredentials:      clientCredentials,
		RequiredAcks:           kafka.RequireOne,
		Balancer:               &kafka.Hash{}, // stable by key
		BatchSize:              100,
		BatchBytes:             1 << 20, // 1 MiB
		BatchTimeout:           50 * time.Millisecond,
		Compression:            kafka.Snappy,
		Async:                  false,
		AllowAutoTopicCreation: true,
		WriteTimeout:           10 * time.Second,

		// Consumer defaults
		Partition:      -1, // Read from all partitions
		MinBytes:       1,
		MaxBytes:       1 << 20, // 1 MiB
		MaxWait:        500 * time.Millisecond,
		ReadTimeout:    10 * time.Second,
		CommitInterval: 1 * time.Second,
		StartOffset:    kafka.FirstOffset,
	}
}

// NewProducer creates a new Kafka producer with the specified configuration.
// The producer uses SASL SCRAM-SHA512 authentication and establishes a connection
// to the Kafka cluster. It validates that at least one broker is provided.
//
// The producer supports:
//   - Batching for efficient message delivery
//   - Compression to reduce network overhead
//   - Configurable acknowledgment levels
//   - Custom partitioning strategies
//   - Auto topic creation
//
// Parameters:
//   - cfg: Configuration containing brokers, credentials, and producer settings
//
// Returns:
//   - *Producer: A new producer instance ready to send messages
//   - error: Any error that occurred during initialization
//
// Example:
//
//	config := DefaultProducerConfig(credentials, brokers)
//	producer, err := NewProducer(config)
//	if err != nil {
//	    log.Fatal(err)
//	}
//	defer producer.Close()
func NewProducer(cfg Config) (*Producer, error) {
	if len(cfg.Brokers) == 0 {
		log.Println("kafka: no brokers provided")
		return nil, errors.New("kafka: no brokers provided")
	}

	transport := &kafka.Transport{}
	mech, mecherr := scram.Mechanism(scram.SHA512, cfg.ClientCredentials.Username, cfg.ClientCredentials.Password)
	if mecherr != nil {
		log.Println("kafka: error setting up SASL mechanism:", mecherr)
		return nil, mecherr
	}
	transport.SASL = mech

	// Create client for ACL checks
	client := &kafka.Client{
		Addr:      kafka.TCP(cfg.Brokers...),
		Transport: transport,
	}

	w := &kafka.Writer{
		Addr:                   kafka.TCP(cfg.Brokers...),
		Balancer:               cfg.Balancer,
		RequiredAcks:           cfg.RequiredAcks,
		BatchSize:              cfg.BatchSize,
		BatchBytes:             cfg.BatchBytes,
		BatchTimeout:           cfg.BatchTimeout,
		Compression:            cfg.Compression,
		Transport:              transport,
		Async:                  cfg.Async,
		AllowAutoTopicCreation: cfg.AllowAutoTopicCreation,
	}
	return &Producer{w: w, client: client}, nil
}

// Close gracefully shuts down the producer and releases all resources.
// This method should be called when the producer is no longer needed,
// typically using defer immediately after successful creation.
//
// The method is safe to call multiple times and will not panic if
// the producer is nil or already closed.
//
// Returns:
//   - error: Any error that occurred during the close operation
//
// Example:
//
//	producer, err := NewProducer(config)
//	if err != nil {
//	    log.Fatal(err)
//	}
//	defer producer.Close() // Always close the producer
func (p *Producer) Close() error {
	if p == nil || p.w == nil {
		return nil
	}
	return p.w.Close()
}

// SendEvent JSON-encodes an EventJson and sends it to the specified Kafka topic.
// The event is serialized to JSON with UUID fields automatically converted to strings.
//
// Partitioning:
//   - Primary key: evt.Id (if not zero UUID)
//   - Fallback key: evt.SessionId (if Id is zero UUID)
//   - Ensures consistent partitioning for related events
//
// Headers:
//   - Automatically adds "content-type: application/json"
//   - Automatically adds "message-type: Event"
//   - Custom headers can be provided via headers parameter
//
// Timeouts:
//   - Uses context deadline if present
//   - Falls back to 10-second timeout if context has no deadline
//
// Parameters:
//   - ctx: Context for cancellation and timeout control
//   - topic: Target Kafka topic name
//   - evt: Event to send (must be valid EventJson with required fields)
//   - headers: Optional custom headers to add to the message
//
// Returns:
//   - error: Any error that occurred during JSON marshaling or message sending
//
// Example:
//
//	err := producer.SendEvent(ctx, "user-events", event,
//	    Header{Key: "source", Value: "user-service"})
func (p *Producer) SendEvent(ctx context.Context, topic string, evt models.EventJson, headers ...Header) error {
	if p == nil || p.w == nil {
		return errors.New("kafka: producer not initialized")
	}

	// Marshal Event to JSON (uuid.UUID fields serialize as strings).
	buf, err := json.Marshal(evt)
	if err != nil {
		log.Println("kafka: error marshalling event:", err)
		return err
	}

	// Choose a stable key for partitioning.
	key := evt.Id.String()
	if key == "00000000-0000-0000-0000-000000000000" {
		key = evt.SessionId.String()
	}

	kh := make([]kafka.Header, 0, len(headers)+2)
	// Helpful default headers (adjust/remove as needed)
	kh = append(kh,
		kafka.Header{Key: "content-type", Value: []byte("application/json")},
		kafka.Header{Key: "message-type", Value: []byte("Event")},
	)
	for _, h := range headers {
		kh = append(kh, kafka.Header{Key: h.Key, Value: []byte(h.Value)})
	}

	writeCtx := ctx
	if deadline, has := ctx.Deadline(); !has || time.Until(deadline) <= 0 {
		var cancel context.CancelFunc
		writeCtx, cancel = context.WithTimeout(ctx, 10*time.Second)
		defer cancel()
	}

	werr := p.w.WriteMessages(writeCtx, kafka.Message{
		Topic:   topic,
		Key:     []byte(key),
		Value:   buf,
		Headers: kh,
		Time:    time.Now(),
	})

	if werr != nil {
		log.Println("kafka: error writing message:", werr)
		return werr
	}
	return nil
}
