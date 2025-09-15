// kafka_service.go
// Package kafka provides a Kafka producer for sending Event messages.
//
// Example usage:
//
//	import (
//		"context"
//		"github.com/google/uuid"
//		"github.com/grasp/ds-event-streams/sdks/goland/kafka"
//		"github.com/grasp/ds-event-streams/sdks/goland/models"
//	)
//
//	// Create security credentials
//	security := kafka.Security{
//		Username: "kafka_user",
//		Password: "kafka_pass",
//	}
//
//	// Create producer configuration
//	config := kafka.DefaultConfig(security)
//	// Or customize:
//	// config := kafka.Config{
//	//     Brokers: []string{"localhost:9092", "localhost:9093"},
//	//     Security: security,
//	//     BatchSize: 50,
//	//     // ... other options
//	// }
//
//	// Create producer instance
//	producer, err := kafka.NewProducer(config)
//	if err != nil {
//		log.Fatal(err)
//	}
//	defer producer.Close()
//
//	// Create an event
//	event := models.EventJson{
//		Id:          uuid.New(),
//		SessionId:   uuid.New(),
//		RequestId:   uuid.New(),
//		TenantId:    uuid.New(),
//		EventType:   "file.uploaded.v1",
//		EventSource: "uploader-service",
//		Payload:     &map[string]interface{}{"key": "value"},
//		// ... other fields
//	}
//
//	// Send single event to a topic
//	err = producer.SendEvent(context.Background(), "events-topic", event)
//	if err != nil {
//		log.Printf("Failed to send event: %v", err)
//	}
//
//	// Send batch of events to a topic
//	events := []models.EventJson{event, /* more events */}
//	err = producer.SendEvents(context.Background(), "events-topic", events, nil)
//	if err != nil {
//		log.Printf("Failed to send events: %v", err)
//	}
//
//	// Send with custom headers
//	headers := []kafka.Header{
//		{Key: "source", Value: "my-service"},
//		{Key: "version", Value: "1.0"},
//	}
//	err = producer.SendEvent(context.Background(), "events-topic", event, headers...)
//	if err != nil {
//		log.Printf("Failed to send event with headers: %v", err)
//	}
package kafka

import (
	"context"
	"encoding/json"
	"errors"
	"time"

	"github.com/grasp/ds-event-streams/sdks/goland/models"
	"github.com/segmentio/kafka-go"
	"github.com/segmentio/kafka-go/sasl/scram"
)

// Producer wraps a kafka-go Writer for sending model messages to any topic.
type Producer struct {
	w      *kafka.Writer
	client *kafka.Client
}

type Security struct {
	Username string
	Password string
}

// Config controls writer behavior. Tune as needed.
type Config struct {
	Brokers                []string // e.g. []string{"broker1:9092","broker2:9092"}
	Security               Security
	RequiredAcks           kafka.RequiredAcks
	Balancer               kafka.Balancer
	BatchSize              int
	BatchBytes             int64
	BatchTimeout           time.Duration
	Compression            kafka.Compression
	Async                  bool
	AllowAutoTopicCreation bool
	WriteTimeout           time.Duration // per-message write timeout
}

// DefaultConfig gives sensible production-ish defaults.
func DefaultConfig(security Security) Config {
	return Config{
		Brokers:                []string{"b0.kafka.ds.local:9095"},
		Security:               security,
		RequiredAcks:           kafka.RequireOne,
		Balancer:               &kafka.Hash{}, // stable by key
		BatchSize:              100,
		BatchBytes:             1 << 20, // 1 MiB
		BatchTimeout:           50 * time.Millisecond,
		Compression:            kafka.Snappy,
		Async:                  false,
		AllowAutoTopicCreation: true,
		WriteTimeout:           10 * time.Second,
	}
}

func NewProducer(cfg Config) (*Producer, error) {
	if len(cfg.Brokers) == 0 {
		return nil, errors.New("kafka: no brokers provided")
	}

	transport := &kafka.Transport{}
	mesh, _ := scram.Mechanism(scram.SHA512, cfg.Security.Username, cfg.Security.Password)
	transport.SASL = mesh

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

func (p *Producer) Close() error {
	if p == nil || p.w == nil {
		return nil
	}
	return p.w.Close()
}

// checkTopicWritePermission checks if we can write to a topic by attempting to get topic metadata
func (p *Producer) checkTopicWritePermission(ctx context.Context, topic string) error {
	if p == nil || p.client == nil {
		return errors.New("kafka: producer not initialized")
	}

	// Try to get topic metadata - this will fail if we don't have permission
	req := &kafka.MetadataRequest{
		Topics: []string{topic},
	}

	_, err := p.client.Metadata(ctx, req)
	if err != nil {
		// Check for specific authorization error patterns
		errStr := err.Error()
		if containsAny(errStr, []string{"authorization", "access denied", "not authorized", "permission denied"}) {
			return errors.New("kafka: access denied to topic '" + topic + "' - " + err.Error())
		}
		// For other errors (network, timeout, etc.), we'll allow the send to proceed
		// as the actual write operation might still succeed
		return nil
	}

	return nil
}

// containsAny checks if the string contains any of the substrings
func containsAny(s string, substrs []string) bool {
	for _, substr := range substrs {
		if len(s) >= len(substr) {
			for i := 0; i <= len(s)-len(substr); i++ {
				if s[i:i+len(substr)] == substr {
					return true
				}
			}
		}
	}
	return false
}

// Header is a simple string header.
type Header struct {
	Key   string
	Value string
}

// SendEvent JSON-encodes the event and writes it to the specified Kafka topic.
// Partition key: evt.Id (falls back to SessionId if zero).
// Checks topic write permissions using kafka client before sending.
func (p *Producer) SendEvent(ctx context.Context, topic string, evt models.EventJson, headers ...Header) error {
	if p == nil || p.w == nil {
		return errors.New("kafka: producer not initialized")
	}

	// Check ACL permissions before sending using kafka client
	if err := p.checkTopicWritePermission(ctx, topic); err != nil {
		return err
	}

	// Marshal Event to JSON (uuid.UUID fields serialize as strings).
	buf, err := json.Marshal(evt)
	if err != nil {
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

	return p.w.WriteMessages(writeCtx, kafka.Message{
		Topic:   topic,
		Key:     []byte(key),
		Value:   buf,
		Headers: kh,
		Time:    time.Now(),
	})
}

// SendEvents sends a batch of events efficiently to the specified topic.
// keys[i] is optional; if provided, used for partitioning.
// Checks topic write permissions using kafka client before sending.
func (p *Producer) SendEvents(ctx context.Context, topic string, evts []models.EventJson, keys []string, headers ...Header) error {
	if p == nil || p.w == nil {
		return errors.New("kafka: producer not initialized")
	}
	if len(evts) == 0 {
		return nil
	}
	if len(keys) > 0 && len(keys) != len(evts) {
		return errors.New("kafka: len(keys) must match len(evts) or be zero")
	}

	// Check ACL permissions before sending using kafka client
	if err := p.checkTopicWritePermission(ctx, topic); err != nil {
		return err
	}

	kh := make([]kafka.Header, 0, len(headers)+2)
	kh = append(kh,
		kafka.Header{Key: "content-type", Value: []byte("application/json")},
		kafka.Header{Key: "message-type", Value: []byte("Event")},
	)
	for _, h := range headers {
		kh = append(kh, kafka.Header{Key: h.Key, Value: []byte(h.Value)})
	}

	msgs := make([]kafka.Message, 0, len(evts))
	for i, e := range evts {
		b, err := json.Marshal(e)
		if err != nil {
			return err
		}
		var k []byte
		if len(keys) > 0 {
			k = []byte(keys[i])
		} else {
			k = []byte(e.Id.String())
			if string(k) == "00000000-0000-0000-0000-000000000000" {
				k = []byte(e.SessionId.String())
			}
		}
		msgs = append(msgs, kafka.Message{
			Topic:   topic,
			Key:     k,
			Value:   b,
			Headers: kh,
			Time:    time.Now(),
		})
	}

	writeCtx := ctx
	if deadline, has := ctx.Deadline(); !has || time.Until(deadline) <= 0 {
		var cancel context.CancelFunc
		writeCtx, cancel = context.WithTimeout(ctx, 20*time.Second)
		defer cancel()
	}

	return p.w.WriteMessages(writeCtx, msgs...)
}
