// kafka_service.go
// Package kafka provides a Kafka producer and consumer for Event messages.
//
// Producer Example usage:
//
//	import (
//		"context"
//		"github.com/google/uuid"
//		"github.com/grasp-labs/ds-event-stream-go-sdk/kafka"
//		"github.com/grasp-labs/ds-event-stream-go-sdk/models"
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
//
// Consumer Example usage:
//
//	// Create consumer configuration
//	config := kafka.DefaultConfig(security)
//	config.GroupID = "my-consumer-group" // optional
//	// Or customize:
//	// config := kafka.Config{
//	//     Brokers: []string{"localhost:9092", "localhost:9093"},
//	//     Security: security,
//	//     GroupID: "my-consumer-group", // optional
//	//     // ... other options
//	// }
//
//	// Create consumer instance
//	consumer, err := kafka.NewConsumer(config)
//	if err != nil {
//		log.Fatal(err)
//	}
//	defer consumer.Close()
//
//	// Read single event from topic
//	event, err := consumer.ReadEvent(context.Background(), "events-topic")
//	if err != nil {
//		log.Printf("Failed to read event: %v", err)
//	} else {
//		log.Printf("Received event: %s", event.EventType)
//	}
//
//	// Read with specific consumer group
//	event, err = consumer.ReadEvent(context.Background(), "events-topic", "my-group")
//	if err != nil {
//		log.Printf("Failed to read event: %v", err)
//	}
//
//	// Read multiple events from topic
//	events, err := consumer.ReadEvents(context.Background(), "events-topic", 10)
//	if err != nil {
//		log.Printf("Failed to read events: %v", err)
//	} else {
//		log.Printf("Received %d events", len(events))
//	}
package kafka

import (
	"context"
	"encoding/json"
	"errors"
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

// Consumer wraps a kafka-go Reader for reading model messages from topics.
type Consumer struct {
	config  Config
	client  *kafka.Client
	readers map[string]*kafka.Reader // topic -> reader mapping
}

type Security struct {
	Username string
	Password string
}

// Config controls producer and consumer behavior. Tune as needed.
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

	// Consumer-specific fields (optional)
	GroupID        string        // Consumer group ID (optional, can be set per read operation)
	Partition      int           // Partition to read from (use -1 for all partitions)
	MinBytes       int           // Minimum number of bytes to fetch in each request
	MaxBytes       int           // Maximum number of bytes to fetch in each request
	MaxWait        time.Duration // Maximum amount of time to wait for messages
	ReadTimeout    time.Duration // per-message read timeout
	CommitInterval time.Duration // How often to commit offsets
	StartOffset    int64         // Where to start reading (kafka.FirstOffset or kafka.LastOffset)
}

// DefaultConfig gives sensible production-ish defaults.
func DefaultConfig(security Security, environment string, useInternalHostnames bool, customBrokers []string) Config {

	brokers := customBrokers
	if len(customBrokers) == 0 {
		switch environment {
		case "dev":
			if useInternalHostnames {
				brokers = []string{"kafka.kafka-dev.svc.cluster.local:9092"}
			} else {
				brokers = []string{"b0.dev.kafka.ds.local:9095"}
			}
		case "prod":
			if useInternalHostnames {
				brokers = []string{"kafka.kafka.svc.cluster.local:9092"}
			} else {
				brokers = []string{"b0.kafka.ds.local:9095", "b1.kafka.ds.local:9095", "b2.kafka.ds.local:9095"}
			}
		}
	}

	return Config{
		Brokers:                brokers, // []string{"b0.dev.kafka.ds.local:9095"},
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

func NewConsumer(cfg Config) (*Consumer, error) {
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

	return &Consumer{
		config:  cfg,
		client:  client,
		readers: make(map[string]*kafka.Reader),
	}, nil
}

func (p *Producer) Close() error {
	if p == nil || p.w == nil {
		return nil
	}
	return p.w.Close()
}

func (c *Consumer) Close() error {
	if c == nil || c.readers == nil {
		return nil
	}

	var lastErr error
	for _, reader := range c.readers {
		if err := reader.Close(); err != nil {
			lastErr = err
		}
	}
	return lastErr
}

// getOrCreateReader gets or creates a reader for the specified topic and groupID
func (c *Consumer) getOrCreateReader(topic, groupID string) (*kafka.Reader, error) {
	if c == nil {
		return nil, errors.New("kafka: consumer not initialized")
	}

	key := topic
	if groupID != "" {
		key = groupID + ":" + topic
	}

	if reader, exists := c.readers[key]; exists {
		return reader, nil
	}

	readerConfig := kafka.ReaderConfig{
		Brokers:        c.config.Brokers,
		Topic:          topic,
		GroupID:        groupID,
		MinBytes:       c.config.MinBytes,
		MaxBytes:       c.config.MaxBytes,
		MaxWait:        c.config.MaxWait,
		CommitInterval: c.config.CommitInterval,
		StartOffset:    c.config.StartOffset,
	}

	// Set partition if specified (for partition-specific reading)
	if c.config.Partition >= 0 {
		readerConfig.Partition = c.config.Partition
		readerConfig.GroupID = "" // Can't use group ID with specific partition
	}

	reader := kafka.NewReader(readerConfig)
	if err := reader.SetOffsetAt(context.Background(), time.Now()); err != nil {
		if errClose := reader.Close(); errClose != nil {
			return nil, errors.Join(err, errClose)
		}
		return nil, err
	}

	c.readers[key] = reader
	return reader, nil
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

// checkTopicReadPermission checks if we can read from a topic by attempting to get topic metadata
func (c *Consumer) checkTopicReadPermission(ctx context.Context, topic string) error {
	if c == nil || c.client == nil {
		return errors.New("kafka: consumer not initialized")
	}

	// Try to get topic metadata - this will fail if we don't have permission
	req := &kafka.MetadataRequest{
		Topics: []string{topic},
	}

	_, err := c.client.Metadata(ctx, req)
	if err != nil {
		// Check for specific authorization error patterns
		errStr := err.Error()
		if containsAny(errStr, []string{"authorization", "access denied", "not authorized", "permission denied"}) {
			return errors.New("kafka: access denied to topic '" + topic + "' - " + err.Error())
		}
		// For other errors (network, timeout, etc.), we'll allow the read to proceed
		// as the actual read operation might still succeed
		return nil
	}

	return nil
}

// ReadEvent reads a single event from the specified topic and returns it as EventJson.
// Blocks until a message is available or context is cancelled.
// groupID is optional - if empty, will read from all partitions without consumer group semantics.
func (c *Consumer) ReadEvent(ctx context.Context, topic string, groupID ...string) (*models.EventJson, error) {
	if c == nil {
		return nil, errors.New("kafka: consumer not initialized")
	}
	if topic == "" {
		return nil, errors.New("kafka: topic is required")
	}

	gid := ""
	if len(groupID) > 0 {
		gid = groupID[0]
	} else if c.config.GroupID != "" {
		gid = c.config.GroupID
	}

	// Check ACL permissions before reading
	if err := c.checkTopicReadPermission(ctx, topic); err != nil {
		return nil, err
	}

	reader, err := c.getOrCreateReader(topic, gid)
	if err != nil {
		return nil, err
	}

	readCtx := ctx
	if deadline, has := ctx.Deadline(); !has || time.Until(deadline) <= 0 {
		var cancel context.CancelFunc
		readCtx, cancel = context.WithTimeout(ctx, 10*time.Second)
		defer cancel()
	}

	msg, err := reader.ReadMessage(readCtx)
	if err != nil {
		return nil, err
	}

	var event models.EventJson
	if err := json.Unmarshal(msg.Value, &event); err != nil {
		return nil, errors.New("kafka: failed to unmarshal event - " + err.Error())
	}

	return &event, nil
}

// ReadEvents reads multiple events from the specified topic with a specified limit.
// Returns up to 'limit' events or all available events if fewer are available.
// Blocks until at least one message is available or context is cancelled.
// groupID is optional - if empty, will read from all partitions without consumer group semantics.
func (c *Consumer) ReadEvents(ctx context.Context, topic string, limit int, groupID ...string) ([]models.EventJson, error) {
	if c == nil {
		return nil, errors.New("kafka: consumer not initialized")
	}
	if topic == "" {
		return nil, errors.New("kafka: topic is required")
	}
	if limit <= 0 {
		return nil, errors.New("kafka: limit must be greater than 0")
	}

	gid := ""
	if len(groupID) > 0 {
		gid = groupID[0]
	} else if c.config.GroupID != "" {
		gid = c.config.GroupID
	}

	// Check ACL permissions before reading
	if err := c.checkTopicReadPermission(ctx, topic); err != nil {
		return nil, err
	}

	reader, err := c.getOrCreateReader(topic, gid)
	if err != nil {
		return nil, err
	}

	readCtx := ctx
	if deadline, has := ctx.Deadline(); !has || time.Until(deadline) <= 0 {
		var cancel context.CancelFunc
		readCtx, cancel = context.WithTimeout(ctx, 30*time.Second)
		defer cancel()
	}

	events := make([]models.EventJson, 0, limit)

	for i := 0; i < limit; i++ {
		msg, err := reader.ReadMessage(readCtx)
		if err != nil {
			if i > 0 {
				// Return what we have if we got some messages
				break
			}
			return nil, err
		}

		var event models.EventJson
		if err := json.Unmarshal(msg.Value, &event); err != nil {
			// Skip malformed messages and continue
			continue
		}

		events = append(events, event)
	}

	return events, nil
}

// CommitMessages manually commits the current offset for a specific topic reader.
// Only needed if using manual commit mode.
func (c *Consumer) CommitMessages(ctx context.Context, topic string, msgs ...kafka.Message) error {
	if c == nil {
		return errors.New("kafka: consumer not initialized")
	}
	if topic == "" {
		return errors.New("kafka: topic is required")
	}

	// Find the reader for this topic
	var reader *kafka.Reader
	for key, r := range c.readers {
		if key == topic || (len(key) > len(topic) && key[len(key)-len(topic):] == topic) {
			reader = r
			break
		}
	}

	if reader == nil {
		return errors.New("kafka: no active reader for topic " + topic)
	}

	return reader.CommitMessages(ctx, msgs...)
}

// Stats returns reader statistics for a specific topic.
func (c *Consumer) Stats(topic string) (kafka.ReaderStats, error) {
	if c == nil {
		return kafka.ReaderStats{}, errors.New("kafka: consumer not initialized")
	}
	if topic == "" {
		return kafka.ReaderStats{}, errors.New("kafka: topic is required")
	}

	// Find the reader for this topic
	var reader *kafka.Reader
	for key, r := range c.readers {
		if key == topic || (len(key) > len(topic) && key[len(key)-len(topic):] == topic) {
			reader = r
			break
		}
	}

	if reader == nil {
		return kafka.ReaderStats{}, errors.New("kafka: no active reader for topic " + topic)
	}

	return reader.Stats(), nil
}
