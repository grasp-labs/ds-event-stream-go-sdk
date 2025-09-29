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

// Consumer wraps a kafka-go Reader for reading model messages from topics.
type Consumer struct {
	config  Config
	client  *kafka.Client
	readers map[string]*kafka.Reader // topic -> reader mapping
}

type ClientCredentials struct {
	Username string
	Password string
}

//go:generate stringer -type=Environment
type Environment int

const (
	Dev Environment = iota
	Prod
)

// Header is a simple string header.
type Header struct {
	Key   string
	Value string
}

// Config controls producer and consumer behavior. Tune as needed.
type Config struct {
	Brokers                []string // e.g. []string{"broker1:9092","broker2:9092"}
	ClientCredentials      ClientCredentials
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

func GetBootstrapServers(environment Environment, useInternalHostnames bool) []string {
	switch environment {
	case Prod:
		if useInternalHostnames {
			return []string{"kafka.kafka.svc.cluster.local:9092"}
		} else {
			return []string{"b0.kafka.ds.local:9095", "b1.kafka.ds.local:9095", "b2.kafka.ds.local:9095"}
		}
	default: // Dev
		if useInternalHostnames {
			return []string{"kafka.kafka-dev.svc.cluster.local:9092"}
		} else {
			return []string{"b0.dev.kafka.ds.local:9095"}
		}
	}
}

// DefaultConfig gives sensible production-ish defaults.
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

func DefaultConsumerConfig(clientCredentials ClientCredentials, bootstrapServers []string, groupID string) Config {
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
		GroupID:        groupID,
	}
}

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

func NewConsumer(cfg Config) (*Consumer, error) {
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
			log.Println("kafka: error closing reader:", err)
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
		log.Println("kafka: error setting offset:", err)
		if errClose := reader.Close(); errClose != nil {
			return nil, errors.Join(err, errClose)
		}
		return nil, err
	}

	c.readers[key] = reader
	return reader, nil
}

// SendEvent JSON-encodes the event and writes it to the specified Kafka topic.
// Partition key: evt.Id (falls back to SessionId if zero).
// Checks topic write permissions using kafka client before sending.
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

// ReadEvent reads a single event from the specified topic and returns it as EventJson.
// Blocks until a message is available or context is cancelled.
// groupID is optional - if empty, will read from all partitions without consumer group semantics.
func (c *Consumer) ReadEvent(ctx context.Context, topic string, groupID ...string) (*models.EventJson, error) {
	if c == nil {
		log.Println("kafka: consumer not initialized")
		return nil, errors.New("kafka: consumer not initialized")
	}
	if topic == "" {
		log.Println("kafka: topic is required")
		return nil, errors.New("kafka: topic is required")
	}

	gid := ""
	if len(groupID) > 0 {
		gid = groupID[0]
	} else if c.config.GroupID != "" {
		gid = c.config.GroupID
	}

	reader, err := c.getOrCreateReader(topic, gid)
	if err != nil {
		log.Println("kafka: error getting reader:", err)
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
		log.Println("kafka: error reading message:", err)
		return nil, err
	}

	var event models.EventJson
	if err := json.Unmarshal(msg.Value, &event); err != nil {
		log.Println("kafka: failed to unmarshal event:", err)
		return nil, errors.New("kafka: failed to unmarshal event - " + err.Error())
	}

	return &event, nil
}

// CommitEvents manually commits the current offset for a specific topic reader.
// Only needed if using manual commit mode.
func (c *Consumer) CommitEvents(ctx context.Context, topic string, msgs ...kafka.Message) error {
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
