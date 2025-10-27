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

// GetBootstrapServers returns the appropriate Kafka bootstrap servers for the specified environment.
// For Dev environment:
//   - External: ["b0.dev.kafka.ds.local:9095"]
//   - Internal: ["kafka-dev.kafka.svc.cluster.local:9092"]
//
// For Prod environment:
//   - External: ["b0.kafka.ds.local:9095", "b1.kafka.ds.local:9095", "b2.kafka.ds.local:9095"]
//   - Internal: ["kafka.kafka.svc.cluster.local:9092"]
//
// Parameters:
//   - environment: The target environment (Dev or Prod)
//   - useInternalHostnames: If true, returns internal cluster hostnames for in-cluster communication
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

// DefaultConsumerConfig creates a Config with sensible production defaults for Kafka consumers.
// The configuration includes:
//   - SASL SCRAM-SHA512 authentication
//   - Snappy compression
//   - 1MB max bytes per fetch request
//   - 500ms max wait time
//   - 1 second commit interval
//   - Reading from first offset
//   - All partitions (-1)
//
// Parameters:
//   - clientCredentials: Username and password for Kafka authentication
//   - bootstrapServers: List of Kafka broker addresses
//   - groupID: Consumer group ID for coordinated consumption
//
// Returns a Config suitable for creating a Consumer.
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

// NewProducer creates a new Kafka producer with the specified configuration.
// The producer uses SASL SCRAM-SHA512 authentication and establishes a connection
// to the Kafka cluster. It validates that at least one broker is provided.
//
// The producer supports:
//   - Batch sending with configurable size and timeout
//   - Multiple compression algorithms (Snappy, Gzip, LZ4, Zstd)
//   - Configurable acknowledgment levels (None, One, All)
//   - Custom partitioning strategies
//   - Automatic topic creation (if enabled)
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

// NewConsumer creates a new Kafka consumer with the specified configuration.
// The consumer uses SASL SCRAM-SHA512 authentication and establishes a connection
// to the Kafka cluster. It validates that at least one broker is provided.
//
// The consumer supports:
//   - Consumer group coordination for load balancing
//   - Configurable fetch sizes and timeouts
//   - Automatic offset management with configurable commit intervals
//   - Reading from specific partitions or all partitions
//   - Multiple topics with separate readers
//
// Parameters:
//   - cfg: Configuration containing brokers, credentials, and consumer settings
//
// Returns:
//   - *Consumer: A new consumer instance ready to read messages
//   - error: Any error that occurred during initialization
//
// Example:
//
//	config := DefaultConsumerConfig(credentials, brokers, "my-group")
//	consumer, err := NewConsumer(config)
//	if err != nil {
//	    log.Fatal(err)
//	}
//	defer consumer.Close()
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

// Close gracefully shuts down the consumer and releases all resources.
// This method closes all active topic readers and cleans up their connections.
// Any errors from closing individual readers are logged, but only the last
// error is returned.
//
// The method is safe to call multiple times and will not panic if
// the consumer is nil or already closed.
//
// Returns:
//   - error: The last error encountered while closing readers, or nil if all closed successfully
//
// Example:
//
//	consumer, err := NewConsumer(config)
//	if err != nil {
//	    log.Fatal(err)
//	}
//	defer consumer.Close() // Always close the consumer
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

// getOrCreateReader gets an existing reader or creates a new one for the specified topic and groupID.
// Readers are cached using a key combining groupID and topic to allow multiple readers
// for the same topic with different consumer groups.
//
// The reader is configured with the consumer's settings including:
//   - Fetch sizes (MinBytes, MaxBytes)
//   - Timeouts (MaxWait)
//   - Offset management (CommitInterval, StartOffset)
//   - Partition assignment (all partitions by default)
//
// If a specific partition is configured (>= 0), the reader will only read from that
// partition and consumer group functionality is disabled.
//
// Parameters:
//   - topic: The Kafka topic name to read from
//   - groupID: Consumer group ID for coordination (empty for no consumer group)
//
// Returns:
//   - *kafka.Reader: The reader instance for the topic/group combination
//   - error: Any error that occurred during reader creation or offset setting
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

	// Create SASL mechanism for the reader
	mechanism, err := scram.Mechanism(scram.SHA512, c.config.ClientCredentials.Username, c.config.ClientCredentials.Password)
	if err != nil {
		return nil, err
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
		// Add SASL authentication transport
		Dialer: &kafka.Dialer{
			Timeout:       10 * time.Second,
			DualStack:     true,
			SASLMechanism: mechanism,
		},
	}

	// Set partition if specified (for partition-specific reading)
	if c.config.Partition >= 0 {
		readerConfig.Partition = c.config.Partition
		readerConfig.GroupID = "" // Can't use group ID with specific partition
	}

	reader := kafka.NewReader(readerConfig)

	// Only set offset manually when NOT using consumer groups
	// Consumer groups manage their own offsets automatically
	if groupID == "" {
		if err := reader.SetOffsetAt(context.Background(), time.Now()); err != nil {
			log.Println("kafka: error setting offset:", err)
			if errClose := reader.Close(); errClose != nil {
				return nil, errors.Join(err, errClose)
			}
			return nil, err
		}
	}

	c.readers[key] = reader
	return reader, nil
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
//   - Automatically adds "content-type: application/json" and "message-type: Event"
//   - Custom headers can be provided via the headers parameter
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

// ReadEvent reads a single event from the specified Kafka topic and deserializes it to EventJson.
// This method blocks until a message is available, the context is cancelled, or a timeout occurs.
//
// Consumer Group Behavior:
//   - If groupID is provided, uses consumer group semantics for load balancing
//   - If groupID is empty, uses the consumer's default GroupID from config
//   - If no group ID at all, reads from all partitions without coordination
//
// Message Processing:
//   - Automatically deserializes JSON to EventJson struct
//   - UUID fields are automatically parsed from JSON strings
//   - Invalid JSON messages return an error
//
// Timeouts:
//   - Uses context deadline if present
//   - Falls back to 10-second timeout if context has no deadline
//
// Parameters:
//   - ctx: Context for cancellation and timeout control
//   - topic: Kafka topic name to read from
//   - groupID: Optional consumer group ID (first element used if multiple provided)
//
// Returns:
//   - *models.EventJson: The deserialized event, or nil if error occurred
//   - error: Any error during reading or JSON unmarshaling
//
// Example:
//
//	event, err := consumer.ReadEvent(ctx, "user-events", "my-group")
//	if err != nil {
//	    log.Printf("Failed to read: %v", err)
//	}
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

// ReadEventWithMessage reads an event from the specified Kafka topic and returns both
// the parsed event and the raw Kafka message for manual offset management.
// This method is useful when you need fine-grained control over when to commit offsets.
//
// The method works similarly to ReadEvent but additionally returns the kafka.Message
// which can be used with CommitEvents for manual offset commits.
//
// Parameters:
//   - ctx: Context for cancellation and timeout control
//   - topic: The topic to read from
//   - groupID: Optional consumer group ID (overrides config if provided)
//
// Returns:
//   - *models.EventJson: The parsed event data
//   - kafka.Message: The raw Kafka message (needed for commits)
//   - error: Any error that occurred during reading
//
// Example:
//
//	event, msg, err := consumer.ReadEventWithMessage(ctx, "user-events")
//	if err != nil {
//	    return err
//	}
//
//	// Process the event...
//	processEvent(event)
//
//	// Commit the message after successful processing
//	err = consumer.CommitEvents(ctx, "user-events", msg)
func (c *Consumer) ReadEventWithMessage(ctx context.Context, topic string, groupID ...string) (*models.EventJson, kafka.Message, error) {
	if c == nil {
		log.Println("kafka: consumer not initialized")
		return nil, kafka.Message{}, errors.New("kafka: consumer not initialized")
	}
	if topic == "" {
		log.Println("kafka: topic is required")
		return nil, kafka.Message{}, errors.New("kafka: topic is required")
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
		return nil, kafka.Message{}, err
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
		return nil, kafka.Message{}, err
	}

	var event models.EventJson
	if err := json.Unmarshal(msg.Value, &event); err != nil {
		log.Println("kafka: failed to unmarshal event:", err)
		return nil, kafka.Message{}, errors.New("kafka: failed to unmarshal event - " + err.Error())
	}

	return &event, msg, nil
}

// CommitEvents manually commits the offset for specific messages on a topic.
// This method is typically used when automatic offset commits are disabled
// and you want fine-grained control over when offsets are committed.
//
// The method finds the appropriate reader for the specified topic and commits
// the provided messages. This ensures that these messages won't be redelivered
// if the consumer restarts.
//
// Usage:
//   - Call after successfully processing messages
//   - Only needed if CommitInterval is set to a very large value or manual mode
//   - Messages should be from the same topic specified in the topic parameter
//
// Parameters:
//   - ctx: Context for cancellation and timeout control
//   - topic: The topic name these messages belong to
//   - msgs: The Kafka messages to commit (obtained from ReadEvent operations)
//
// Returns:
//   - error: Any error that occurred during the commit operation
//
// Example:
//
//	// After processing messages successfully
//	err := consumer.CommitEvents(ctx, "user-events", msg1, msg2)
//	if err != nil {
//	    log.Printf("Failed to commit: %v", err)
//	}
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

// Stats returns detailed statistics for the reader associated with the specified topic.
// The statistics include metrics such as:
//   - Messages read count
//   - Bytes read total
//   - Current lag behind the latest offset
//   - Partition assignments
//   - Connection status
//   - Commit information
//
// This information is useful for monitoring consumer performance and debugging
// consumption issues. The stats are real-time snapshots of the reader's state.
//
// Parameters:
//   - topic: The topic name to get statistics for
//
// Returns:
//   - kafka.ReaderStats: Detailed statistics structure with consumption metrics
//   - error: Any error if the topic reader is not found or consumer not initialized
//
// Example:
//
//	stats, err := consumer.Stats("user-events")
//	if err != nil {
//	    log.Printf("Failed to get stats: %v", err)
//	} else {
//	    log.Printf("Messages read: %d, Lag: %d", stats.Messages, stats.Lag)
//	}
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
