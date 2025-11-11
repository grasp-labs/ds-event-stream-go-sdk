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

// Consumer wraps a kafka-go Reader for reading model messages from topics.
type Consumer struct {
	config  Config
	readers map[string]*kafka.Reader // topic -> reader mapping
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

	return &Consumer{
		config:  cfg,
		readers: make(map[string]*kafka.Reader),
	}, nil
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

	// Validate brokers configuration
	if len(c.config.Brokers) == 0 {
		return nil, errors.New("kafka: no brokers provided")
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
	if groupID == "" && c.config.Partition < 0 {
		// Set manual offset for non-group consumers based on configuration
		if err := reader.SetOffset(c.config.StartOffset); err != nil {
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

// CommitMessages commits the offset for the provided messages without requiring
// a topic parameter. The topics are automatically extracted from the messages.
// This is a convenience method that's more ergonomic when you already have the
// messages from ReadEventWithMessage.
//
// If messages are from multiple topics, they will be grouped by topic and
// committed using the appropriate reader for each topic. All commits are performed
// within the same context, but errors from any topic will cause the entire
// operation to fail.
//
// Parameters:
//   - ctx: Context for cancellation and timeout control
//   - msgs: One or more kafka messages to commit
//
// Returns:
//   - error: Any error that occurred during commit
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
//	// Commit the message (topic is extracted automatically)
//	err = consumer.CommitEvent(ctx, msg)

// CommitEvent commits a single Kafka message by extracting the topic from the message
// and using the appropriate reader for that topic. This is a convenience method for
// the common case of committing a single message without having to specify the topic.
//
// This method automatically:
//   - Extracts the topic from the message
//   - Finds the correct reader for that topic
//   - Commits the message using the topic-specific reader
//
// Parameters:
//   - ctx: Context for the commit operation (can include timeout/cancellation)
//   - message: The Kafka message to commit (must have a valid topic)
//
// Returns:
//   - error: Any error during validation or commit operation
//
// Example:
//
//	event, msg, err := consumer.ReadEventWithMessage(ctx, "user-events")
//	if err == nil {
//	    // Process the event...
//
//	    // Commit the message (topic is extracted automatically)
//	    err = consumer.CommitEvent(ctx, msg)
//	}
func (c *Consumer) CommitEvent(ctx context.Context, message kafka.Message) error {
	if c == nil {
		return errors.New("kafka: consumer not initialized")
	}
	if message.Topic == "" {
		return errors.New("kafka: message topic is empty")
	}

	// Find the reader for this topic
	var reader *kafka.Reader
	for key, r := range c.readers {
		if key == message.Topic || (len(key) > len(message.Topic) && key[len(key)-len(message.Topic):] == message.Topic) {
			reader = r
			break
		}
	}

	if reader == nil {
		return errors.New("kafka: no active reader for topic " + message.Topic)
	}

	// Commit the message
	return reader.CommitMessages(ctx, message)
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
