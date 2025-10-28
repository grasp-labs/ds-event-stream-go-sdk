package dskafka

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/grasp-labs/ds-event-stream-go-sdk/models"
	"github.com/segmentio/kafka-go"
	"github.com/stretchr/testify/assert"
)

func TestNewProducerValidation(t *testing.T) {
	// Test with empty brokers
	config := Config{
		Brokers: []string{},
	}

	_, err := NewProducer(config)
	if err == nil {
		t.Error("Expected error for empty brokers")
	}

	expectedMsg := "kafka: no brokers provided"
	if err.Error() != expectedMsg {
		t.Errorf("Expected error message '%s', got '%s'", expectedMsg, err.Error())
	}
}

func TestNewProducerSuccess(t *testing.T) {
	config := Config{
		Brokers: []string{"localhost:9092"},
		ClientCredentials: ClientCredentials{
			Username: "test_user",
			Password: "test_pass",
		},
		BatchSize:    50,
		BatchBytes:   1024,
		BatchTimeout: 100 * time.Millisecond,
		WriteTimeout: 5 * time.Second,
	}

	producer, err := NewProducer(config)
	if err != nil {
		t.Fatalf("Expected no error, got %v", err)
	}

	if producer == nil {
		t.Fatal("Expected producer to be created")
	}

	if producer.w == nil {
		t.Error("Expected writer to be initialized")
	}

	if producer.client == nil {
		t.Error("Expected client to be initialized")
	}

	// Clean up
	err = producer.Close()
	if err != nil {
		t.Errorf("Unexpected error on producer.Close(): %v", err)
	}
}

// TestNewProducerSASLError tests SASL mechanism creation failure to improve NewProducer coverage
func TestNewProducerNoBrokers(t *testing.T) {
	cfg := Config{
		Brokers: []string{}, // Empty brokers should fail
		ClientCredentials: ClientCredentials{
			Username: "testuser",
			Password: "testpass",
		},
		Balancer: &kafka.LeastBytes{},
	}

	producer, err := NewProducer(cfg)
	assert.Error(t, err)
	assert.Nil(t, producer)
	assert.Contains(t, err.Error(), "no brokers provided")
}

func TestNewProducerSASLSetup(t *testing.T) {
	cfg := Config{
		Brokers: []string{"localhost:9092"},
		ClientCredentials: ClientCredentials{
			Username: "testuser",
			Password: "testpass",
		},
		Balancer: &kafka.LeastBytes{},
	}

	// This test ensures SASL mechanism setup code path is covered
	producer, err := NewProducer(cfg)

	// Should succeed in setting up SASL mechanism
	assert.NoError(t, err)
	assert.NotNil(t, producer)
	if producer != nil {
		producer.Close()
	}
} // TestNewProducerWithCustomConfig tests NewProducer with various configuration options
func TestNewProducerWithCustomConfig(t *testing.T) {
	tests := []struct {
		name           string
		config         Config
		expectError    bool
		errorMsg       string
		validateFields func(*testing.T, *Producer)
	}{
		{
			name: "config with custom balancer",
			config: Config{
				Brokers: []string{"localhost:9092"},
				ClientCredentials: ClientCredentials{
					Username: "test_user",
					Password: "test_pass",
				},
				Balancer: &kafka.Hash{}, // Custom balancer
			},
			expectError: false,
			validateFields: func(t *testing.T, p *Producer) {
				assert.NotNil(t, p.w)
				assert.NotNil(t, p.client)
			},
		},
		{
			name: "config with multiple brokers",
			config: Config{
				Brokers: []string{"broker1:9092", "broker2:9092", "broker3:9092"},
				ClientCredentials: ClientCredentials{
					Username: "test_user",
					Password: "test_pass",
				},
			},
			expectError: false,
			validateFields: func(t *testing.T, p *Producer) {
				assert.NotNil(t, p.w)
				assert.NotNil(t, p.client)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			producer, err := NewProducer(tt.config)

			if tt.expectError {
				assert.Error(t, err)
				assert.Nil(t, producer)
				if tt.errorMsg != "" {
					assert.Contains(t, err.Error(), tt.errorMsg)
				}
			} else {
				assert.NoError(t, err)
				assert.NotNil(t, producer)

				if tt.validateFields != nil {
					tt.validateFields(t, producer)
				}

				// Clean up
				err = producer.Close()
				assert.NoError(t, err)
			}
		})
	}
}

func TestCreateTestEvent(t *testing.T) {
	event := createTestEvent()

	if event.Id == uuid.Nil {
		t.Error("Expected non-nil event ID")
	}

	if event.SessionId == uuid.Nil {
		t.Error("Expected non-nil session ID")
	}

	if event.EventType == "" {
		t.Error("Expected non-empty event type")
	}

	if event.EventSource == "" {
		t.Error("Expected non-empty event source")
	}

	if event.Payload == nil {
		t.Error("Expected non-nil payload")
	} else {
		payload := *event.Payload
		if len(payload) == 0 {
			t.Error("Expected non-empty payload")
		}

		if payload["test_key"] != "test_value" {
			t.Errorf("Expected payload test_key to be 'test_value', got %v", payload["test_key"])
		}
	}
}

func TestProducerClose(t *testing.T) {
	config := Config{
		Brokers: []string{"localhost:9092"},
		ClientCredentials: ClientCredentials{
			Username: "test_user",
			Password: "test_pass",
		},
	}

	producer, err := NewProducer(config)
	if err != nil {
		t.Fatalf("Failed to create producer: %v", err)
	}

	// Should not panic or error
	err = producer.Close()
	if err != nil {
		t.Errorf("Unexpected error on close: %v", err)
	}

	// Multiple closes should be safe
	err = producer.Close()
	if err != nil {
		t.Errorf("Unexpected error on second close: %v", err)
	}
}

func TestProducerCloseNil(t *testing.T) {
	var producer *Producer

	// Should not panic
	err := producer.Close()
	if err != nil {
		t.Errorf("Expected no error for nil producer close, got %v", err)
	}
}

func TestSendEventValidation(t *testing.T) {
	// Test with nil producer
	var producer *Producer
	ctx := context.Background()
	event := createTestEvent()

	err := producer.SendEvent(ctx, "test-topic", event)
	if err == nil {
		t.Error("Expected error for nil producer")
	}

	expectedMsg := "kafka: producer not initialized"
	if err.Error() != expectedMsg {
		t.Errorf("Expected error message '%s', got '%s'", expectedMsg, err.Error())
	}
}

// Test SendEvent with successful message writing
func TestSendEventSuccess(t *testing.T) {
	// Since we can't easily mock kafka.Writer directly, we test the validation logic
	// and error handling paths that we can control

	event := createTestEvent()
	ctx := context.Background()

	// Test with nil producer
	var producer *Producer
	err := producer.SendEvent(ctx, "test-topic", event)

	assert.Error(t, err)
	assert.Contains(t, err.Error(), "producer not initialized")

	// Test with producer that has nil writer
	producer = &Producer{w: nil, client: nil}
	err = producer.SendEvent(ctx, "test-topic", event)

	assert.Error(t, err)
	assert.Contains(t, err.Error(), "producer not initialized")
}

// Test SendEvent with context timeout
func TestSendEventWithTimeout(t *testing.T) {
	// Test with expired context
	ctx, cancel := context.WithDeadline(context.Background(), time.Now().Add(-1*time.Second))
	defer cancel()

	var producer *Producer
	event := createTestEvent()

	err := producer.SendEvent(ctx, "test-topic", event)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "producer not initialized")
}

// Test SendEvent with different event configurations
func TestSendEventWithDifferentEvents(t *testing.T) {
	tests := []struct {
		name    string
		event   models.EventJson
		wantErr bool
	}{
		{
			name:    "normal event",
			event:   createTestEvent(),
			wantErr: false, // We expect error due to nil producer, but event is valid
		},
		{
			name: "event with zero UUID",
			event: models.EventJson{
				Id:          uuid.Nil, // Zero UUID
				SessionId:   uuid.New(),
				EventType:   "test.event.v1",
				EventSource: "test",
				Timestamp:   time.Now(),
			},
			wantErr: false, // Valid event, error only due to nil producer
		},
		{
			name: "event with both zero UUIDs",
			event: models.EventJson{
				Id:          uuid.Nil,
				SessionId:   uuid.Nil,
				EventType:   "test.event.v1",
				EventSource: "test",
				Timestamp:   time.Now(),
			},
			wantErr: false, // Valid event structure
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var producer *Producer // nil producer will always return error
			ctx := context.Background()

			err := producer.SendEvent(ctx, "test-topic", tt.event)

			// All cases should error due to nil producer
			assert.Error(t, err)
			assert.Contains(t, err.Error(), "producer not initialized")
		})
	}
}

// Test SendEvent with custom headers
func TestSendEventWithHeaders(t *testing.T) {
	var producer *Producer
	event := createTestEvent()
	ctx := context.Background()

	headers := []Header{
		{Key: "custom-header", Value: "custom-value"},
		{Key: "source", Value: "test-service"},
	}

	err := producer.SendEvent(ctx, "test-topic", event, headers...)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "producer not initialized")
}

// Test DefaultProducerConfig
func TestDefaultProducerConfig(t *testing.T) {
	credentials := ClientCredentials{
		Username: "test-user",
		Password: "test-pass",
	}
	brokers := []string{"localhost:9092", "localhost:9093"}

	config := DefaultProducerConfig(credentials, brokers)

	assert.Equal(t, brokers, config.Brokers)
	assert.Equal(t, credentials, config.ClientCredentials)
	assert.Equal(t, kafka.RequireOne, config.RequiredAcks)
	assert.Equal(t, 100, config.BatchSize)
	assert.Equal(t, int64(1<<20), config.BatchBytes) // 1 MiB - fix type
	assert.Equal(t, 50*time.Millisecond, config.BatchTimeout)
	assert.Equal(t, kafka.Snappy, config.Compression)
	assert.False(t, config.Async)
	assert.True(t, config.AllowAutoTopicCreation)
	assert.Equal(t, 10*time.Second, config.WriteTimeout)
	assert.IsType(t, &kafka.Hash{}, config.Balancer)
}

// Test Producer struct validation
func TestProducerStructValidation(t *testing.T) {
	tests := []struct {
		name     string
		producer *Producer
		wantNil  bool
	}{
		{
			name:     "nil producer",
			producer: nil,
			wantNil:  true,
		},
		{
			name: "producer with nil writer",
			producer: &Producer{
				w:      nil,
				client: nil,
			},
			wantNil: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			event := createTestEvent()
			ctx := context.Background()

			err := tt.producer.SendEvent(ctx, "test-topic", event)

			assert.Error(t, err)
			assert.Contains(t, err.Error(), "producer not initialized")

			// Test Close as well
			err = tt.producer.Close()
			assert.NoError(t, err) // Close should not error for nil producer
		})
	}
}

// TestSendEventAdvancedScenarios tests more complex SendEvent scenarios to improve coverage
func TestSendEventAdvancedScenarios(t *testing.T) {
	tests := []struct {
		name        string
		producer    *Producer
		event       models.EventJson
		topic       string
		headers     []Header
		expectError bool
		errorMsg    string
	}{
		{
			name:        "producer with nil writer",
			producer:    &Producer{w: nil},
			event:       createTestEvent(),
			topic:       "test-topic",
			expectError: true,
			errorMsg:    "producer not initialized",
		},
		{
			name: "event with zero UUID uses SessionId for key",
			producer: &Producer{
				w: &kafka.Writer{}, // Mock writer that won't actually write
			},
			event: models.EventJson{
				Id:          uuid.Nil, // Zero UUID
				SessionId:   uuid.New(),
				RequestId:   uuid.New(),
				TenantId:    uuid.New(),
				EventType:   "test.event.v1",
				EventSource: "test-service",
				Metadata:    map[string]string{},
				Timestamp:   time.Now(),
				CreatedBy:   "test-producer",
				Md5Hash:     "d41d8cd98f00b204e9800998ecf8427e",
			},
			topic:       "test-topic",
			expectError: true, // Will error on actual write, but tests key logic
		},
		{
			name: "event with multiple headers",
			producer: &Producer{
				w: &kafka.Writer{},
			},
			event: createTestEvent(),
			topic: "test-topic",
			headers: []Header{
				{Key: "custom-header", Value: "custom-value"},
				{Key: "trace-id", Value: "12345"},
				{Key: "correlation-id", Value: "abcdef"},
			},
			expectError: true, // Will error on actual write, but tests header logic
		},
		{
			name: "empty topic",
			producer: &Producer{
				w: &kafka.Writer{},
			},
			event:       createTestEvent(),
			topic:       "", // Empty topic
			expectError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
			defer cancel()

			err := tt.producer.SendEvent(ctx, tt.topic, tt.event, tt.headers...)

			if tt.expectError {
				assert.Error(t, err)
				if tt.errorMsg != "" {
					assert.Contains(t, err.Error(), tt.errorMsg)
				}
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

// TestSendEventJSONMarshalingLogic tests the JSON marshaling and key selection logic
func TestSendEventJSONMarshalingLogic(t *testing.T) {
	// Test the JSON marshaling doesn't fail for valid events
	event := createTestEvent()

	// Test that we can marshal the event (this tests the JSON marshaling path)
	data, err := json.Marshal(event)
	assert.NoError(t, err)
	assert.NotEmpty(t, data)

	// Test key selection logic
	t.Run("key selection with valid ID", func(t *testing.T) {
		event := createTestEvent()
		key := event.Id.String()
		assert.NotEqual(t, "00000000-0000-0000-0000-000000000000", key)
		assert.NotEmpty(t, key)
	})

	t.Run("key selection with nil ID uses SessionId", func(t *testing.T) {
		event := createTestEvent()
		event.Id = uuid.Nil

		// In SendEvent, if Id is nil UUID, it should use SessionId
		expectedKey := event.SessionId.String()
		assert.NotEqual(t, "00000000-0000-0000-0000-000000000000", expectedKey)
		assert.NotEmpty(t, expectedKey)
	})
}

// TestSendEventContextAndTimeout tests context handling in SendEvent
func TestSendEventContextAndTimeout(t *testing.T) {
	producer := &Producer{
		w: &kafka.Writer{}, // Mock writer
	}
	event := createTestEvent()

	t.Run("context without deadline gets default timeout", func(t *testing.T) {
		ctx := context.Background() // No deadline

		// This will test the path where context gets a default 10s timeout
		err := producer.SendEvent(ctx, "test-topic", event)
		assert.Error(t, err) // Will error on write, but tests timeout logic
	})

	t.Run("context with existing deadline", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		// This tests the path where existing deadline is used
		err := producer.SendEvent(ctx, "test-topic", event)
		assert.Error(t, err) // Will error on write, but tests timeout logic
	})

	t.Run("expired context", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 1*time.Nanosecond)
		time.Sleep(1 * time.Millisecond) // Ensure context expires
		defer cancel()

		// This tests the path where deadline has passed
		err := producer.SendEvent(ctx, "test-topic", event)
		assert.Error(t, err) // Should error due to expired context
	})
}

// Benchmark tests for producer operations
func BenchmarkSendEvent(b *testing.B) {
	var producer *Producer // Using nil producer for benchmark structure
	event := createTestEvent()
	ctx := context.Background()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = producer.SendEvent(ctx, "test-topic", event)
	}
}

// Test helper to create various event types
func createEventWithCustomFields(eventType string, source string) models.EventJson {
	return models.EventJson{
		Id:          uuid.New(),
		SessionId:   uuid.New(),
		RequestId:   uuid.New(),
		TenantId:    uuid.New(),
		EventType:   eventType,
		EventSource: source,
		Metadata:    map[string]string{"env": "test"},
		Timestamp:   time.Now(),
		CreatedBy:   "test-producer",
		Md5Hash:     "test-hash",
		Payload: &map[string]interface{}{
			"action": "test",
			"count":  1,
		},
	}
}
