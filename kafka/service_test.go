package kafka

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/grasp/ds-event-streams/sdks/goland/models"
)

func TestDefaultConfig(t *testing.T) {
	security := Security{
		Username: "test_user",
		Password: "test_pass",
	}

	config := DefaultConfig(security)

	// Test default values
	if len(config.Brokers) != 1 || config.Brokers[0] != "b0.kafka.ds.local:9095" {
		t.Errorf("Expected default broker, got %v", config.Brokers)
	}

	if config.Security.Username != "test_user" {
		t.Errorf("Expected username 'test_user', got %s", config.Security.Username)
	}

	if config.Security.Password != "test_pass" {
		t.Errorf("Expected password 'test_pass', got %s", config.Security.Password)
	}

	if config.BatchSize != 100 {
		t.Errorf("Expected batch size 100, got %d", config.BatchSize)
	}

	if config.WriteTimeout != 10*time.Second {
		t.Errorf("Expected write timeout 10s, got %v", config.WriteTimeout)
	}

	if !config.AllowAutoTopicCreation {
		t.Error("Expected AllowAutoTopicCreation to be true")
	}
}

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
		Security: Security{
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
	producer.Close()
}

func TestCreateTestEvent(t *testing.T) {
	event := createTestEvent()

	if event.Id == uuid.Nil {
		t.Error("Expected non-nil event ID")
	}

	if event.SessionId == uuid.Nil {
		t.Error("Expected non-nil session ID")
	}

	if event.EventType == uuid.Nil {
		t.Error("Expected non-nil event type")
	}

	if event.EventSource == uuid.Nil {
		t.Error("Expected non-nil event source")
	}

	if len(event.Payload) == 0 {
		t.Error("Expected non-empty payload")
	}

	if event.Payload["test_key"] != "test_value" {
		t.Errorf("Expected payload test_key to be 'test_value', got %v", event.Payload["test_key"])
	}
}

func TestProducerClose(t *testing.T) {
	config := Config{
		Brokers: []string{"localhost:9092"},
		Security: Security{
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

func TestSendEventsValidation(t *testing.T) {
	// Test with nil producer
	var producer *Producer
	ctx := context.Background()
	events := []models.Event{createTestEvent()}

	err := producer.SendEvents(ctx, "test-topic", events, nil)
	if err == nil {
		t.Error("Expected error for nil producer")
	}

	// Test with empty events slice
	config := Config{
		Brokers:  []string{"localhost:9092"},
		Security: Security{Username: "test", Password: "test"},
	}
	producer, _ = NewProducer(config)
	defer producer.Close()

	err = producer.SendEvents(ctx, "test-topic", []models.Event{}, nil)
	if err != nil {
		t.Errorf("Expected no error for empty events slice, got %v", err)
	}

	// Test with mismatched keys length
	events = []models.Event{createTestEvent(), createTestEvent()}
	keys := []string{"key1"} // Only one key for two events

	err = producer.SendEvents(ctx, "test-topic", events, keys)
	if err == nil {
		t.Error("Expected error for mismatched keys length")
	}

	expectedMsg := "kafka: len(keys) must match len(evts) or be zero"
	if err.Error() != expectedMsg {
		t.Errorf("Expected error message '%s', got '%s'", expectedMsg, err.Error())
	}
}

func TestHeader(t *testing.T) {
	header := Header{
		Key:   "test-header",
		Value: "test-value",
	}

	if header.Key != "test-header" {
		t.Errorf("Expected header key 'test-header', got %s", header.Key)
	}

	if header.Value != "test-value" {
		t.Errorf("Expected header value 'test-value', got %s", header.Value)
	}
}

// Helper function to create a test event
func createTestEvent() models.Event {
	return models.Event{
		Id:          uuid.New(),
		SessionId:   uuid.New(),
		RequestId:   uuid.New(),
		TenantId:    uuid.New(),
		EventType:   uuid.New(),
		EventSource: uuid.New(),
		Metadata:    map[string]uuid.UUID{"meta1": uuid.New()},
		Timestamp:   time.Now(),
		CreatedBy:   uuid.New(),
		Md5Hash:     uuid.New(),
		Payload: map[string]interface{}{
			"test_key": "test_value",
			"number":   42,
			"boolean":  true,
		},
	}
}

// Benchmark tests
func BenchmarkEventCreation(b *testing.B) {
	for i := 0; i < b.N; i++ {
		createTestEvent()
	}
}

func BenchmarkEventJSONMarshal(b *testing.B) {
	event := createTestEvent()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		_, _ = event.MarshalJSON()
	}
}
