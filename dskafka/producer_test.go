package dskafka

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
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
