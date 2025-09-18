//go:build integration
// +build integration

package kafka

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/grasp-labs/ds-event-stream-go-sdk/models"
)

// TestIntegrationSendEvent tests sending events to a real Kafka instance
// This test should be run with the 'integration' build tag: go test -tags=integration

func TestIntegrationSendEvent(t *testing.T) {
	// Skip if not running integration tests
	if testing.Short() {
		t.Skip("Skipping integration test")
	}

	// Setup real Kafka configuration
	config := Config{
		Brokers: []string{"localhost:9092"}, // Adjust for your test environment
		Security: Security{
			Username: "test_user",
			Password: "test_pass",
		},
		BatchSize:    10,
		BatchTimeout: 100 * time.Millisecond,
		WriteTimeout: 5 * time.Second,
	}

	producer, err := NewProducer(config)
	if err != nil {
		t.Fatalf("Failed to create producer: %v", err)
	}
	defer producer.Close()

	// Create test event
	event := models.EventJson{
		Id:          uuid.New(),
		SessionId:   uuid.New(),
		RequestId:   uuid.New(),
		TenantId:    uuid.New(),
		EventType:   "integration.test.v1",
		EventSource: "integration-test",
		Metadata:    map[string]string{"test": "integration"},
		Timestamp:   time.Now(),
		CreatedBy:   "integration-test",
		Md5Hash:     "d41d8cd98f00b204e9800998ecf8427e",
		Payload: &map[string]interface{}{
			"test_message": "integration test",
			"timestamp":    time.Now().Unix(),
		},
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Test sending single event
	err = producer.SendEvent(ctx, "test-events", event)
	if err != nil {
		t.Errorf("Failed to send event: %v", err)
	}

	// Test sending with custom headers
	headers := []Header{
		{Key: "source", Value: "integration-test"},
		{Key: "version", Value: "1.0"},
	}

	err = producer.SendEvent(ctx, "test-events", event, headers...)
	if err != nil {
		t.Errorf("Failed to send event with headers: %v", err)
	}
}

func TestIntegrationSendEvents(t *testing.T) {
	// Skip if not running integration tests
	if testing.Short() {
		t.Skip("Skipping integration test")
	}

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
	defer producer.Close()

	// Create multiple test events
	events := make([]models.EventJson, 3)
	for i := range events {
		events[i] = models.EventJson{
			Id:          uuid.New(),
			SessionId:   uuid.New(),
			RequestId:   uuid.New(),
			TenantId:    uuid.New(),
			EventType:   "integration.batch.test.v1",
			EventSource: "integration-test",
			Metadata:    map[string]string{"batch": "test"},
			Timestamp:   time.Now(),
			CreatedBy:   "integration-test",
			Md5Hash:     "d41d8cd98f00b204e9800998ecf8427e",
			Payload: &map[string]interface{}{
				"batch_index": i,
				"test_data":   "batch test",
			},
		}
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Test batch sending
	err = producer.SendEvents(ctx, "test-events", events, nil)
	if err != nil {
		t.Errorf("Failed to send events batch: %v", err)
	}

	// Test batch sending with custom keys
	keys := []string{"key-1", "key-2", "key-3"}
	err = producer.SendEvents(ctx, "test-events", events, keys)
	if err != nil {
		t.Errorf("Failed to send events batch with keys: %v", err)
	}
}

func TestIntegrationACLCheck(t *testing.T) {
	// Skip if not running integration tests
	if testing.Short() {
		t.Skip("Skipping integration test")
	}

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
	defer producer.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Test ACL check for allowed topic
	err = producer.checkTopicWritePermission(ctx, "test-events")
	if err != nil {
		t.Logf("ACL check failed (expected if topic doesn't exist or no permissions): %v", err)
	}

	// Test ACL check for non-existent topic
	err = producer.checkTopicWritePermission(ctx, "non-existent-topic-12345")
	if err != nil {
		t.Logf("ACL check failed for non-existent topic (expected): %v", err)
	}
}

// Helper function for load testing
func BenchmarkIntegrationSendEvent(b *testing.B) {
	if testing.Short() {
		b.Skip("Skipping integration benchmark")
	}

	config := Config{
		Brokers: []string{"localhost:9092"},
		Security: Security{
			Username: "test_user",
			Password: "test_pass",
		},
	}

	producer, err := NewProducer(config)
	if err != nil {
		b.Fatalf("Failed to create producer: %v", err)
	}
	defer producer.Close()

	event := models.EventJson{
		Id:          uuid.New(),
		SessionId:   uuid.New(),
		RequestId:   uuid.New(),
		TenantId:    uuid.New(),
		EventType:   "benchmark.test.v1",
		EventSource: "benchmark-test",
		Metadata:    map[string]string{"bench": "test"},
		Timestamp:   time.Now(),
		CreatedBy:   "benchmark-test",
		Md5Hash:     "d41d8cd98f00b204e9800998ecf8427e",
		Payload: &map[string]interface{}{
			"benchmark": true,
		},
	}

	ctx := context.Background()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		// Update event ID for each iteration
		event.Id = uuid.New()
		err := producer.SendEvent(ctx, "benchmark-events", event)
		if err != nil {
			b.Errorf("Failed to send event: %v", err)
		}
	}
}
