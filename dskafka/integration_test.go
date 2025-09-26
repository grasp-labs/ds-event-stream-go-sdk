//go:build integration
// +build integration

package dskafka

import (
	"context"
	"log"
	"os"
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

	password := os.Getenv("DS_CONSUMPTION_INGRESS_V1_PASSWORD")
	log.Printf("Password: %s", password)

	// Setup credentials
	credentials := ClientCredentials{
		Username: "ds.consumption.ingress.v1",
		Password: password,
	}

	bootstrapServers := GetBootstrapServers(Dev, false)

	// Setup real Kafka configuration
	config := DefaultProducerConfig(credentials, bootstrapServers)

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
	err = producer.SendEvent(ctx, "ds.workflow.pipeline.job.requested.v1", event)
	if err != nil {
		t.Errorf("Failed to send event: %v", err)
	}

	// Test sending with custom headers
	headers := []Header{
		{Key: "source", Value: "integration-test"},
		{Key: "version", Value: "1.0"},
	}

	err = producer.SendEvent(ctx, "ds.workflow.pipeline.job.requested.v1", event, headers...)
	if err != nil {
		t.Errorf("Failed to send event with headers: %v", err)
	}
}

// Helper function for load testing
func BenchmarkIntegrationSendEvent(b *testing.B) {
	if testing.Short() {
		b.Skip("Skipping integration benchmark")
	}

	password := os.Getenv("DS_CONSUMPTION_INGRESS_V1_PASSWORD")

	// Setup credentials
	credentials := ClientCredentials{
		Username: "ds.consumption.ingress.v1",
		Password: password,
	}

	bootstrapServers := GetBootstrapServers(Dev, false)

	// Setup real Kafka configuration
	config := DefaultProducerConfig(credentials, bootstrapServers)

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
