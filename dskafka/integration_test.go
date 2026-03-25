//go:build integration

package dskafka

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/grasp-labs/ds-event-stream-go-sdk/v2/models"
	"github.com/grasp-labs/ds-go-kit/x/log"
)

// TestIntegrationSendEvent tests sending events to a real Kafka instance
// This test should be run with the 'integration' build tag: go test -tags=integration

func TestIntegrationSendEvent(t *testing.T) {
	// Skip if not running integration tests
	if testing.Short() {
		t.Skip("Skipping integration test")
	}

	password := os.Getenv("DS_CONSUMPTION_INGRESS_V1_PASSWORD")
	ctx := context.Background()
	log.Info(ctx, "Password set: %v, length: %d", password != "", len(password))

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

	// Create test event using validated constructor
	event, err := models.NewEvent(
		"integration.test.v1",                    // eventType
		"integration-test",                       // eventSource
		"integration-test",                       // createdBy
		uuid.New(),                               // tenantId
		uuid.New(),                               // sessionId
		uuid.New(),                               // requestId
		map[string]string{"test": "integration"}, // metadata
		"d41d8cd98f00b204e9800998ecf8427e",       // md5Hash
	)
	if err != nil {
		t.Fatalf("Failed to create test event: %v", err)
	}

	// Set optional payload
	event.SetPayload(map[string]interface{}{
		"test_message": "integration test",
		"timestamp":    time.Now().Unix(),
	})

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

	ctx := context.Background()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		// Create a new event for each iteration
		event, err := models.NewEvent(
			"benchmark.test.v1",                // eventType
			"benchmark-test",                   // eventSource
			"benchmark-test",                   // createdBy
			uuid.New(),                         // tenantId
			uuid.New(),                         // sessionId
			uuid.New(),                         // requestId
			map[string]string{"bench": "test"}, // metadata
			"d41d8cd98f00b204e9800998ecf8427e", // md5Hash
		)
		if err != nil {
			b.Fatalf("Failed to create event: %v", err)
		}

		event.SetPayload(map[string]interface{}{
			"benchmark": true,
		})

		err = producer.SendEvent(ctx, "benchmark-events", event)
		if err != nil {
			b.Errorf("Failed to send event: %v", err)
		}
	}
}
