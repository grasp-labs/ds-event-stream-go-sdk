package dskafka

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/grasp-labs/ds-event-stream-go-sdk/models"
)

func TestDefaultConfig(t *testing.T) {
	clientCredentials := ClientCredentials{
		Username: "test_user",
		Password: "test_pass",
	}

	bootstrapServers := GetBootstrapServers(Dev, false)
	groupID := "test-group"

	config := DefaultConsumerConfig(clientCredentials, bootstrapServers, groupID)

	if config.ClientCredentials.Username != clientCredentials.Username {
		t.Errorf("Expected username 'test_user', got %s", config.ClientCredentials.Username)
	}

	if config.ClientCredentials.Password != clientCredentials.Password {
		t.Errorf("Expected password 'test_pass', got %s", config.ClientCredentials.Password)
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

func TestGetBootstrapServersProdExternalHostname(t *testing.T) {
	bootstrapServers := GetBootstrapServers(Prod, false)

	// Test default values
	if len(bootstrapServers) != 3 || bootstrapServers[0] != "b0.kafka.ds.local:9095" {
		t.Errorf("Expected prod brokers with external hostnames, got %v", bootstrapServers)
	}
}

func TestGetBootstrapServersProdInternalHostname(t *testing.T) {

	bootstrapServers := GetBootstrapServers(Prod, true)

	// Test default values
	if len(bootstrapServers) != 1 || bootstrapServers[0] != "kafka.kafka.svc.cluster.local:9092" {
		t.Errorf("Expected prod brokers with internal hostnames, got %v", bootstrapServers)
	}
}

func TestGetBootstrapServersDevExternalHostname(t *testing.T) {
	bootstrapServers := GetBootstrapServers(Dev, false)

	// Test default values
	if len(bootstrapServers) != 1 || bootstrapServers[0] != "b0.dev.kafka.ds.local:9095" {
		t.Errorf("Expected dev brokers with internal hostnames, got %v", bootstrapServers)
	}
}

func TestGetBootstrapServersDevInternalHostname(t *testing.T) {

	bootstrapServers := GetBootstrapServers(Dev, true)

	// Test default values
	if len(bootstrapServers) != 1 || bootstrapServers[0] != "kafka.kafka-dev.svc.cluster.local:9092" {
		t.Errorf("Expected dev brokers with internal hostnames, got %v", bootstrapServers)
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
func createTestEvent() models.EventJson {
	return models.EventJson{
		Id:          uuid.New(),
		SessionId:   uuid.New(),
		RequestId:   uuid.New(),
		TenantId:    uuid.New(),
		EventType:   "test.event.created.v1",
		EventSource: "test-service",
		Metadata:    map[string]string{"meta1": "value1"},
		Timestamp:   time.Now(),
		CreatedBy:   "test-producer",
		Md5Hash:     "d41d8cd98f00b204e9800998ecf8427e",
		Payload: &map[string]interface{}{
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
		_, _ = json.Marshal(event)
	}
}
