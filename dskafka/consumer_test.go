package dskafka

import (
	"context"
	"testing"
	"time"
)

func TestDefaultConsumerConfig(t *testing.T) {
	expectedClientCredentials := ClientCredentials{
		Username: "test_user",
		Password: "test_pass",
	}

	bootstrapServers := GetBootstrapServers(Dev, false)
	groupID := "test-group"
	readTimeout := 10 * time.Second
	partition := -1

	config := DefaultConsumerConfig(expectedClientCredentials, bootstrapServers, groupID)

	// Test default values
	if len(config.Brokers) != 1 || config.Brokers[0] != bootstrapServers[0] {
		t.Errorf("Expected default broker, got %v", config.Brokers)
	}

	if config.ClientCredentials.Username != expectedClientCredentials.Username {
		t.Errorf("Expected username 'test_user', got %s", config.ClientCredentials.Username)
	}

	if config.ClientCredentials.Password != expectedClientCredentials.Password {
		t.Errorf("Expected password 'test_pass', got %s", config.ClientCredentials.Password)
	}

	if config.GroupID != groupID {
		t.Errorf("Expected group ID 'test-group', got %s", config.GroupID)
	}

	if config.ReadTimeout != readTimeout {
		t.Errorf("Expected read timeout 10s, got %v", config.ReadTimeout)
	}

	if config.Partition != partition {
		t.Errorf("Expected partition -1 (all partitions), got %d", config.Partition)
	}
}

func TestNewConsumerValidation(t *testing.T) {
	clientCredentials := ClientCredentials{
		Username: "test_user",
		Password: "test_pass",
	}

	// Test with empty brokers
	config := Config{
		Brokers:           []string{},
		ClientCredentials: clientCredentials,
		GroupID:           "test-group",
	}

	_, err := NewConsumer(config)
	if err == nil || err.Error() != "kafka: no brokers provided" {
		t.Errorf("Expected 'no brokers provided' error, got %v", err)
	}
}

func TestNewConsumerSuccess(t *testing.T) {
	clientCredentials := ClientCredentials{
		Username: "test_user",
		Password: "test_pass",
	}

	bootstrapServers := GetBootstrapServers(Dev, false)

	groupID := "test-group"

	config := DefaultConsumerConfig(clientCredentials, bootstrapServers, groupID)

	consumer, err := NewConsumer(config)
	if err != nil {
		t.Fatalf("Expected successful consumer creation, got error: %v", err)
	}

	if consumer == nil {
		t.Fatal("Expected non-nil consumer")
	}

	if consumer.readers == nil {
		t.Error("Expected non-nil readers map")
	}

	if consumer.client == nil {
		t.Error("Expected non-nil client")
	}

	// Test Close
	err = consumer.Close()
	if err != nil {
		t.Errorf("Expected successful close, got error: %v", err)
	}
}

func TestConsumerClose(t *testing.T) {
	clientCredentials := ClientCredentials{
		Username: "test_user",
		Password: "test_pass",
	}

	bootstrapServers := GetBootstrapServers(Dev, false)
	groupID := "test-group"

	config := DefaultConsumerConfig(clientCredentials, bootstrapServers, groupID)
	consumer, err := NewConsumer(config)
	if err != nil {
		t.Fatalf("Failed to create consumer: %v", err)
	}

	// Test normal close
	err = consumer.Close()
	if err != nil {
		t.Errorf("Expected successful close, got error: %v", err)
	}

	// Test close on already closed consumer
	err = consumer.Close()
	if err != nil {
		t.Errorf("Expected successful close on already closed consumer, got error: %v", err)
	}
}

func TestConsumerCloseNil(t *testing.T) {
	var consumer *Consumer

	// Test close on nil consumer
	err := consumer.Close()
	if err != nil {
		t.Errorf("Expected nil error for nil consumer close, got %v", err)
	}
}

func TestReadEventValidation(t *testing.T) {
	var consumer *Consumer

	// Test ReadEvent on nil consumer
	_, err := consumer.ReadEvent(context.Background(), "test-topic")
	if err == nil || err.Error() != "kafka: consumer not initialized" {
		t.Errorf("Expected 'consumer not initialized' error, got %v", err)
	}
}

func TestConsumerStats(t *testing.T) {
	security := ClientCredentials{
		Username: "test_user",
		Password: "test_pass",
	}
	bootstrapServers := GetBootstrapServers(Dev, false)
	groupID := "test-group"

	config := DefaultConsumerConfig(security, bootstrapServers, groupID)
	consumer, err := NewConsumer(config)
	if err != nil {
		t.Fatalf("Failed to create consumer: %v", err)
	}
	defer func() {
		err := consumer.Close()
		if err != nil {
			t.Errorf("Unexpected error on consumer.Close(): %v", err)
		}
	}()

	// Test getting stats for non-existent topic
	_, err = consumer.Stats("test-topic")
	if err == nil || err.Error() != "kafka: no active reader for topic test-topic" {
		t.Errorf("Expected 'no active reader' error, got %v", err)
	}
}

func TestConsumerStatsNil(t *testing.T) {
	var consumer *Consumer

	// Test stats on nil consumer
	_, err := consumer.Stats("test-topic")
	if err == nil || err.Error() != "kafka: consumer not initialized" {
		t.Errorf("Expected 'consumer not initialized' error, got %v", err)
	}
}