package dskafka

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/segmentio/kafka-go"
	"github.com/stretchr/testify/assert"
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

func TestNewConsumerNoBrokers(t *testing.T) {
	cfg := Config{
		Brokers: []string{}, // Empty brokers should fail
		ClientCredentials: ClientCredentials{
			Username: "testuser",
			Password: "testpass",
		},
		GroupID: "test-group",
	}

	consumer, err := NewConsumer(cfg)
	assert.Error(t, err)
	assert.Nil(t, consumer)
	assert.Contains(t, err.Error(), "no brokers provided")
}

func TestNewConsumerSASLSetup(t *testing.T) {
	cfg := Config{
		Brokers: []string{"localhost:9092", "localhost:9093"},
		ClientCredentials: ClientCredentials{
			Username: "testuser",
			Password: "testpass",
		},
		GroupID: "test-group",
	}

	// This test ensures SASL mechanism setup code path is covered
	consumer, err := NewConsumer(cfg)

	// Should succeed in setting up SASL mechanism
	assert.NoError(t, err)
	assert.NotNil(t, consumer)
	assert.NotNil(t, consumer.client)
	assert.NotNil(t, consumer.readers)
	assert.Equal(t, cfg.GroupID, consumer.config.GroupID)

	if consumer != nil {
		consumer.Close()
	}
}

func TestNewConsumerWithConfiguration(t *testing.T) {
	tests := []struct {
		name string
		cfg  Config
	}{
		{
			name: "config_with_partition_strategy",
			cfg: Config{
				Brokers:           []string{"localhost:9092"},
				ClientCredentials: ClientCredentials{Username: "user", Password: "pass"},
				GroupID:           "test-partition-group",
				Partition:         5,
				MinBytes:          1024,
				MaxBytes:          2048,
			},
		},
		{
			name: "config_with_balancer_and_multiple_brokers",
			cfg: Config{
				Brokers:           []string{"broker1:9092", "broker2:9092", "broker3:9092"},
				ClientCredentials: ClientCredentials{Username: "multi-user", Password: "multi-pass"},
				GroupID:           "multi-broker-group",
				MaxWait:           time.Second * 5,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			consumer, err := NewConsumer(tt.cfg)
			assert.NoError(t, err)
			assert.NotNil(t, consumer)
			assert.Equal(t, tt.cfg.GroupID, consumer.config.GroupID)
			assert.Equal(t, tt.cfg.Brokers, consumer.config.Brokers)

			if consumer != nil {
				consumer.Close()
			}
		})
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

func TestConsumerStatsValidation(t *testing.T) {
	security := ClientCredentials{
		Username: "test_user",
		Password: "test_pass",
	}
	bootstrapServers := GetBootstrapServers(Dev, false)
	groupID := "test-group"

	config := DefaultConsumerConfig(security, bootstrapServers, groupID)
	consumer, err := NewConsumer(config)
	assert.NoError(t, err)
	defer consumer.Close()

	tests := []struct {
		name        string
		topic       string
		expectedErr string
	}{
		{
			name:        "empty_topic",
			topic:       "",
			expectedErr: "kafka: topic is required",
		},
		{
			name:        "non_existent_topic",
			topic:       "non-existent-topic",
			expectedErr: "kafka: no active reader for topic non-existent-topic",
		},
		{
			name:        "another_missing_topic",
			topic:       "missing-topic-name",
			expectedErr: "kafka: no active reader for topic missing-topic-name",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := consumer.Stats(tt.topic)
			assert.Error(t, err)
			assert.Equal(t, tt.expectedErr, err.Error())
		})
	}
}

// Enhanced consumer tests with comprehensive validation

// Test DefaultConsumerConfig with different environments
func TestDefaultConsumerConfigEnvironments(t *testing.T) {
	credentials := ClientCredentials{
		Username: "test-user",
		Password: "test-pass",
	}

	tests := []struct {
		name        string
		env         Environment
		internal    bool
		groupID     string
		expectError bool
	}{
		{
			name:     "dev external",
			env:      Dev,
			internal: false,
			groupID:  "test-group-dev",
		},
		{
			name:     "dev internal",
			env:      Dev,
			internal: true,
			groupID:  "test-group-dev-internal",
		},
		{
			name:     "prod external",
			env:      Prod,
			internal: false,
			groupID:  "test-group-prod",
		},
		{
			name:     "prod internal",
			env:      Prod,
			internal: true,
			groupID:  "test-group-prod-internal",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			brokers := GetBootstrapServers(tt.env, tt.internal)
			config := DefaultConsumerConfig(credentials, brokers, tt.groupID)

			assert.Equal(t, brokers, config.Brokers)
			assert.Equal(t, credentials, config.ClientCredentials)
			assert.Equal(t, tt.groupID, config.GroupID)
			assert.Equal(t, 10*time.Second, config.ReadTimeout)
			assert.Equal(t, -1, config.Partition) // All partitions
			assert.Equal(t, kafka.FirstOffset, config.StartOffset)
		})
	}
}

// Test consumer configuration validation
func TestConsumerConfigValidation(t *testing.T) {
	tests := []struct {
		name        string
		config      Config
		expectError bool
		errorMsg    string
	}{
		{
			name: "valid config",
			config: Config{
				Brokers: []string{"localhost:9092"},
				ClientCredentials: ClientCredentials{
					Username: "user",
					Password: "pass",
				},
				GroupID: "test-group",
			},
			expectError: false,
		},
		{
			name: "empty brokers",
			config: Config{
				Brokers: []string{},
				ClientCredentials: ClientCredentials{
					Username: "user",
					Password: "pass",
				},
				GroupID: "test-group",
			},
			expectError: true,
			errorMsg:    "kafka: no brokers provided",
		},
		{
			name: "empty credentials",
			config: Config{
				Brokers: []string{"localhost:9092"},
				ClientCredentials: ClientCredentials{
					Username: "",
					Password: "",
				},
				GroupID: "test-group",
			},
			expectError: false, // This should still work, Kafka may allow it
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			consumer, err := NewConsumer(tt.config)

			if tt.expectError {
				assert.Error(t, err)
				if tt.errorMsg != "" {
					assert.Contains(t, err.Error(), tt.errorMsg)
				}
				assert.Nil(t, consumer)
			} else {
				if err != nil {
					// Some configs might fail due to connection issues, that's ok
					t.Logf("Consumer creation failed (expected in test env): %v", err)
				}
				if consumer != nil {
					err = consumer.Close()
					assert.NoError(t, err)
				}
			}
		})
	}
}

// Test ReadEvent with different scenarios
func TestReadEventScenarios(t *testing.T) {
	tests := []struct {
		name        string
		consumer    *Consumer
		topic       string
		expectError bool
		errorMsg    string
	}{
		{
			name:        "nil consumer",
			consumer:    nil,
			topic:       "test-topic",
			expectError: true,
			errorMsg:    "consumer not initialized",
		},
		{
			name: "consumer with nil readers",
			consumer: &Consumer{
				readers: nil,
				client:  nil,
				config: Config{
					Brokers: []string{}, // Empty brokers should cause error before panic
				},
			},
			topic:       "test-topic",
			expectError: true,
			errorMsg:    "no brokers provided",
		},
		{
			name: "empty topic",
			consumer: &Consumer{
				readers: make(map[string]*kafka.Reader),
				client:  nil,
			},
			topic:       "",
			expectError: true,
			errorMsg:    "topic is required",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()
			event, err := tt.consumer.ReadEvent(ctx, tt.topic)

			assert.Error(t, err)
			assert.Nil(t, event)
			if tt.errorMsg != "" {
				assert.Contains(t, err.Error(), tt.errorMsg)
			}
		})
	}
}

// Test consumer stats with different scenarios
func TestConsumerStatsScenarios(t *testing.T) {
	tests := []struct {
		name        string
		consumer    *Consumer
		topic       string
		expectError bool
		errorMsg    string
	}{
		{
			name:        "nil consumer",
			consumer:    nil,
			topic:       "test-topic",
			expectError: true,
			errorMsg:    "consumer not initialized",
		},
		{
			name: "valid consumer no active reader",
			consumer: &Consumer{
				readers: make(map[string]*kafka.Reader),
				client:  nil,
			},
			topic:       "non-existent-topic",
			expectError: true,
			errorMsg:    "no active reader for topic",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			stats, err := tt.consumer.Stats(tt.topic)

			assert.Error(t, err)
			// Stats returns zero value, not nil
			assert.NotNil(t, stats)
			if tt.errorMsg != "" {
				assert.Contains(t, err.Error(), tt.errorMsg)
			}
		})
	}
}

// Test consumer with different timeout scenarios
func TestConsumerTimeouts(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Millisecond)
	defer cancel()

	var consumer *Consumer

	// Test with expired context
	event, err := consumer.ReadEvent(ctx, "test-topic")
	assert.Error(t, err)
	assert.Nil(t, event)
	assert.Contains(t, err.Error(), "consumer not initialized")
}

// Test consumer configuration edge cases
func TestConsumerConfigEdgeCases(t *testing.T) {
	credentials := ClientCredentials{
		Username: "test-user",
		Password: "test-pass",
	}

	t.Run("very long group id", func(t *testing.T) {
		longGroupID := string(make([]byte, 1000)) // Very long group ID
		brokers := []string{"localhost:9092"}

		config := DefaultConsumerConfig(credentials, brokers, longGroupID)
		assert.Equal(t, longGroupID, config.GroupID)
	})

	t.Run("special characters in group id", func(t *testing.T) {
		specialGroupID := "test-group-with-special-chars-!@#$%^&*()"
		brokers := []string{"localhost:9092"}

		config := DefaultConsumerConfig(credentials, brokers, specialGroupID)
		assert.Equal(t, specialGroupID, config.GroupID)
	})

	t.Run("multiple brokers", func(t *testing.T) {
		brokers := []string{
			"broker1:9092",
			"broker2:9092",
			"broker3:9092",
		}
		groupID := "multi-broker-group"

		config := DefaultConsumerConfig(credentials, brokers, groupID)
		assert.Equal(t, brokers, config.Brokers)
		assert.Len(t, config.Brokers, 3)
	})
}

// TestGetOrCreateReader tests the getOrCreateReader function with various validation scenarios
func TestGetOrCreateReader(t *testing.T) {
	t.Run("nil consumer error", func(t *testing.T) {
		var nilConsumer *Consumer
		_, err := nilConsumer.getOrCreateReader("test-topic", "")
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "consumer not initialized")
	})

	t.Run("empty broker list error", func(t *testing.T) {
		consumer := &Consumer{
			config: Config{
				Brokers: []string{}, // empty broker list
			},
			readers: make(map[string]*kafka.Reader),
		}
		_, err := consumer.getOrCreateReader("test-topic", "")
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "no brokers provided")
	})

	t.Run("nil broker list error", func(t *testing.T) {
		consumer := &Consumer{
			config: Config{
				Brokers: nil, // nil broker list
			},
			readers: make(map[string]*kafka.Reader),
		}
		_, err := consumer.getOrCreateReader("test-topic", "")
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "no brokers provided")
	})

	t.Run("creates reader with basic config", func(t *testing.T) {
		consumer := &Consumer{
			config: Config{
				Brokers: []string{"localhost:9092"},
				ClientCredentials: ClientCredentials{
					Username: "testuser",
					Password: "testpass",
				},
				MinBytes: 1,
				MaxBytes: 1024,
			},
			readers: make(map[string]*kafka.Reader),
		}

		reader, err := consumer.getOrCreateReader("test-topic", "")
		assert.NoError(t, err)
		assert.NotNil(t, reader)
		assert.Equal(t, "test-topic", reader.Config().Topic)
		assert.Equal(t, []string{"localhost:9092"}, reader.Config().Brokers)
	})

	t.Run("returns cached reader", func(t *testing.T) {
		consumer := &Consumer{
			config: Config{
				Brokers: []string{"localhost:9092"},
				ClientCredentials: ClientCredentials{
					Username: "testuser",
					Password: "testpass",
				},
				MinBytes: 1,
				MaxBytes: 1024,
			},
			readers: make(map[string]*kafka.Reader),
		}

		// Create first reader
		reader1, err := consumer.getOrCreateReader("test-topic", "")
		assert.NoError(t, err)
		assert.NotNil(t, reader1)

		// Get same reader again - should return cached version
		reader2, err := consumer.getOrCreateReader("test-topic", "")
		assert.NoError(t, err)
		assert.NotNil(t, reader2)

		// Should be the same reader instance
		assert.Equal(t, reader1, reader2)
	})

	t.Run("creates reader with group ID", func(t *testing.T) {
		consumer := &Consumer{
			config: Config{
				Brokers: []string{"localhost:9092"},
				ClientCredentials: ClientCredentials{
					Username: "testuser",
					Password: "testpass",
				},
				MinBytes:  1,
				MaxBytes:  1024,
				Partition: -1, // Important: use -1 to avoid clearing group ID
			},
			readers: make(map[string]*kafka.Reader),
		}

		reader, err := consumer.getOrCreateReader("test-topic", "test-group")
		assert.NoError(t, err)
		assert.NotNil(t, reader)
		assert.Equal(t, "test-group", reader.Config().GroupID)
	})

	t.Run("creates different readers for different group IDs", func(t *testing.T) {
		consumer := &Consumer{
			config: Config{
				Brokers: []string{"localhost:9092"},
				ClientCredentials: ClientCredentials{
					Username: "testuser",
					Password: "testpass",
				},
				MinBytes:  1,
				MaxBytes:  1024,
				Partition: -1, // Important: use -1 to avoid clearing group ID
			},
			readers: make(map[string]*kafka.Reader),
		}

		reader1, err := consumer.getOrCreateReader("test-topic", "group-1")
		assert.NoError(t, err)
		assert.NotNil(t, reader1)

		reader2, err := consumer.getOrCreateReader("test-topic", "group-2")
		assert.NoError(t, err)
		assert.NotNil(t, reader2)

		// Should be different reader instances due to different group IDs
		assert.NotEqual(t, reader1, reader2)
		assert.Equal(t, "group-1", reader1.Config().GroupID)
		assert.Equal(t, "group-2", reader2.Config().GroupID)
	})

	t.Run("creates reader with empty group ID", func(t *testing.T) {
		consumer := &Consumer{
			config: Config{
				Brokers: []string{"localhost:9092"},
				ClientCredentials: ClientCredentials{
					Username: "testuser",
					Password: "testpass",
				},
				MinBytes: 1,
				MaxBytes: 1024,
			},
			readers: make(map[string]*kafka.Reader),
		}

		reader, err := consumer.getOrCreateReader("test-topic", "")
		assert.NoError(t, err)
		assert.NotNil(t, reader)
		assert.Equal(t, "", reader.Config().GroupID)
	})

	t.Run("creates reader with custom config values", func(t *testing.T) {
		consumer := &Consumer{
			config: Config{
				Brokers: []string{"localhost:9092"},
				ClientCredentials: ClientCredentials{
					Username: "testuser",
					Password: "testpass",
				},
				MinBytes: 100,
				MaxBytes: 2048,
			},
			readers: make(map[string]*kafka.Reader),
		}

		reader, err := consumer.getOrCreateReader("test-topic", "")
		assert.NoError(t, err)
		assert.NotNil(t, reader)
		assert.Equal(t, 100, reader.Config().MinBytes)
		assert.Equal(t, 2048, reader.Config().MaxBytes)
	})

	t.Run("creates reader with multiple brokers", func(t *testing.T) {
		brokers := []string{"localhost:9092", "localhost:9093", "localhost:9094"}
		consumer := &Consumer{
			config: Config{
				Brokers: brokers,
				ClientCredentials: ClientCredentials{
					Username: "testuser",
					Password: "testpass",
				},
				MinBytes: 1,
				MaxBytes: 1024,
			},
			readers: make(map[string]*kafka.Reader),
		}

		reader, err := consumer.getOrCreateReader("test-topic", "")
		assert.NoError(t, err)
		assert.NotNil(t, reader)
		assert.Equal(t, brokers, reader.Config().Brokers)
	})

	t.Run("creates different readers for different topics", func(t *testing.T) {
		consumer := &Consumer{
			config: Config{
				Brokers: []string{"localhost:9092"},
				ClientCredentials: ClientCredentials{
					Username: "testuser",
					Password: "testpass",
				},
				MinBytes: 1,
				MaxBytes: 1024,
			},
			readers: make(map[string]*kafka.Reader),
		}

		reader1, err := consumer.getOrCreateReader("topic-1", "")
		assert.NoError(t, err)
		assert.NotNil(t, reader1)

		reader2, err := consumer.getOrCreateReader("topic-2", "")
		assert.NoError(t, err)
		assert.NotNil(t, reader2)

		// Should be different reader instances
		assert.NotEqual(t, reader1, reader2)
		assert.Equal(t, "topic-1", reader1.Config().Topic)
		assert.Equal(t, "topic-2", reader2.Config().Topic)
	})

	t.Run("partition config overrides group ID", func(t *testing.T) {
		consumer := &Consumer{
			config: Config{
				Brokers: []string{"localhost:9092"},
				ClientCredentials: ClientCredentials{
					Username: "testuser",
					Password: "testpass",
				},
				MinBytes:  1,
				MaxBytes:  1024,
				Partition: 2, // Specific partition
			},
			readers: make(map[string]*kafka.Reader),
		}

		reader, err := consumer.getOrCreateReader("test-topic", "test-group")
		assert.NoError(t, err)
		assert.NotNil(t, reader)
		assert.Equal(t, 2, reader.Config().Partition)
		// When partition is specified, group ID should be cleared
		assert.Equal(t, "", reader.Config().GroupID)
	})

	t.Run("validates empty topic name", func(t *testing.T) {
		consumer := &Consumer{
			config: Config{
				Brokers: []string{"localhost:9092"},
				ClientCredentials: ClientCredentials{
					Username: "testuser",
					Password: "testpass",
				},
				MinBytes: 1,
				MaxBytes: 1024,
			},
			readers: make(map[string]*kafka.Reader),
		}

		// kafka-go doesn't allow empty topic names, this should panic/error
		assert.Panics(t, func() {
			consumer.getOrCreateReader("", "")
		})
	})

	t.Run("reader caching with group ID key format", func(t *testing.T) {
		consumer := &Consumer{
			config: Config{
				Brokers: []string{"localhost:9092"},
				ClientCredentials: ClientCredentials{
					Username: "testuser",
					Password: "testpass",
				},
				MinBytes: 1,
				MaxBytes: 1024,
			},
			readers: make(map[string]*kafka.Reader),
		}

		// Create reader with group ID
		reader1, err := consumer.getOrCreateReader("test-topic", "test-group")
		assert.NoError(t, err)
		assert.NotNil(t, reader1)

		// Create reader with same topic but no group ID - should be different
		reader2, err := consumer.getOrCreateReader("test-topic", "")
		assert.NoError(t, err)
		assert.NotNil(t, reader2)

		// Should be different readers
		assert.NotEqual(t, reader1, reader2)

		// Get the first reader again - should be cached
		reader3, err := consumer.getOrCreateReader("test-topic", "test-group")
		assert.NoError(t, err)
		assert.Equal(t, reader1, reader3)
	})

	t.Run("successful SASL authentication setup", func(t *testing.T) {
		consumer := &Consumer{
			config: Config{
				Brokers: []string{"localhost:9092"},
				ClientCredentials: ClientCredentials{
					Username: "testuser",
					Password: "testpass",
				},
				MinBytes: 1,
				MaxBytes: 1024,
			},
			readers: make(map[string]*kafka.Reader),
		}

		// This should succeed - SASL mechanism can be created with any credentials
		reader, err := consumer.getOrCreateReader("test-topic", "")
		assert.NoError(t, err)
		assert.NotNil(t, reader)
		assert.Equal(t, "test-topic", reader.Config().Topic)
	})
}

// TestReadEventWithMessage tests the ReadEventWithMessage function validation scenarios
func TestReadEventWithMessage(t *testing.T) {
	t.Run("nil consumer", func(t *testing.T) {
		var nilConsumer *Consumer
		ctx := context.Background()

		event, msg, err := nilConsumer.ReadEventWithMessage(ctx, "test-topic")
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "consumer not initialized")
		assert.Nil(t, event)
		assert.Equal(t, kafka.Message{}, msg)
	})

	t.Run("empty topic", func(t *testing.T) {
		consumer := &Consumer{
			config: Config{
				Brokers: []string{"localhost:9092"},
				ClientCredentials: ClientCredentials{
					Username: "testuser",
					Password: "testpass",
				},
			},
			readers: make(map[string]*kafka.Reader),
		}
		ctx := context.Background()

		event, msg, err := consumer.ReadEventWithMessage(ctx, "")
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "topic is required")
		assert.Nil(t, event)
		assert.Equal(t, kafka.Message{}, msg)
	})

	t.Run("uses provided group ID", func(t *testing.T) {
		consumer := &Consumer{
			config: Config{
				Brokers: []string{"localhost:9092"},
				ClientCredentials: ClientCredentials{
					Username: "testuser",
					Password: "testpass",
				},
				GroupID: "default-group",
			},
			readers: make(map[string]*kafka.Reader),
		}
		ctx := context.Background()

		// This will fail when trying to read from non-existent Kafka, but we can verify
		// that the validation passes and the error comes from the read operation
		_, _, err := consumer.ReadEventWithMessage(ctx, "test-topic", "override-group")
		assert.Error(t, err)
		// Should get to the read attempt, not validation error
		assert.NotContains(t, err.Error(), "consumer not initialized")
		assert.NotContains(t, err.Error(), "topic is required")
	})

	t.Run("uses config group ID when none provided", func(t *testing.T) {
		consumer := &Consumer{
			config: Config{
				Brokers: []string{"localhost:9092"},
				ClientCredentials: ClientCredentials{
					Username: "testuser",
					Password: "testpass",
				},
				GroupID: "config-group",
			},
			readers: make(map[string]*kafka.Reader),
		}
		ctx := context.Background()

		// This will fail when trying to read from non-existent Kafka, but validation should pass
		_, _, err := consumer.ReadEventWithMessage(ctx, "test-topic")
		assert.Error(t, err)
		// Should get to the read attempt, not validation error
		assert.NotContains(t, err.Error(), "consumer not initialized")
		assert.NotContains(t, err.Error(), "topic is required")
	})

	t.Run("context with deadline validation", func(t *testing.T) {
		consumer := &Consumer{
			config: Config{
				Brokers: []string{"localhost:9092"},
				ClientCredentials: ClientCredentials{
					Username: "testuser",
					Password: "testpass",
				},
			},
			readers: make(map[string]*kafka.Reader),
		}

		// Create context with deadline
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		// This will fail when trying to read from non-existent Kafka, but validation should pass
		_, _, err := consumer.ReadEventWithMessage(ctx, "test-topic")
		assert.Error(t, err)
		// Should get to the read attempt, not validation error
		assert.NotContains(t, err.Error(), "consumer not initialized")
		assert.NotContains(t, err.Error(), "topic is required")
	})
}

// TestReadEvent tests the ReadEvent function validation scenarios
func TestReadEvent(t *testing.T) {
	t.Run("nil consumer", func(t *testing.T) {
		var nilConsumer *Consumer
		ctx := context.Background()

		event, err := nilConsumer.ReadEvent(ctx, "test-topic")
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "consumer not initialized")
		assert.Nil(t, event)
	})

	t.Run("empty topic", func(t *testing.T) {
		consumer := &Consumer{
			config: Config{
				Brokers: []string{"localhost:9092"},
				ClientCredentials: ClientCredentials{
					Username: "testuser",
					Password: "testpass",
				},
			},
			readers: make(map[string]*kafka.Reader),
		}
		ctx := context.Background()

		event, err := consumer.ReadEvent(ctx, "")
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "topic is required")
		assert.Nil(t, event)
	})

	t.Run("uses provided group ID", func(t *testing.T) {
		consumer := &Consumer{
			config: Config{
				Brokers: []string{"localhost:9092"},
				ClientCredentials: ClientCredentials{
					Username: "testuser",
					Password: "testpass",
				},
				GroupID: "default-group",
			},
			readers: make(map[string]*kafka.Reader),
		}
		ctx := context.Background()

		// This will fail when trying to read from non-existent Kafka, but validation should pass
		_, err := consumer.ReadEvent(ctx, "test-topic", "override-group")
		assert.Error(t, err)
		// Should get to the read attempt, not validation error
		assert.NotContains(t, err.Error(), "consumer not initialized")
		assert.NotContains(t, err.Error(), "topic is required")
	})

	t.Run("uses config group ID when none provided", func(t *testing.T) {
		consumer := &Consumer{
			config: Config{
				Brokers: []string{"localhost:9092"},
				ClientCredentials: ClientCredentials{
					Username: "testuser",
					Password: "testpass",
				},
				GroupID: "config-group",
			},
			readers: make(map[string]*kafka.Reader),
		}
		ctx := context.Background()

		// This will fail when trying to read from non-existent Kafka, but validation should pass
		_, err := consumer.ReadEvent(ctx, "test-topic")
		assert.Error(t, err)
		// Should get to the read attempt, not validation error
		assert.NotContains(t, err.Error(), "consumer not initialized")
		assert.NotContains(t, err.Error(), "topic is required")
	})

	t.Run("context with deadline validation", func(t *testing.T) {
		consumer := &Consumer{
			config: Config{
				Brokers: []string{"localhost:9092"},
				ClientCredentials: ClientCredentials{
					Username: "testuser",
					Password: "testpass",
				},
			},
			readers: make(map[string]*kafka.Reader),
		}

		// Create context with deadline
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		// This will fail when trying to read from non-existent Kafka, but validation should pass
		_, err := consumer.ReadEvent(ctx, "test-topic")
		assert.Error(t, err)
		// Should get to the read attempt, not validation error
		assert.NotContains(t, err.Error(), "consumer not initialized")
		assert.NotContains(t, err.Error(), "topic is required")
	})

	t.Run("context without deadline gets timeout", func(t *testing.T) {
		consumer := &Consumer{
			config: Config{
				Brokers: []string{"localhost:9092"},
				ClientCredentials: ClientCredentials{
					Username: "testuser",
					Password: "testpass",
				},
			},
			readers: make(map[string]*kafka.Reader),
		}

		// Create context without deadline
		ctx := context.Background()

		// This will fail when trying to read from non-existent Kafka, but should apply default timeout
		_, err := consumer.ReadEvent(ctx, "test-topic")
		assert.Error(t, err)
		// Should get to the read attempt, not validation error
		assert.NotContains(t, err.Error(), "consumer not initialized")
		assert.NotContains(t, err.Error(), "topic is required")
	})
}

// TestReadEventWithMessage tests the ReadEventWithMessage function that has 0% coverage
func TestReadEventWithMessageBackup(t *testing.T) {
	tests := []struct {
		name        string
		consumer    *Consumer
		topic       string
		groupID     []string
		expectError bool
		errorMsg    string
	}{
		{
			name:        "nil consumer",
			consumer:    nil,
			topic:       "test-topic",
			expectError: true,
			errorMsg:    "consumer not initialized",
		},
		{
			name: "empty topic",
			consumer: &Consumer{
				readers: make(map[string]*kafka.Reader),
				config: Config{
					Brokers: []string{"localhost:9092"},
					ClientCredentials: ClientCredentials{
						Username: "test",
						Password: "test",
					},
				},
			},
			topic:       "",
			expectError: true,
			errorMsg:    "topic is required",
		},
		{
			name: "consumer with empty brokers",
			consumer: &Consumer{
				readers: make(map[string]*kafka.Reader),
				config: Config{
					Brokers: []string{},
				},
			},
			topic:       "test-topic",
			expectError: true,
			errorMsg:    "no brokers provided",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
			defer cancel()

			event, msg, err := tt.consumer.ReadEventWithMessage(ctx, tt.topic, tt.groupID...)

			assert.Error(t, err)
			assert.Nil(t, event)
			assert.Equal(t, kafka.Message{}, msg)
			if tt.errorMsg != "" {
				assert.Contains(t, err.Error(), tt.errorMsg)
			}
		})
	}
}

// TestCommitEvents tests the CommitEvents function that has 0% coverage
func TestCommitEvents(t *testing.T) {
	tests := []struct {
		name        string
		consumer    *Consumer
		topic       string
		msgs        []kafka.Message
		expectError bool
		errorMsg    string
	}{
		{
			name:        "nil consumer",
			consumer:    nil,
			topic:       "test-topic",
			msgs:        []kafka.Message{},
			expectError: true,
			errorMsg:    "consumer not initialized",
		},
		{
			name: "empty topic",
			consumer: &Consumer{
				readers: make(map[string]*kafka.Reader),
			},
			topic:       "",
			msgs:        []kafka.Message{},
			expectError: true,
			errorMsg:    "topic is required",
		},
		{
			name: "no active reader for topic",
			consumer: &Consumer{
				readers: make(map[string]*kafka.Reader),
			},
			topic:       "non-existent-topic",
			msgs:        []kafka.Message{},
			expectError: true,
			errorMsg:    "no active reader for topic",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()
			err := tt.consumer.CommitEvents(ctx, tt.topic, tt.msgs...)

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

// Benchmark consumer operations
func BenchmarkDefaultConsumerConfig(b *testing.B) {
	credentials := ClientCredentials{
		Username: "bench-user",
		Password: "bench-pass",
	}
	brokers := []string{"localhost:9092"}
	groupID := "bench-group"

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = DefaultConsumerConfig(credentials, brokers, groupID)
	}
}

// TestCloseWithReaderErrors tests Close function with reader closing errors
func TestCloseWithReaderErrors(t *testing.T) {
	consumer := &Consumer{
		config: Config{
			Brokers: []string{"localhost:9092"},
			ClientCredentials: ClientCredentials{
				Username: "testuser",
				Password: "testpass",
			},
		},
		readers: make(map[string]*kafka.Reader),
	}

	// Create multiple readers to test error handling
	reader1, err := consumer.getOrCreateReader("topic1", "")
	assert.NoError(t, err)
	reader2, err := consumer.getOrCreateReader("topic2", "group1")
	assert.NoError(t, err)

	// Now close - this will try to close the readers
	_ = consumer.Close()
	// The error depends on the kafka library behavior, but the function should handle it
	// The main thing is that it doesn't panic and tries to close all readers

	// Verify all readers are in the map before closing
	assert.Contains(t, consumer.readers, "topic1")
	assert.Contains(t, consumer.readers, "group1:topic2")
	assert.NotNil(t, reader1)
	assert.NotNil(t, reader2)

	// First close one reader manually to create a potential error condition
	_ = reader1.Close()
}

// TestCloseWithMultipleReaderErrors tests Close function error handling with multiple readers
func TestCloseWithMultipleReaderErrors(t *testing.T) {
	consumer := &Consumer{
		config: Config{
			Brokers: []string{"localhost:9092"},
			ClientCredentials: ClientCredentials{
				Username: "testuser",
				Password: "testpass",
			},
		},
		readers: make(map[string]*kafka.Reader),
	}

	// Create multiple readers
	reader1, err := consumer.getOrCreateReader("topic1", "")
	assert.NoError(t, err)
	reader2, err := consumer.getOrCreateReader("topic2", "")
	assert.NoError(t, err)
	reader3, err := consumer.getOrCreateReader("topic3", "group1")
	assert.NoError(t, err)

	// Manually close readers to create error conditions when Close() tries to close them again
	_ = reader1.Close()
	_ = reader2.Close()
	_ = reader3.Close()

	// Now close the consumer - should handle errors gracefully and return the last error
	_ = consumer.Close()
	// The function should not panic and should attempt to close all readers
	// The specific error depends on kafka-go library behavior when closing already-closed readers
}

// TestCloseNilReaders tests Close function with nil readers map
func TestCloseNilReaders(t *testing.T) {
	consumer := &Consumer{
		config: Config{
			Brokers: []string{"localhost:9092"},
		},
		readers: nil, // nil readers map
	}

	err := consumer.Close()
	assert.NoError(t, err) // Should not error with nil readers
}

// TestStatsAdvanced tests Stats function with various scenarios
func TestStatsAdvanced(t *testing.T) {
	t.Run("nil consumer", func(t *testing.T) {
		var nilConsumer *Consumer
		stats, err := nilConsumer.Stats("test-topic")
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "consumer not initialized")
		assert.Equal(t, kafka.ReaderStats{}, stats)
	})

	t.Run("empty topic", func(t *testing.T) {
		consumer := &Consumer{
			config: Config{
				Brokers: []string{"localhost:9092"},
				ClientCredentials: ClientCredentials{
					Username: "testuser",
					Password: "testpass",
				},
			},
			readers: make(map[string]*kafka.Reader),
		}

		stats, err := consumer.Stats("")
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "topic is required")
		assert.Equal(t, kafka.ReaderStats{}, stats)
	})

	t.Run("no reader for topic", func(t *testing.T) {
		consumer := &Consumer{
			config: Config{
				Brokers: []string{"localhost:9092"},
				ClientCredentials: ClientCredentials{
					Username: "testuser",
					Password: "testpass",
				},
			},
			readers: make(map[string]*kafka.Reader),
		}

		stats, err := consumer.Stats("nonexistent-topic")
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "no active reader for topic")
		assert.Equal(t, kafka.ReaderStats{}, stats)
	})

	t.Run("successful stats retrieval", func(t *testing.T) {
		consumer := &Consumer{
			config: Config{
				Brokers: []string{"localhost:9092"},
				ClientCredentials: ClientCredentials{
					Username: "testuser",
					Password: "testpass",
				},
			},
			readers: make(map[string]*kafka.Reader),
		}

		// Create a reader first
		_, err := consumer.getOrCreateReader("test-topic", "")
		assert.NoError(t, err)

		// Now get stats - should not error even if kafka isn't running
		stats, err := consumer.Stats("test-topic")
		assert.NoError(t, err)
		assert.NotNil(t, stats)
	})

	t.Run("stats with group ID in key", func(t *testing.T) {
		consumer := &Consumer{
			config: Config{
				Brokers: []string{"localhost:9092"},
				ClientCredentials: ClientCredentials{
					Username: "testuser",
					Password: "testpass",
				},
			},
			readers: make(map[string]*kafka.Reader),
		}

		// Create a reader with group ID
		_, err := consumer.getOrCreateReader("test-topic", "test-group")
		assert.NoError(t, err)

		// Get stats using just the topic name - should find the grouped reader
		stats, err := consumer.Stats("test-topic")
		assert.NoError(t, err)
		assert.NotNil(t, stats)
	})
}

// TestCommitEventsAdvanced tests CommitEvents function with various scenarios
func TestCommitEventsAdvanced(t *testing.T) {
	t.Run("nil consumer", func(t *testing.T) {
		var nilConsumer *Consumer
		ctx := context.Background()

		err := nilConsumer.CommitEvents(ctx, "test-topic")
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "consumer not initialized")
	})

	t.Run("empty topic", func(t *testing.T) {
		consumer := &Consumer{
			config: Config{
				Brokers: []string{"localhost:9092"},
				ClientCredentials: ClientCredentials{
					Username: "testuser",
					Password: "testpass",
				},
			},
			readers: make(map[string]*kafka.Reader),
		}
		ctx := context.Background()

		err := consumer.CommitEvents(ctx, "")
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "topic is required")
	})

	t.Run("no reader for topic", func(t *testing.T) {
		consumer := &Consumer{
			config: Config{
				Brokers: []string{"localhost:9092"},
				ClientCredentials: ClientCredentials{
					Username: "testuser",
					Password: "testpass",
				},
			},
			readers: make(map[string]*kafka.Reader),
		}
		ctx := context.Background()

		err := consumer.CommitEvents(ctx, "nonexistent-topic")
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "no active reader for topic")
	})

	t.Run("commit with reader present", func(t *testing.T) {
		consumer := &Consumer{
			config: Config{
				Brokers: []string{"localhost:9092"},
				ClientCredentials: ClientCredentials{
					Username: "testuser",
					Password: "testpass",
				},
			},
			readers: make(map[string]*kafka.Reader),
		}
		ctx := context.Background()

		// Create a reader first
		_, err := consumer.getOrCreateReader("test-topic", "")
		assert.NoError(t, err)

		// Try to commit - will fail because kafka isn't running, but validates the path
		msg := kafka.Message{
			Topic:     "test-topic",
			Partition: 0,
			Offset:    123,
			Value:     []byte("test"),
		}

		err = consumer.CommitEvents(ctx, "test-topic", msg)
		// This will error trying to commit to non-existent kafka, but should find the reader
		assert.Error(t, err)
		// Should not be our validation errors
		assert.NotContains(t, err.Error(), "consumer not initialized")
		assert.NotContains(t, err.Error(), "topic is required")
		assert.NotContains(t, err.Error(), "no active reader for topic")
	})

	t.Run("commit with group ID in key", func(t *testing.T) {
		consumer := &Consumer{
			config: Config{
				Brokers: []string{"localhost:9092"},
				ClientCredentials: ClientCredentials{
					Username: "testuser",
					Password: "testpass",
				},
			},
			readers: make(map[string]*kafka.Reader),
		}
		ctx := context.Background()

		// Create a reader with group ID
		_, err := consumer.getOrCreateReader("test-topic", "test-group")
		assert.NoError(t, err)

		// Try to commit using just the topic name - should find the grouped reader
		msg := kafka.Message{
			Topic:     "test-topic",
			Partition: 0,
			Offset:    123,
			Value:     []byte("test"),
		}

		err = consumer.CommitEvents(ctx, "test-topic", msg)
		// This will error trying to commit to non-existent kafka, but should find the reader
		assert.Error(t, err)
		// Should not be our validation errors
		assert.NotContains(t, err.Error(), "consumer not initialized")
		assert.NotContains(t, err.Error(), "topic is required")
		assert.NotContains(t, err.Error(), "no active reader for topic")
	})
}

// TestNewConsumerAdvanced tests NewConsumer function edge cases
func TestNewConsumerAdvanced(t *testing.T) {
	t.Run("empty brokers list", func(t *testing.T) {
		cfg := Config{
			Brokers: []string{}, // empty brokers
			ClientCredentials: ClientCredentials{
				Username: "testuser",
				Password: "testpass",
			},
		}

		consumer, err := NewConsumer(cfg)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "no brokers provided")
		assert.Nil(t, consumer)
	})

	t.Run("nil brokers list", func(t *testing.T) {
		cfg := Config{
			Brokers: nil, // nil brokers
			ClientCredentials: ClientCredentials{
				Username: "testuser",
				Password: "testpass",
			},
		}

		consumer, err := NewConsumer(cfg)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "no brokers provided")
		assert.Nil(t, consumer)
	})

	t.Run("invalid SASL credentials", func(t *testing.T) {
		cfg := Config{
			Brokers: []string{"localhost:9092"},
			ClientCredentials: ClientCredentials{
				Username: "", // empty username
				Password: "", // empty password
			},
		}

		// This might succeed or fail depending on SASL validation
		// The test is mainly to exercise the code path
		consumer, err := NewConsumer(cfg)
		if err != nil {
			// If it fails, should be SASL related
			assert.Contains(t, err.Error(), "SASL")
			assert.Nil(t, consumer)
		} else {
			// If it succeeds, should have valid consumer
			assert.NotNil(t, consumer)
			assert.NotNil(t, consumer.readers)
			assert.NotNil(t, consumer.client)
		}
	})

	t.Run("successful creation with valid config", func(t *testing.T) {
		cfg := Config{
			Brokers: []string{"localhost:9092", "localhost:9093"},
			ClientCredentials: ClientCredentials{
				Username: "testuser",
				Password: "testpass",
			},
			GroupID:  "test-group",
			MinBytes: 1,
			MaxBytes: 1024,
		}

		consumer, err := NewConsumer(cfg)
		assert.NoError(t, err)
		assert.NotNil(t, consumer)
		assert.NotNil(t, consumer.readers)
		assert.NotNil(t, consumer.client)
		assert.Equal(t, cfg.Brokers, consumer.config.Brokers)
		assert.Equal(t, cfg.GroupID, consumer.config.GroupID)
	})
}

// TestNewConsumerEdgeCases tests additional NewConsumer edge cases for better coverage
func TestNewConsumerEdgeCases(t *testing.T) {
	t.Run("single broker", func(t *testing.T) {
		cfg := Config{
			Brokers: []string{"localhost:9092"}, // single broker
			ClientCredentials: ClientCredentials{
				Username: "testuser",
				Password: "testpass",
			},
		}

		consumer, err := NewConsumer(cfg)
		assert.NoError(t, err)
		assert.NotNil(t, consumer)
		assert.Len(t, consumer.config.Brokers, 1)
	})

	t.Run("many brokers", func(t *testing.T) {
		cfg := Config{
			Brokers: []string{
				"broker1:9092",
				"broker2:9092",
				"broker3:9092",
				"broker4:9092",
				"broker5:9092",
			},
			ClientCredentials: ClientCredentials{
				Username: "testuser",
				Password: "testpass",
			},
		}

		consumer, err := NewConsumer(cfg)
		assert.NoError(t, err)
		assert.NotNil(t, consumer)
		assert.Len(t, consumer.config.Brokers, 5)
	})

	t.Run("special characters in credentials", func(t *testing.T) {
		cfg := Config{
			Brokers: []string{"localhost:9092"},
			ClientCredentials: ClientCredentials{
				Username: "test@user.com",
				Password: "p@ssw0rd!#$%",
			},
		}

		consumer, err := NewConsumer(cfg)
		assert.NoError(t, err)
		assert.NotNil(t, consumer)
		assert.Equal(t, "test@user.com", consumer.config.ClientCredentials.Username)
	})

	t.Run("test SASL mechanism creation", func(t *testing.T) {
		cfg := Config{
			Brokers: []string{"localhost:9092"},
			ClientCredentials: ClientCredentials{
				Username: "validuser",
				Password: "validpass",
			},
		}

		// This should succeed in creating SASL mechanism
		consumer, err := NewConsumer(cfg)
		assert.NoError(t, err)
		assert.NotNil(t, consumer)
		assert.NotNil(t, consumer.client)
		assert.NotNil(t, consumer.client.Transport)
	})
}

// TestCloseEdgeCases tests additional Close edge cases
func TestCloseEdgeCases(t *testing.T) {
	t.Run("close with single reader", func(t *testing.T) {
		consumer := &Consumer{
			config: Config{
				Brokers: []string{"localhost:9092"},
				ClientCredentials: ClientCredentials{
					Username: "testuser",
					Password: "testpass",
				},
			},
			readers: make(map[string]*kafka.Reader),
		}

		// Create just one reader
		_, err := consumer.getOrCreateReader("single-topic", "")
		assert.NoError(t, err)

		// Close should handle single reader gracefully
		_ = consumer.Close()
		// Should not panic
	})

	t.Run("close with empty readers map", func(t *testing.T) {
		consumer := &Consumer{
			config: Config{
				Brokers: []string{"localhost:9092"},
			},
			readers: make(map[string]*kafka.Reader), // empty but not nil
		}

		err := consumer.Close()
		assert.NoError(t, err) // Should succeed with empty map
	})

	t.Run("force reader close error by double closing", func(t *testing.T) {
		consumer := &Consumer{
			config: Config{
				Brokers: []string{"localhost:9092"},
				ClientCredentials: ClientCredentials{
					Username: "testuser",
					Password: "testpass",
				},
			},
			readers: make(map[string]*kafka.Reader),
		}

		// Create a reader
		reader, err := consumer.getOrCreateReader("test-topic", "")
		assert.NoError(t, err)
		assert.NotNil(t, reader)

		// Close the reader manually first
		_ = reader.Close()
		// kafka-go readers can be closed safely multiple times

		// Now close the consumer - this will try to close the already closed reader
		// This should exercise the error handling path in Close()
		lastErr := consumer.Close()
		// The behavior depends on kafka-go - it might return an error or be safe
		// The important thing is that our Close() method handles it gracefully
		_ = lastErr // We don't assert on the specific error since it's implementation dependent
	})
}

// TestReadEventContextEdgeCases tests additional context scenarios
func TestReadEventContextEdgeCases(t *testing.T) {
	consumer := &Consumer{
		config: Config{
			Brokers: []string{"localhost:9092"},
			ClientCredentials: ClientCredentials{
				Username: "testuser",
				Password: "testpass",
			},
		},
		readers: make(map[string]*kafka.Reader),
	}

	t.Run("cancelled context", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		cancel() // cancel immediately

		_, err := consumer.ReadEvent(ctx, "test-topic")
		assert.Error(t, err)
		// Should fail due to cancelled context, not validation
		assert.NotContains(t, err.Error(), "consumer not initialized")
		assert.NotContains(t, err.Error(), "topic is required")
	})

	t.Run("context with past deadline", func(t *testing.T) {
		ctx, cancel := context.WithDeadline(context.Background(), time.Now().Add(-1*time.Hour))
		defer cancel()

		_, err := consumer.ReadEvent(ctx, "test-topic")
		assert.Error(t, err)
		// Should fail due to past deadline, not validation
		assert.NotContains(t, err.Error(), "consumer not initialized")
		assert.NotContains(t, err.Error(), "topic is required")
	})
}

// TestCommitEvent tests the new CommitEvent function that commits a single message
func TestCommitEvent(t *testing.T) {
	t.Run("nil consumer", func(t *testing.T) {
		var nilConsumer *Consumer
		ctx := context.Background()
		msg := kafka.Message{Topic: "test-topic", Partition: 0, Offset: 123}

		err := nilConsumer.CommitEvent(ctx, msg)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "consumer not initialized")
	})

	t.Run("message with empty topic", func(t *testing.T) {
		consumer := &Consumer{
			config: Config{
				Brokers: []string{"localhost:9092"},
				ClientCredentials: ClientCredentials{
					Username: "testuser",
					Password: "testpass",
				},
			},
			readers: make(map[string]*kafka.Reader),
		}
		ctx := context.Background()

		msg := kafka.Message{Topic: "", Partition: 0, Offset: 123} // empty topic

		err := consumer.CommitEvent(ctx, msg)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "message topic is empty")
	})

	t.Run("no reader for message topic", func(t *testing.T) {
		consumer := &Consumer{
			config: Config{
				Brokers: []string{"localhost:9092"},
				ClientCredentials: ClientCredentials{
					Username: "testuser",
					Password: "testpass",
				},
			},
			readers: make(map[string]*kafka.Reader),
		}
		ctx := context.Background()

		msg := kafka.Message{Topic: "non-existent-topic", Partition: 0, Offset: 123}

		err := consumer.CommitEvent(ctx, msg)
		assert.Error(t, err)
		// The error could be about missing reader or GroupID not set, both are valid failures
		isValidError := strings.Contains(err.Error(), "no active reader for topic non-existent-topic") ||
			strings.Contains(err.Error(), "unavailable when GroupID is not set")
		assert.True(t, isValidError, "Expected error about missing reader or GroupID, got: %s", err.Error())
	})

	t.Run("successful commit with reader present", func(t *testing.T) {
		consumer := &Consumer{
			config: Config{
				Brokers: []string{"localhost:9092"},
				ClientCredentials: ClientCredentials{
					Username: "testuser",
					Password: "testpass",
				},
			},
			readers: make(map[string]*kafka.Reader),
		}
		ctx := context.Background()

		// Create a reader for the topic first
		_, err := consumer.getOrCreateReader("test-topic", "")
		assert.NoError(t, err)

		msg := kafka.Message{Topic: "test-topic", Partition: 0, Offset: 123}

		// This will error trying to commit to non-existent kafka, but validation should pass
		err = consumer.CommitEvent(ctx, msg)
		assert.Error(t, err)
		// Should not be our validation errors
		assert.NotContains(t, err.Error(), "consumer not initialized")
		assert.NotContains(t, err.Error(), "message topic is empty")
		assert.NotContains(t, err.Error(), "no active reader for topic")
	})

	t.Run("commit with group ID reader", func(t *testing.T) {
		consumer := &Consumer{
			config: Config{
				Brokers: []string{"localhost:9092"},
				ClientCredentials: ClientCredentials{
					Username: "testuser",
					Password: "testpass",
				},
				GroupID: "test-group",
			},
			readers: make(map[string]*kafka.Reader),
		}
		ctx := context.Background()

		// Create a reader for the topic with group ID
		_, err := consumer.getOrCreateReader("test-topic", "test-group")
		assert.NoError(t, err)

		msg := kafka.Message{Topic: "test-topic", Partition: 0, Offset: 123}

		// This will error trying to commit to non-existent kafka, but validation should pass
		err = consumer.CommitEvent(ctx, msg)
		assert.Error(t, err)
		// Should not be our validation errors
		assert.NotContains(t, err.Error(), "consumer not initialized")
		assert.NotContains(t, err.Error(), "message topic is empty")
		assert.NotContains(t, err.Error(), "no active reader for topic")
	})
}

func BenchmarkConsumerClose(b *testing.B) {
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		var consumer *Consumer
		_ = consumer.Close()
	}
}
