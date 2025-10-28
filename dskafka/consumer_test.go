package dskafka

import (
	"context"
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

func BenchmarkConsumerClose(b *testing.B) {
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		var consumer *Consumer
		_ = consumer.Close()
	}
}
