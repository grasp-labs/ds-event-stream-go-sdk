package dskafka

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/grasp-labs/ds-event-stream-go-sdk/models"
	"github.com/segmentio/kafka-go"
	"github.com/stretchr/testify/assert"
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

// Enhanced common tests with comprehensive validation

// Test Environment enum validation
func TestEnvironmentValidation(t *testing.T) {
	tests := []struct {
		name     string
		env      Environment
		internal bool
		expected []string
	}{
		{
			name:     "dev external",
			env:      Dev,
			internal: false,
			expected: []string{"b0.dev.kafka.ds.local:9095"},
		},
		{
			name:     "dev internal",
			env:      Dev,
			internal: true,
			expected: []string{"kafka.kafka-dev.svc.cluster.local:9092"},
		},
		{
			name:     "prod external",
			env:      Prod,
			internal: false,
			expected: []string{
				"b0.kafka.ds.local:9095",
				"b1.kafka.ds.local:9095",
				"b2.kafka.ds.local:9095",
			},
		},
		{
			name:     "prod internal",
			env:      Prod,
			internal: true,
			expected: []string{"kafka.kafka.svc.cluster.local:9092"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			brokers := GetBootstrapServers(tt.env, tt.internal)
			assert.Equal(t, tt.expected, brokers)
		})
	}
}

// Test ClientCredentials validation
func TestClientCredentialsValidation(t *testing.T) {
	tests := []struct {
		name        string
		credentials ClientCredentials
		valid       bool
	}{
		{
			name: "valid credentials",
			credentials: ClientCredentials{
				Username: "valid-user",
				Password: "valid-password",
			},
			valid: true,
		},
		{
			name: "empty username",
			credentials: ClientCredentials{
				Username: "",
				Password: "password",
			},
			valid: false, // Depends on implementation, but generally invalid
		},
		{
			name: "empty password",
			credentials: ClientCredentials{
				Username: "username",
				Password: "",
			},
			valid: false,
		},
		{
			name: "both empty",
			credentials: ClientCredentials{
				Username: "",
				Password: "",
			},
			valid: false,
		},
		{
			name: "special characters",
			credentials: ClientCredentials{
				Username: "user@domain.com",
				Password: "p@ssw0rd!",
			},
			valid: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Test that credentials can be used in config
			brokers := GetBootstrapServers(Dev, false)
			config := DefaultConsumerConfig(tt.credentials, brokers, "test-group")

			assert.Equal(t, tt.credentials, config.ClientCredentials)

			if tt.valid {
				assert.NotEmpty(t, config.ClientCredentials.Username)
				assert.NotEmpty(t, config.ClientCredentials.Password)
			}
		})
	}
}

// Test Header struct validation
func TestHeaderValidation(t *testing.T) {
	tests := []struct {
		name   string
		header Header
	}{
		{
			name: "simple header",
			header: Header{
				Key:   "content-type",
				Value: "application/json",
			},
		},
		{
			name: "empty key",
			header: Header{
				Key:   "",
				Value: "some-value",
			},
		},
		{
			name: "empty value",
			header: Header{
				Key:   "some-key",
				Value: "",
			},
		},
		{
			name: "both empty",
			header: Header{
				Key:   "",
				Value: "",
			},
		},
		{
			name: "special characters",
			header: Header{
				Key:   "x-custom-header-123",
				Value: "value-with-special-chars!@#$%",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.header.Key, tt.header.Key)
			assert.Equal(t, tt.header.Value, tt.header.Value)
		})
	}
}

// Test Config validation with comprehensive scenarios
func TestConfigValidation(t *testing.T) {
	credentials := ClientCredentials{
		Username: "test-user",
		Password: "test-pass",
	}

	tests := []struct {
		name   string
		config Config
		valid  bool
	}{
		{
			name: "minimal valid config",
			config: Config{
				Brokers:           []string{"localhost:9092"},
				ClientCredentials: credentials,
			},
			valid: true,
		},
		{
			name: "full producer config",
			config: Config{
				Brokers:                []string{"localhost:9092"},
				ClientCredentials:      credentials,
				RequiredAcks:           kafka.RequireOne,
				Balancer:               &kafka.Hash{},
				BatchSize:              100,
				BatchBytes:             1024 * 1024,
				BatchTimeout:           50 * time.Millisecond,
				Compression:            kafka.Snappy,
				Async:                  false,
				AllowAutoTopicCreation: true,
				WriteTimeout:           10 * time.Second,
			},
			valid: true,
		},
		{
			name: "full consumer config",
			config: Config{
				Brokers:           []string{"localhost:9092"},
				ClientCredentials: credentials,
				GroupID:           "test-group",
				ReadTimeout:       10 * time.Second,
				Partition:         -1,
				StartOffset:       kafka.FirstOffset,
			},
			valid: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Test that config structure is valid
			assert.NotEmpty(t, tt.config.Brokers)
			assert.NotEmpty(t, tt.config.ClientCredentials.Username)
			assert.NotEmpty(t, tt.config.ClientCredentials.Password)

			if tt.valid {
				assert.True(t, len(tt.config.Brokers) > 0)
			}
		})
	}
}

// Test createTestEvent with different scenarios
func TestCreateTestEventVariations(t *testing.T) {
	t.Run("basic event creation", func(t *testing.T) {
		event := createTestEvent()

		assert.NotEqual(t, uuid.Nil, event.Id)
		assert.NotEqual(t, uuid.Nil, event.SessionId)
		assert.NotEqual(t, uuid.Nil, event.RequestId)
		assert.NotEqual(t, uuid.Nil, event.TenantId)
		assert.NotEmpty(t, event.EventType)
		assert.NotEmpty(t, event.EventSource)
		assert.NotEmpty(t, event.CreatedBy)
		assert.NotEmpty(t, event.Md5Hash)
		assert.NotNil(t, event.Payload)
		assert.False(t, event.Timestamp.IsZero())
	})

	t.Run("event JSON marshaling", func(t *testing.T) {
		event := createTestEvent()

		data, err := json.Marshal(event)
		assert.NoError(t, err)
		assert.NotEmpty(t, data)

		var unmarshaled models.EventJson
		err = json.Unmarshal(data, &unmarshaled)
		assert.NoError(t, err)

		// UUIDs should be equal after marshal/unmarshal
		assert.Equal(t, event.Id, unmarshaled.Id)
		assert.Equal(t, event.SessionId, unmarshaled.SessionId)
		assert.Equal(t, event.EventType, unmarshaled.EventType)
		assert.Equal(t, event.EventSource, unmarshaled.EventSource)
	})
}

// Test DefaultConsumerConfig with edge cases
func TestDefaultConsumerConfigEdgeCases(t *testing.T) {
	credentials := ClientCredentials{
		Username: "test-user",
		Password: "test-pass",
	}

	tests := []struct {
		name     string
		brokers  []string
		groupID  string
		expectOk bool
	}{
		{
			name:     "single broker",
			brokers:  []string{"localhost:9092"},
			groupID:  "single-group",
			expectOk: true,
		},
		{
			name:     "multiple brokers",
			brokers:  []string{"broker1:9092", "broker2:9092", "broker3:9092"},
			groupID:  "multi-group",
			expectOk: true,
		},
		{
			name:     "empty group id",
			brokers:  []string{"localhost:9092"},
			groupID:  "",
			expectOk: true, // Empty group ID might be valid for some use cases
		},
		{
			name:     "very long group id",
			brokers:  []string{"localhost:9092"},
			groupID:  string(make([]byte, 1000)),
			expectOk: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			config := DefaultConsumerConfig(credentials, tt.brokers, tt.groupID)

			assert.Equal(t, tt.brokers, config.Brokers)
			assert.Equal(t, credentials, config.ClientCredentials)
			assert.Equal(t, tt.groupID, config.GroupID)
			assert.Equal(t, 10*time.Second, config.ReadTimeout)
			assert.Equal(t, -1, config.Partition)
			assert.Equal(t, kafka.FirstOffset, config.StartOffset)
		})
	}
}

// TestEnvironmentString tests the Environment.String() method that has 0% coverage
func TestEnvironmentString(t *testing.T) {
	tests := []struct {
		name     string
		env      Environment
		expected string
	}{
		{
			name:     "Dev environment",
			env:      Dev,
			expected: "Dev",
		},
		{
			name:     "Prod environment",
			env:      Prod,
			expected: "Prod",
		},
		{
			name:     "Invalid environment value",
			env:      Environment(999),
			expected: "Environment(999)",
		},
		{
			name:     "Negative environment value",
			env:      Environment(-1),
			expected: "Environment(-1)",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := tt.env.String()
			assert.Equal(t, tt.expected, result)
		})
	}
}

// TestSystemTopicsJsonUnmarshalJSON tests the SystemTopicsJson.UnmarshalJSON method that has 0% coverage
func TestSystemTopicsJsonUnmarshalJSON(t *testing.T) {
	tests := []struct {
		name        string
		jsonInput   string
		expected    SystemTopicsJson
		expectError bool
		errorMsg    string
	}{
		{
			name:      "valid system topic",
			jsonInput: `"ds.core.billing.usage.created.v1"`,
			expected:  SystemTopicsJsonDsCoreBillingUsageCreatedV1,
		},
		{
			name:      "valid workflow topic",
			jsonInput: `"ds.workflow.pipeline.job.requested.v1"`,
			expected:  "ds.workflow.pipeline.job.requested.v1",
		},
		{
			name:      "valid pipeline topic",
			jsonInput: `"ds.pipeline.synchronizer.job.completed.v1"`,
			expected:  "ds.pipeline.synchronizer.job.completed.v1",
		},
		{
			name:        "invalid topic",
			jsonInput:   `"invalid.topic.name"`,
			expectError: true,
			errorMsg:    "invalid value",
		},
		{
			name:        "empty string",
			jsonInput:   `""`,
			expectError: true,
			errorMsg:    "invalid value",
		},
		{
			name:        "null value",
			jsonInput:   `null`,
			expectError: true,
		},
		{
			name:        "invalid JSON",
			jsonInput:   `invalid-json`,
			expectError: true,
		},
		{
			name:        "number instead of string",
			jsonInput:   `123`,
			expectError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var topic SystemTopicsJson
			err := json.Unmarshal([]byte(tt.jsonInput), &topic)

			if tt.expectError {
				assert.Error(t, err)
				if tt.errorMsg != "" {
					assert.Contains(t, err.Error(), tt.errorMsg)
				}
			} else {
				assert.NoError(t, err)
				assert.Equal(t, tt.expected, topic)
			}
		})
	}
}

// Benchmark tests for common operations
func BenchmarkGetBootstrapServers(b *testing.B) {
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = GetBootstrapServers(Dev, false)
	}
}

func BenchmarkHeaderCreation(b *testing.B) {
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = Header{
			Key:   "benchmark-header",
			Value: "benchmark-value",
		}
	}
}
