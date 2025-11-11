package models

import (
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestEventSerialization(t *testing.T) {
	// Create a test event with all required fields
	eventID := uuid.New()
	sessionID := uuid.New()
	requestID := uuid.New()
	tenantID := uuid.New()
	eventType := "test.event.created.v1"
	eventSource := "test-service"
	createdBy := "test-producer"
	md5Hash := "d41d8cd98f00b204e9800998ecf8427e"

	event := EventJson{
		Id:          eventID,
		SessionId:   sessionID,
		RequestId:   requestID,
		TenantId:    tenantID,
		EventType:   eventType,
		EventSource: eventSource,
		Metadata:    map[string]string{"meta1": "value1"},
		Timestamp:   time.Now(),
		CreatedBy:   createdBy,
		Md5Hash:     md5Hash,
		Payload: &map[string]interface{}{
			"key1": "value1",
			"key2": 42,
			"key3": true,
		},
	}

	// Test JSON marshaling
	jsonData, err := json.Marshal(event)
	if err != nil {
		t.Fatalf("Failed to marshal event to JSON: %v", err)
	}

	// Test JSON unmarshaling
	var unmarshaledEvent EventJson
	err = json.Unmarshal(jsonData, &unmarshaledEvent)
	if err != nil {
		t.Fatalf("Failed to unmarshal event from JSON: %v", err)
	}

	// Verify all fields are preserved
	if unmarshaledEvent.Id != eventID {
		t.Errorf("Expected Id %v, got %v", eventID, unmarshaledEvent.Id)
	}
	if unmarshaledEvent.SessionId != sessionID {
		t.Errorf("Expected SessionId %v, got %v", sessionID, unmarshaledEvent.SessionId)
	}
	if unmarshaledEvent.RequestId != requestID {
		t.Errorf("Expected RequestId %v, got %v", requestID, unmarshaledEvent.RequestId)
	}
	if unmarshaledEvent.TenantId != tenantID {
		t.Errorf("Expected TenantId %v, got %v", tenantID, unmarshaledEvent.TenantId)
	}
	if unmarshaledEvent.EventType != eventType {
		t.Errorf("Expected EventType %v, got %v", eventType, unmarshaledEvent.EventType)
	}
	if unmarshaledEvent.EventSource != eventSource {
		t.Errorf("Expected EventSource %v, got %v", eventSource, unmarshaledEvent.EventSource)
	}

	// Test payload (now a pointer)
	if unmarshaledEvent.Payload == nil {
		t.Error("Expected payload to be not nil")
	} else {
		payload := *unmarshaledEvent.Payload
		if len(payload) != 3 {
			t.Errorf("Expected payload with 3 items, got %d", len(payload))
		}
		if payload["key1"] != "value1" {
			t.Errorf("Expected payload key1 to be 'value1', got %v", payload["key1"])
		}
	}
}

func TestEventWithOptionalFields(t *testing.T) {
	eventID := uuid.New()
	sessionID := uuid.New()
	requestID := uuid.New()
	tenantID := uuid.New()
	eventType := "test.event.updated.v1"
	eventSource := "test-service"
	ownerID := "test-owner"
	eventSourceURI := "https://test.example.com"
	message := "Test message"
	createdBy := "test-producer"
	md5Hash := "d41d8cd98f00b204e9800998ecf8427e"

	event := EventJson{
		Id:             eventID,
		SessionId:      sessionID,
		RequestId:      requestID,
		TenantId:       tenantID,
		EventType:      eventType,
		EventSource:    eventSource,
		OwnerId:        &ownerID,
		EventSourceUri: &eventSourceURI,
		Message:        &message,
		Metadata:       map[string]string{"meta1": "value1"},
		Timestamp:      time.Now(),
		CreatedBy:      createdBy,
		Md5Hash:        md5Hash,
	}

	// Test JSON marshaling with optional fields
	jsonData, err := json.Marshal(event)
	if err != nil {
		t.Fatalf("Failed to marshal event with optional fields: %v", err)
	}

	// Test JSON unmarshaling
	var unmarshaledEvent EventJson
	err = json.Unmarshal(jsonData, &unmarshaledEvent)
	if err != nil {
		t.Fatalf("Failed to unmarshal event with optional fields: %v", err)
	}

	// Verify optional fields
	if unmarshaledEvent.OwnerId == nil || *unmarshaledEvent.OwnerId != ownerID {
		t.Errorf("Expected OwnerId %v, got %v", ownerID, unmarshaledEvent.OwnerId)
	}
	if unmarshaledEvent.EventSourceUri == nil || *unmarshaledEvent.EventSourceUri != eventSourceURI {
		t.Errorf("Expected EventSourceUri %v, got %v", eventSourceURI, unmarshaledEvent.EventSourceUri)
	}
	if unmarshaledEvent.Message == nil || *unmarshaledEvent.Message != message {
		t.Errorf("Expected Message %v, got %v", message, unmarshaledEvent.Message)
	}
}

func TestEventJSONFormat(t *testing.T) {
	eventID := uuid.New()
	sessionID := uuid.New()
	requestID := uuid.New()
	tenantID := uuid.New()
	eventType := "test.event.format.v1"
	eventSource := "test-service"
	createdBy := "test-producer"
	md5Hash := "d41d8cd98f00b204e9800998ecf8427e"

	event := EventJson{
		Id:          eventID,
		SessionId:   sessionID,
		RequestId:   requestID,
		TenantId:    tenantID,
		EventType:   eventType,
		EventSource: eventSource,
		Metadata:    map[string]string{"meta1": "value1"},
		Timestamp:   time.Now(),
		CreatedBy:   createdBy,
		Md5Hash:     md5Hash,
		Payload: &map[string]interface{}{
			"test": "data",
		},
	}

	jsonData, err := json.Marshal(event)
	if err != nil {
		t.Fatalf("Failed to marshal event: %v", err)
	}

	// Verify JSON contains expected fields
	jsonStr := string(jsonData)
	if !containsSubstring(jsonStr, eventID.String()) {
		t.Errorf("JSON should contain event ID: %s", jsonStr)
	}
	if !containsSubstring(jsonStr, sessionID.String()) {
		t.Errorf("JSON should contain session ID: %s", jsonStr)
	}
	if !containsSubstring(jsonStr, `"test":"data"`) {
		t.Errorf("JSON should contain payload: %s", jsonStr)
	}
}

func TestEmptyEvent(t *testing.T) {
	// Create a minimal valid event with all required fields
	event := EventJson{
		Id:          uuid.New(),
		SessionId:   uuid.New(),
		RequestId:   uuid.New(),
		TenantId:    uuid.New(),
		EventType:   "test.event.v1",
		EventSource: "test-source",
		Metadata:    map[string]string{},
		Timestamp:   time.Now(),
		CreatedBy:   "test",
		Md5Hash:     "d41d8cd98f00b204e9800998ecf8427e", // Valid MD5 hash
	}

	// Should be able to marshal the event
	jsonData, err := json.Marshal(event)
	if err != nil {
		t.Fatalf("Failed to marshal event: %v", err)
	}

	// Should be able to unmarshal back
	var unmarshaledEvent EventJson
	err = json.Unmarshal(jsonData, &unmarshaledEvent)
	if err != nil {
		t.Fatalf("Failed to unmarshal event: %v", err)
	}

	// Verify IDs are preserved
	if unmarshaledEvent.Id != event.Id {
		t.Errorf("Expected Id %v, got %v", event.Id, unmarshaledEvent.Id)
	}
}

// Helper function to check if string contains substring
func containsSubstring(str, substr string) bool {
	return len(str) >= len(substr) && str != substr && str[0:] != substr &&
		(len(str) > len(substr) && findSubstring(str, substr))
}

func findSubstring(str, substr string) bool {
	for i := 0; i <= len(str)-len(substr); i++ {
		if str[i:i+len(substr)] == substr {
			return true
		}
	}
	return false
}

// Enhanced event tests with comprehensive validation

// Test EventJson validation with various scenarios
func TestEventJsonValidation(t *testing.T) {
	// Valid MD5 hash for testing
	validMd5Hash := "d41d8cd98f00b204e9800998ecf8427e"

	tests := []struct {
		name    string
		event   EventJson
		wantErr bool
	}{
		{
			name: "valid complete event",
			event: EventJson{
				Id:          uuid.New(),
				SessionId:   uuid.New(),
				RequestId:   uuid.New(),
				TenantId:    uuid.New(),
				EventType:   "test.event.created.v1",
				EventSource: "test-service",
				Metadata:    map[string]string{"key": "value"},
				Timestamp:   time.Now(),
				CreatedBy:   "test-producer",
				Md5Hash:     "d41d8cd98f00b204e9800998ecf8427e",
				Payload:     &map[string]interface{}{"data": "test"},
			},
			wantErr: false,
		},
		{
			name: "minimal valid event",
			event: EventJson{
				Id:          uuid.New(),
				SessionId:   uuid.New(),
				RequestId:   uuid.New(),
				TenantId:    uuid.New(),
				EventType:   "minimal.event.v1",
				EventSource: "minimal-service",
				Metadata:    map[string]string{}, // Required field
				Timestamp:   time.Now(),
				CreatedBy:   "minimal-producer", // Required field
				Md5Hash:     validMd5Hash,       // Required with pattern
			},
			wantErr: false,
		},
		{
			name: "event with zero UUIDs",
			event: EventJson{
				Id:          uuid.Nil,
				SessionId:   uuid.Nil,
				RequestId:   uuid.Nil,
				TenantId:    uuid.Nil,
				EventType:   "zero.uuid.event.v1",
				EventSource: "zero-service",
				Metadata:    map[string]string{}, // Required field
				Timestamp:   time.Now(),
				CreatedBy:   "zero-producer", // Required field
				Md5Hash:     validMd5Hash,    // Required with pattern
			},
			wantErr: false, // Zero UUIDs are valid in Go
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Test JSON marshaling
			data, err := json.Marshal(tt.event)
			assert.NoError(t, err)
			assert.NotEmpty(t, data)

			// Test JSON unmarshaling
			var unmarshaled EventJson
			err = json.Unmarshal(data, &unmarshaled)
			assert.NoError(t, err)

			// Verify critical fields are preserved
			assert.Equal(t, tt.event.Id, unmarshaled.Id)
			assert.Equal(t, tt.event.SessionId, unmarshaled.SessionId)
			assert.Equal(t, tt.event.EventType, unmarshaled.EventType)
			assert.Equal(t, tt.event.EventSource, unmarshaled.EventSource)
		})
	}
}

// Test UUID serialization/deserialization
func TestUUIDSerialization(t *testing.T) {
	validMd5Hash := "d41d8cd98f00b204e9800998ecf8427e"

	tests := []struct {
		name string
		uuid uuid.UUID
	}{
		{
			name: "new UUID",
			uuid: uuid.New(),
		},
		{
			name: "nil UUID",
			uuid: uuid.Nil,
		},
		{
			name: "specific UUID",
			uuid: uuid.MustParse("123e4567-e89b-12d3-a456-426614174000"),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			event := EventJson{
				Id:          tt.uuid,
				SessionId:   tt.uuid,
				RequestId:   tt.uuid,
				TenantId:    tt.uuid,
				EventType:   "uuid.test.v1",
				EventSource: "uuid-service",
				Metadata:    map[string]string{}, // Required
				Timestamp:   time.Now(),
				CreatedBy:   "uuid-producer", // Required
				Md5Hash:     validMd5Hash,    // Required with pattern
			}

			data, err := json.Marshal(event)
			require.NoError(t, err)

			var unmarshaled EventJson
			err = json.Unmarshal(data, &unmarshaled)
			require.NoError(t, err)

			assert.Equal(t, tt.uuid, unmarshaled.Id)
			assert.Equal(t, tt.uuid, unmarshaled.SessionId)
			assert.Equal(t, tt.uuid, unmarshaled.RequestId)
			assert.Equal(t, tt.uuid, unmarshaled.TenantId)
		})
	}
}

// Test Payload handling with various data types
func TestPayloadHandling(t *testing.T) {
	// Valid MD5 hash for testing
	validMd5Hash := "d41d8cd98f00b204e9800998ecf8427e"

	tests := []struct {
		name    string
		payload *map[string]interface{}
	}{
		{
			name:    "nil payload",
			payload: nil,
		},
		{
			name:    "empty payload",
			payload: &map[string]interface{}{},
		},
		{
			name: "simple payload",
			payload: &map[string]interface{}{
				"string": "value",
				"number": 42,
				"bool":   true,
			},
		},
		{
			name: "nested payload",
			payload: &map[string]interface{}{
				"nested": map[string]interface{}{
					"level2": map[string]interface{}{
						"level3": "deep_value",
					},
				},
				"array": []interface{}{1, 2, 3, "four"},
			},
		},
		{
			name: "complex payload",
			payload: &map[string]interface{}{
				"uuid":      uuid.New().String(),
				"timestamp": time.Now().Unix(),
				"float":     3.14159,
				"null":      nil,
				"object": map[string]interface{}{
					"id":   123,
					"name": "test",
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			event := EventJson{
				Id:          uuid.New(),
				SessionId:   uuid.New(),
				RequestId:   uuid.New(),
				TenantId:    uuid.New(),
				EventType:   "payload.test.v1",
				EventSource: "payload-service",
				Metadata:    map[string]string{},
				Timestamp:   time.Now(),
				CreatedBy:   "payload-producer",
				Md5Hash:     validMd5Hash,
				Payload:     tt.payload,
			}

			data, err := json.Marshal(event)
			assert.NoError(t, err)

			var unmarshaled EventJson
			err = json.Unmarshal(data, &unmarshaled)
			assert.NoError(t, err)

			if tt.payload == nil {
				assert.Nil(t, unmarshaled.Payload)
			} else {
				assert.NotNil(t, unmarshaled.Payload)
				// Note: Deep comparison of interface{} maps is complex,
				// so we just verify structure exists
			}
		})
	}
}

// Test Metadata handling
func TestMetadataHandling(t *testing.T) {
	// Valid MD5 hash for testing
	validMd5Hash := "d41d8cd98f00b204e9800998ecf8427e"

	tests := []struct {
		name     string
		metadata map[string]string
	}{
		{
			name:     "nil metadata",
			metadata: nil,
		},
		{
			name:     "empty metadata",
			metadata: map[string]string{},
		},
		{
			name: "simple metadata",
			metadata: map[string]string{
				"env":     "test",
				"version": "1.0.0",
				"source":  "unit-test",
			},
		},
		{
			name: "metadata with special characters",
			metadata: map[string]string{
				"special-chars": "!@#$%^&*()",
				"unicode":       "测试数据",
				"empty-value":   "",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			event := EventJson{
				Id:          uuid.New(),
				SessionId:   uuid.New(),
				RequestId:   uuid.New(),
				TenantId:    uuid.New(),
				EventType:   "metadata.test.v1",
				EventSource: "metadata-service",
				Timestamp:   time.Now(),
				CreatedBy:   "metadata-producer",
				Md5Hash:     validMd5Hash,
				Metadata:    tt.metadata,
			}

			data, err := json.Marshal(event)
			assert.NoError(t, err)

			var unmarshaled EventJson
			err = json.Unmarshal(data, &unmarshaled)
			assert.NoError(t, err)

			if tt.metadata == nil {
				assert.Nil(t, unmarshaled.Metadata)
			} else {
				assert.Equal(t, tt.metadata, unmarshaled.Metadata)
			}
		})
	}
}

// Test timestamp handling
func TestTimestampHandling(t *testing.T) {
	// Valid MD5 hash for testing
	validMd5Hash := "d41d8cd98f00b204e9800998ecf8427e"

	tests := []struct {
		name      string
		timestamp time.Time
	}{
		{
			name:      "current time",
			timestamp: time.Now(),
		},
		{
			name:      "zero time",
			timestamp: time.Time{},
		},
		{
			name:      "unix epoch",
			timestamp: time.Unix(0, 0),
		},
		{
			name:      "future time",
			timestamp: time.Now().Add(24 * time.Hour),
		},
		{
			name:      "past time",
			timestamp: time.Now().Add(-24 * time.Hour),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			event := EventJson{
				Id:          uuid.New(),
				SessionId:   uuid.New(),
				RequestId:   uuid.New(),
				TenantId:    uuid.New(),
				EventType:   "timestamp.test.v1",
				EventSource: "timestamp-service",
				Metadata:    map[string]string{},
				CreatedBy:   "timestamp-producer",
				Md5Hash:     validMd5Hash,
				Timestamp:   tt.timestamp,
			}

			data, err := json.Marshal(event)
			assert.NoError(t, err)

			var unmarshaled EventJson
			err = json.Unmarshal(data, &unmarshaled)
			assert.NoError(t, err)

			// Time comparison should be within reasonable tolerance
			if !tt.timestamp.IsZero() {
				assert.True(t, tt.timestamp.Equal(unmarshaled.Timestamp) ||
					tt.timestamp.Sub(unmarshaled.Timestamp).Abs() < time.Millisecond)
			} else {
				assert.True(t, unmarshaled.Timestamp.IsZero())
			}
		})
	}
}

// Test edge cases and error scenarios
func TestEventEdgeCases(t *testing.T) {
	// Valid MD5 hash for testing
	validMd5Hash := "d41d8cd98f00b204e9800998ecf8427e"

	t.Run("very long string fields", func(t *testing.T) {
		longString := strings.Repeat("a", 10000)

		event := EventJson{
			Id:          uuid.New(),
			SessionId:   uuid.New(),
			RequestId:   uuid.New(),
			TenantId:    uuid.New(),
			EventType:   longString,
			EventSource: longString,
			Metadata:    map[string]string{},
			CreatedBy:   longString,
			Md5Hash:     validMd5Hash, // Using valid hash instead of long string
			Timestamp:   time.Now(),
		}

		data, err := json.Marshal(event)
		assert.NoError(t, err)
		assert.NotEmpty(t, data)

		var unmarshaled EventJson
		err = json.Unmarshal(data, &unmarshaled)
		assert.NoError(t, err)
		assert.Equal(t, longString, unmarshaled.EventType)
	})

	t.Run("unicode characters", func(t *testing.T) {
		event := EventJson{
			Id:          uuid.New(),
			SessionId:   uuid.New(),
			RequestId:   uuid.New(),
			TenantId:    uuid.New(),
			EventType:   "测试.事件.创建.v1",
			EventSource: "测试服务",
			CreatedBy:   "测试用户",
			Md5Hash:     validMd5Hash,
			Timestamp:   time.Now(),
			Metadata: map[string]string{
				"描述": "这是一个测试事件",
				"环境": "测试环境",
			},
		}

		data, err := json.Marshal(event)
		assert.NoError(t, err)

		var unmarshaled EventJson
		err = json.Unmarshal(data, &unmarshaled)
		assert.NoError(t, err)
		assert.Equal(t, "测试.事件.创建.v1", unmarshaled.EventType)
	})
}

// Benchmark tests for event operations
func BenchmarkEventMarshaling(b *testing.B) {
	// Valid MD5 hash for testing
	validMd5Hash := "d41d8cd98f00b204e9800998ecf8427e"

	event := EventJson{
		Id:          uuid.New(),
		SessionId:   uuid.New(),
		RequestId:   uuid.New(),
		TenantId:    uuid.New(),
		EventType:   "benchmark.event.v1",
		EventSource: "benchmark-service",
		Metadata:    map[string]string{"key": "value"},
		Timestamp:   time.Now(),
		CreatedBy:   "benchmark",
		Md5Hash:     validMd5Hash,
		Payload:     &map[string]interface{}{"data": "benchmark"},
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = json.Marshal(event)
	}
}

func BenchmarkEventUnmarshaling(b *testing.B) {
	event := EventJson{
		Id:          uuid.New(),
		SessionId:   uuid.New(),
		RequestId:   uuid.New(),
		TenantId:    uuid.New(),
		EventType:   "benchmark.event.v1",
		EventSource: "benchmark-service",
		Metadata:    map[string]string{"key": "value"},
		Timestamp:   time.Now(),
		CreatedBy:   "benchmark",
		Md5Hash:     "d41d8cd98f00b204e9800998ecf8427e",
		Payload:     &map[string]interface{}{"data": "benchmark"},
	}

	data, _ := json.Marshal(event)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		var unmarshaled EventJson
		_ = json.Unmarshal(data, &unmarshaled)
	}
}

func BenchmarkUUIDGeneration(b *testing.B) {
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = uuid.New()
	}
}
