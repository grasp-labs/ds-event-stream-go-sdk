package models

import (
	"encoding/json"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestEventBuilder_ValidEvent(t *testing.T) {
	event, err := NewEventBuilder(
		"test.event",
		"test-service",
		"test-user",
		uuid.New(),
		uuid.New(),
		uuid.New(),
		map[string]string{"key": "value"},
		"d41d8cd98f00b204e9800998ecf8427e",
	).Build()

	require.NoError(t, err)
	assert.NotNil(t, event)
}

func TestEventBuilder_EmptyEventType(t *testing.T) {
	event, err := NewEventBuilder(
		"",
		"test-service",
		"test-user",
		uuid.New(),
		uuid.New(),
		uuid.New(),
		map[string]string{"key": "value"},
		"d41d8cd98f00b204e9800998ecf8427e",
	).Build()

	assert.Error(t, err)
	assert.Nil(t, event)
	assert.Contains(t, err.Error(), "event_type cannot be empty")
}

func TestEventBuilder_EmptyEventSource(t *testing.T) {
	event, err := NewEventBuilder(
		"test.event",
		"",
		"test-user",
		uuid.New(),
		uuid.New(),
		uuid.New(),
		map[string]string{"key": "value"},
		"d41d8cd98f00b204e9800998ecf8427e",
	).Build()

	assert.Error(t, err)
	assert.Nil(t, event)
	assert.Contains(t, err.Error(), "event_source cannot be empty")
}

func TestEventBuilder_EmptyCreatedBy(t *testing.T) {
	event, err := NewEventBuilder(
		"test.event",
		"test-service",
		"",
		uuid.New(),
		uuid.New(),
		uuid.New(),
		map[string]string{"key": "value"},
		"d41d8cd98f00b204e9800998ecf8427e",
	).Build()

	assert.Error(t, err)
	assert.Nil(t, event)
	assert.Contains(t, err.Error(), "created_by cannot be empty")
}

func TestEventBuilder_NilMetadata(t *testing.T) {
	event, err := NewEventBuilder(
		"test.event",
		"test-service",
		"test-user",
		uuid.New(),
		uuid.New(),
		uuid.New(),
		nil,
		"d41d8cd98f00b204e9800998ecf8427e",
	).Build()

	assert.Error(t, err)
	assert.Nil(t, event)
	assert.Contains(t, err.Error(), "metadata cannot be nil")
}

func TestEventBuilder_ZeroValueTenantId(t *testing.T) {
	event, err := NewEventBuilder(
		"test.event",
		"test-service",
		"test-user",
		uuid.Nil, // zero-value UUID
		uuid.New(),
		uuid.New(),
		map[string]string{"key": "value"},
		"d41d8cd98f00b204e9800998ecf8427e",
	).Build()

	assert.Error(t, err)
	assert.Nil(t, event)
	assert.Contains(t, err.Error(), "tenant_id cannot be zero-value UUID")
}

func TestEventBuilder_ZeroValueSessionId(t *testing.T) {
	event, err := NewEventBuilder(
		"test.event",
		"test-service",
		"test-user",
		uuid.New(),
		uuid.Nil, // zero-value UUID
		uuid.New(),
		map[string]string{"key": "value"},
		"d41d8cd98f00b204e9800998ecf8427e",
	).Build()

	assert.Error(t, err)
	assert.Nil(t, event)
	assert.Contains(t, err.Error(), "session_id cannot be zero-value UUID")
}

func TestEventBuilder_ZeroValueRequestId(t *testing.T) {
	event, err := NewEventBuilder(
		"test.event",
		"test-service",
		"test-user",
		uuid.New(),
		uuid.New(),
		uuid.Nil, // zero-value UUID
		map[string]string{"key": "value"},
		"d41d8cd98f00b204e9800998ecf8427e",
	).Build()

	assert.Error(t, err)
	assert.Nil(t, event)
	assert.Contains(t, err.Error(), "request_id cannot be zero-value UUID")
}

func TestEventBuilder_EmptyMd5Hash(t *testing.T) {
	event, err := NewEventBuilder(
		"test.event",
		"test-service",
		"test-user",
		uuid.New(),
		uuid.New(),
		uuid.New(),
		map[string]string{"key": "value"},
		"",
	).WithPayload(map[string]interface{}{"data": "test"}).Build()

	assert.Error(t, err)
	assert.Nil(t, event)
	assert.Contains(t, err.Error(), "md5_hash must be a valid 32-character hex string")
}

func TestEventBuilder_EmptyMd5HashWithoutPayload(t *testing.T) {
	// MD5 hash should be optional when payload is nil
	event, err := NewEventBuilder(
		"test.event",
		"test-service",
		"test-user",
		uuid.New(),
		uuid.New(),
		uuid.New(),
		map[string]string{"key": "value"},
		"",
	).Build()

	assert.NoError(t, err)
	assert.NotNil(t, event)
}

func TestEventBuilder_InvalidMd5HashFormat(t *testing.T) {
	tests := []struct {
		name    string
		md5Hash string
	}{
		{"too short", "d41d8cd98f00b204"},
		{"too long", "d41d8cd98f00b204e9800998ecf8427e123"},
		{"invalid characters", "g41d8cd98f00b204e9800998ecf8427e"},
		{"spaces", "d41d8cd9 8f00b204e9800998ecf8427e"},
		{"special chars", "d41d8cd9-8f00-b204-e980-0998ecf8427e"},
		{"lowercase and uppercase mixed but wrong length", "D41D8CD"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			event, err := NewEventBuilder(
				"test.event",
				"test-service",
				"test-user",
				uuid.New(),
				uuid.New(),
				uuid.New(),
				map[string]string{"key": "value"},
				tt.md5Hash,
			).WithPayload(map[string]interface{}{"data": "test"}).Build()

			assert.Error(t, err)
			assert.Nil(t, event)
			assert.Contains(t, err.Error(), "md5_hash must be a valid 32-character hex string")
		})
	}
}

func TestEventBuilder_ValidMd5HashFormats(t *testing.T) {
	tests := []struct {
		name    string
		md5Hash string
	}{
		{"lowercase", "d41d8cd98f00b204e9800998ecf8427e"},
		{"uppercase", "D41D8CD98F00B204E9800998ECF8427E"},
		{"mixed case", "D41d8Cd98F00b204E9800998eCf8427e"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			event, err := NewEventBuilder(
				"test.event",
				"test-service",
				"test-user",
				uuid.New(),
				uuid.New(),
				uuid.New(),
				map[string]string{"key": "value"},
				tt.md5Hash,
			).Build()

			assert.NoError(t, err)
			assert.NotNil(t, event)
		})
	}
}

func TestEventBuilder_ValidMd5HashWithPayload(t *testing.T) {
	// Test that a valid MD5 hash with a payload passes validation
	event, err := NewEventBuilder(
		"test.event",
		"test-service",
		"test-user",
		uuid.New(),
		uuid.New(),
		uuid.New(),
		map[string]string{"key": "value"},
		"5d41402abc4b2a76b9719d911017c592", // MD5 hash of "hello"
	).WithPayload(map[string]interface{}{"data": "test"}).Build()

	require.NoError(t, err)
	assert.NotNil(t, event)

	// Verify the event can be marshaled to JSON
	jsonBytes, err := event.AsJSON()
	require.NoError(t, err)
	assert.NotEmpty(t, jsonBytes)
}

func TestEventBuilder_WithMethods(t *testing.T) {
	msg := "Test message"
	ownerId := "owner-123"

	event, err := NewEventBuilder(
		"test.event",
		"test-service",
		"test-user",
		uuid.New(),
		uuid.New(),
		uuid.New(),
		map[string]string{"key": "value"},
		"d41d8cd98f00b204e9800998ecf8427e",
	).WithMessage(msg).
		WithOwnerId(ownerId).
		WithPayload(map[string]interface{}{"data": "test"}).
		WithTags(map[string]string{"env": "test"}).
		Build()

	require.NoError(t, err)
	assert.NotNil(t, event)
}

func TestEvent_AsJSON_Marshaling(t *testing.T) {
	event, err := NewEventBuilder(
		"test.event",
		"test-service",
		"test-user",
		uuid.New(),
		uuid.New(),
		uuid.New(),
		map[string]string{"key": "value"},
		"d41d8cd98f00b204e9800998ecf8427e",
	).Build()

	require.NoError(t, err)

	// Get the JSON bytes
	jsonBytes, err := event.AsJSON()
	require.NoError(t, err)
	assert.NotEmpty(t, jsonBytes)

	// Unmarshal back
	var unmarshaled EventJson
	err = json.Unmarshal(jsonBytes, &unmarshaled)
	require.NoError(t, err)
	assert.Equal(t, "test.event", unmarshaled.EventType)
	assert.Equal(t, "test-service", unmarshaled.EventSource)
}

func TestEvent_AsJSON_RejectsNilEvent(t *testing.T) {
	var nilEvent *SealedEvent

	jsonBytes, err := nilEvent.AsJSON()
	assert.Error(t, err)
	assert.Nil(t, jsonBytes)
	assert.Contains(t, err.Error(), "event pointer is nil")
}

func TestSealedEvent_Getters(t *testing.T) {
	// Create test data
	tenantId := uuid.New()
	sessionId := uuid.New()
	requestId := uuid.New()
	eventType := "test.event.created"
	eventSource := "test-service"
	createdBy := "test-user"
	md5Hash := "d41d8cd98f00b204e9800998ecf8427e"
	payload := map[string]interface{}{"key": "value"}

	event, err := NewEventBuilder(
		eventType,
		eventSource,
		createdBy,
		tenantId,
		sessionId,
		requestId,
		map[string]string{"meta": "data"},
		md5Hash,
	).WithPayload(payload).Build()

	require.NoError(t, err)
	require.NotNil(t, event)

	// Test all getters
	assert.NotEqual(t, uuid.Nil, event.Id())
	assert.Equal(t, sessionId, event.SessionId())
	assert.Equal(t, eventType, event.EventType())
	assert.Equal(t, eventSource, event.EventSource())
	assert.Equal(t, requestId, event.RequestId())
	assert.Equal(t, tenantId, event.TenantId())
	assert.Equal(t, createdBy, event.CreatedBy())
	assert.Equal(t, md5Hash, event.Md5Hash())
	assert.Equal(t, payload, event.Payload())
	assert.False(t, event.Timestamp().IsZero())
}

func TestSealedEvent_Md5Hash_NilCase(t *testing.T) {
	// Test getter when md5Hash is nil (no payload)
	event, err := NewEventBuilder(
		"test.event",
		"test-service",
		"test-user",
		uuid.New(),
		uuid.New(),
		uuid.New(),
		map[string]string{"key": "value"},
		"", // empty md5Hash, no payload
	).Build()

	require.NoError(t, err)
	require.NotNil(t, event)

	// Should return empty string when md5Hash is nil
	assert.Equal(t, "", event.Md5Hash())
}

func TestEventBuilder_WithAffectedEntityUri(t *testing.T) {
	uri := "https://example.com/entity/123"
	event, err := NewEventBuilder(
		"test.event",
		"test-service",
		"test-user",
		uuid.New(),
		uuid.New(),
		uuid.New(),
		map[string]string{"key": "value"},
		"d41d8cd98f00b204e9800998ecf8427e",
	).WithAffectedEntityUri(uri).
		WithPayload(map[string]interface{}{"data": "test"}).
		Build()

	require.NoError(t, err)
	assert.NotNil(t, event)

	// Verify the URI is set by marshaling and inspecting JSON
	jsonBytes, err := event.AsJSON()
	require.NoError(t, err)

	var unmarshaled EventJson
	err = json.Unmarshal(jsonBytes, &unmarshaled)
	require.NoError(t, err)
	require.NotNil(t, unmarshaled.AffectedEntityUri)
	assert.Equal(t, uri, *unmarshaled.AffectedEntityUri)
}

func TestEventBuilder_WithEventSourceUri(t *testing.T) {
	uri := "https://example.com/source"
	event, err := NewEventBuilder(
		"test.event",
		"test-service",
		"test-user",
		uuid.New(),
		uuid.New(),
		uuid.New(),
		map[string]string{"key": "value"},
		"d41d8cd98f00b204e9800998ecf8427e",
	).WithEventSourceUri(uri).
		WithPayload(map[string]interface{}{"data": "test"}).
		Build()

	require.NoError(t, err)
	assert.NotNil(t, event)

	jsonBytes, err := event.AsJSON()
	require.NoError(t, err)

	var unmarshaled EventJson
	err = json.Unmarshal(jsonBytes, &unmarshaled)
	require.NoError(t, err)
	require.NotNil(t, unmarshaled.EventSourceUri)
	assert.Equal(t, uri, *unmarshaled.EventSourceUri)
}

func TestEventBuilder_WithPayloadUri(t *testing.T) {
	uri := "https://example.com/payload/456"
	event, err := NewEventBuilder(
		"test.event",
		"test-service",
		"test-user",
		uuid.New(),
		uuid.New(),
		uuid.New(),
		map[string]string{"key": "value"},
		"d41d8cd98f00b204e9800998ecf8427e",
	).WithPayloadUri(uri).
		WithPayload(map[string]interface{}{"data": "test"}).
		Build()

	require.NoError(t, err)
	assert.NotNil(t, event)

	jsonBytes, err := event.AsJSON()
	require.NoError(t, err)

	var unmarshaled EventJson
	err = json.Unmarshal(jsonBytes, &unmarshaled)
	require.NoError(t, err)
	require.NotNil(t, unmarshaled.PayloadUri)
	assert.Equal(t, uri, *unmarshaled.PayloadUri)
}

func TestEventBuilder_WithContextUri(t *testing.T) {
	uri := "https://example.com/context/789"
	event, err := NewEventBuilder(
		"test.event",
		"test-service",
		"test-user",
		uuid.New(),
		uuid.New(),
		uuid.New(),
		map[string]string{"key": "value"},
		"d41d8cd98f00b204e9800998ecf8427e",
	).WithContextUri(uri).
		WithPayload(map[string]interface{}{"data": "test"}).
		Build()

	require.NoError(t, err)
	assert.NotNil(t, event)

	jsonBytes, err := event.AsJSON()
	require.NoError(t, err)

	var unmarshaled EventJson
	err = json.Unmarshal(jsonBytes, &unmarshaled)
	require.NoError(t, err)
	require.NotNil(t, unmarshaled.ContextUri)
	assert.Equal(t, uri, *unmarshaled.ContextUri)
}

func TestEventBuilder_WithContext(t *testing.T) {
	ctx := map[string]interface{}{
		"traceId": "abc123",
		"spanId":  "def456",
	}
	event, err := NewEventBuilder(
		"test.event",
		"test-service",
		"test-user",
		uuid.New(),
		uuid.New(),
		uuid.New(),
		map[string]string{"key": "value"},
		"d41d8cd98f00b204e9800998ecf8427e",
	).WithContext(ctx).
		WithPayload(map[string]interface{}{"data": "test"}).
		Build()

	require.NoError(t, err)
	assert.NotNil(t, event)

	jsonBytes, err := event.AsJSON()
	require.NoError(t, err)

	var unmarshaled EventJson
	err = json.Unmarshal(jsonBytes, &unmarshaled)
	require.NoError(t, err)
	require.NotNil(t, unmarshaled.Context)

	contextMap, ok := unmarshaled.Context.(map[string]interface{})
	require.True(t, ok)
	assert.Equal(t, "abc123", contextMap["traceId"])
	assert.Equal(t, "def456", contextMap["spanId"])
}

func TestEventBuilder_AllOptionalFields(t *testing.T) {
	// Test building an event with all optional fields set
	event, err := NewEventBuilder(
		"test.event",
		"test-service",
		"test-user",
		uuid.New(),
		uuid.New(),
		uuid.New(),
		map[string]string{"meta": "data"},
		"d41d8cd98f00b204e9800998ecf8427e",
	).WithMessage("Test message").
		WithOwnerId("owner-123").
		WithPayload(map[string]interface{}{"key": "value"}).
		WithAffectedEntityUri("https://example.com/entity/1").
		WithEventSourceUri("https://example.com/source").
		WithPayloadUri("https://example.com/payload/1").
		WithContextUri("https://example.com/context/1").
		WithContext(map[string]interface{}{"trace": "xyz"}).
		WithTags(map[string]string{"env": "prod", "region": "us-west"}).
		Build()

	require.NoError(t, err)
	assert.NotNil(t, event)

	// Verify all fields by unmarshaling
	jsonBytes, err := event.AsJSON()
	require.NoError(t, err)

	var unmarshaled EventJson
	err = json.Unmarshal(jsonBytes, &unmarshaled)
	require.NoError(t, err)

	assert.NotNil(t, unmarshaled.Message)
	assert.NotNil(t, unmarshaled.OwnerId)
	assert.NotNil(t, unmarshaled.Payload)
	assert.NotNil(t, unmarshaled.AffectedEntityUri)
	assert.NotNil(t, unmarshaled.EventSourceUri)
	assert.NotNil(t, unmarshaled.PayloadUri)
	assert.NotNil(t, unmarshaled.ContextUri)
	assert.NotNil(t, unmarshaled.Context)
	assert.NotNil(t, unmarshaled.Tags)

	assert.Equal(t, "Test message", *unmarshaled.Message)
	assert.Equal(t, "owner-123", *unmarshaled.OwnerId)
	assert.Equal(t, 2, len(*unmarshaled.Tags))
}
