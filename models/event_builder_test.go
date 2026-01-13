package models

import (
	"encoding/json"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewEvent_ValidEvent(t *testing.T) {
	event, err := NewEvent(
		"test.event",
		"test-service",
		"test-user",
		uuid.New(),
		uuid.New(),
		uuid.New(),
		map[string]string{"key": "value"},
		"d41d8cd98f00b204e9800998ecf8427e",
	)

	require.NoError(t, err)
	assert.NotNil(t, event)
	assert.Equal(t, "test.event", event.EventType())
	assert.Equal(t, "test-service", event.EventSource())
	assert.Equal(t, "test-user", event.CreatedBy())
	assert.NotEqual(t, uuid.Nil, event.Id())
}

func TestNewEvent_EmptyEventType(t *testing.T) {
	event, err := NewEvent(
		"",
		"test-service",
		"test-user",
		uuid.New(),
		uuid.New(),
		uuid.New(),
		map[string]string{"key": "value"},
		"d41d8cd98f00b204e9800998ecf8427e",
	)

	assert.Error(t, err)
	assert.Nil(t, event)
	assert.Contains(t, err.Error(), "event_type cannot be empty")
}

func TestNewEvent_EmptyEventSource(t *testing.T) {
	event, err := NewEvent(
		"test.event",
		"",
		"test-user",
		uuid.New(),
		uuid.New(),
		uuid.New(),
		map[string]string{"key": "value"},
		"d41d8cd98f00b204e9800998ecf8427e",
	)

	assert.Error(t, err)
	assert.Nil(t, event)
	assert.Contains(t, err.Error(), "event_source cannot be empty")
}

func TestNewEvent_EmptyCreatedBy(t *testing.T) {
	event, err := NewEvent(
		"test.event",
		"test-service",
		"",
		uuid.New(),
		uuid.New(),
		uuid.New(),
		map[string]string{"key": "value"},
		"d41d8cd98f00b204e9800998ecf8427e",
	)

	assert.Error(t, err)
	assert.Nil(t, event)
	assert.Contains(t, err.Error(), "created_by cannot be empty")
}

func TestNewEvent_NilMetadata(t *testing.T) {
	event, err := NewEvent(
		"test.event",
		"test-service",
		"test-user",
		uuid.New(),
		uuid.New(),
		uuid.New(),
		nil,
		"d41d8cd98f00b204e9800998ecf8427e",
	)

	assert.Error(t, err)
	assert.Nil(t, event)
	assert.Contains(t, err.Error(), "metadata cannot be nil")
}

func TestNewEvent_EmptyMd5Hash(t *testing.T) {
	event, err := NewEvent(
		"test.event",
		"test-service",
		"test-user",
		uuid.New(),
		uuid.New(),
		uuid.New(),
		map[string]string{"key": "value"},
		"",
	)

	assert.Error(t, err)
	assert.Nil(t, event)
	assert.Contains(t, err.Error(), "md5_hash cannot be empty")
}

func TestNewEvent_InvalidMd5HashFormat(t *testing.T) {
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
			event, err := NewEvent(
				"test.event",
				"test-service",
				"test-user",
				uuid.New(),
				uuid.New(),
				uuid.New(),
				map[string]string{"key": "value"},
				tt.md5Hash,
			)

			assert.Error(t, err)
			assert.Nil(t, event)
			assert.Contains(t, err.Error(), "md5_hash must be a valid 32-character hex string")
		})
	}
}

func TestNewEvent_ValidMd5HashFormats(t *testing.T) {
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
			event, err := NewEvent(
				"test.event",
				"test-service",
				"test-user",
				uuid.New(),
				uuid.New(),
				uuid.New(),
				map[string]string{"key": "value"},
				tt.md5Hash,
			)

			assert.NoError(t, err)
			assert.NotNil(t, event)
			assert.Equal(t, tt.md5Hash, event.Md5Hash())
		})
	}
}

func TestEvent_SetOptionalFields(t *testing.T) {
	event, err := NewEvent(
		"test.event",
		"test-service",
		"test-user",
		uuid.New(),
		uuid.New(),
		uuid.New(),
		map[string]string{"key": "value"},
		"d41d8cd98f00b204e9800998ecf8427e",
	)

	require.NoError(t, err)

	// Test setting optional fields
	event.SetMessage("Test message")
	event.SetOwnerId("owner-123")
	event.SetPayload(map[string]interface{}{"data": "test"})
	event.SetTags(map[string]string{"env": "test"})

	assert.NotNil(t, event.Message())
	assert.Equal(t, "Test message", *event.Message())
	assert.NotNil(t, event.OwnerId())
	assert.Equal(t, "owner-123", *event.OwnerId())
	assert.NotNil(t, event.Payload())
	assert.NotNil(t, event.Tags())
}

func TestEvent_AsJSON_Marshaling(t *testing.T) {
	event, err := NewEvent(
		"test.event",
		"test-service",
		"test-user",
		uuid.New(),
		uuid.New(),
		uuid.New(),
		map[string]string{"key": "value"},
		"d41d8cd98f00b204e9800998ecf8427e",
	)

	require.NoError(t, err)

	// Get the JSON representation
	eventJSON, err := event.AsJSON()
	require.NoError(t, err)

	// Marshal to JSON
	jsonBytes, err := json.Marshal(eventJSON)
	require.NoError(t, err)
	assert.NotEmpty(t, jsonBytes)

	// Unmarshal back
	var unmarshaled EventJson
	err = json.Unmarshal(jsonBytes, &unmarshaled)
	require.NoError(t, err)
	assert.Equal(t, event.EventType(), unmarshaled.EventType)
	assert.Equal(t, event.EventSource(), unmarshaled.EventSource)
}

func TestEvent_Getters(t *testing.T) {
	tenantId := uuid.New()
	sessionId := uuid.New()
	requestId := uuid.New()
	metadata := map[string]string{"key": "value"}

	event, err := NewEvent(
		"test.event",
		"test-service",
		"test-user",
		tenantId,
		sessionId,
		requestId,
		metadata,
		"d41d8cd98f00b204e9800998ecf8427e",
	)

	require.NoError(t, err)

	// Test all getters
	assert.Equal(t, "test.event", event.EventType())
	assert.Equal(t, "test-service", event.EventSource())
	assert.Equal(t, "test-user", event.CreatedBy())
	assert.Equal(t, tenantId, event.TenantId())
	assert.Equal(t, sessionId, event.SessionId())
	assert.Equal(t, requestId, event.RequestId())
	assert.Equal(t, metadata, event.Metadata())
	assert.Equal(t, "d41d8cd98f00b204e9800998ecf8427e", event.Md5Hash())
	assert.False(t, event.Timestamp().IsZero())
	assert.NotEqual(t, uuid.Nil, event.Id())
}

func TestEvent_AsJSON_RejectsBlankEvent(t *testing.T) {
	// Create a blank/zero-value Event (bypassing NewEvent)
	var blankEvent Event

	// AsJSON should reject it
	eventJson, err := blankEvent.AsJSON()
	assert.Error(t, err)
	assert.Nil(t, eventJson)
	assert.Contains(t, err.Error(), "must be created using NewEvent()")
}

func TestEvent_AsJSON_RejectsNilEvent(t *testing.T) {
	// Test calling AsJSON on a nil Event pointer
	var nilEvent *Event

	// AsJSON should reject it without panicking
	eventJson, err := nilEvent.AsJSON()
	assert.Error(t, err)
	assert.Nil(t, eventJson)
	assert.Contains(t, err.Error(), "event pointer is nil")
}

func TestEvent_AsJSON_ReturnsImmutableCopy(t *testing.T) {
	// Create a valid event
	event, err := NewEvent(
		"test.event",
		"test-service",
		"test-user",
		uuid.New(),
		uuid.New(),
		uuid.New(),
		map[string]string{"key": "value"},
		"d41d8cd98f00b204e9800998ecf8427e",
	)
	require.NoError(t, err)

	// Get the JSON representation
	eventJson1, err := event.AsJSON()
	require.NoError(t, err)
	originalEventType := eventJson1.EventType

	// Modify the returned EventJson
	eventJson1.EventType = "modified.event.type"
	eventJson1.EventSource = "modified-source"

	// Get the JSON representation again
	eventJson2, err := event.AsJSON()
	require.NoError(t, err)

	// Verify the original Event was not affected by the modification
	assert.Equal(t, originalEventType, eventJson2.EventType)
	assert.Equal(t, "test-service", eventJson2.EventSource)
	assert.NotEqual(t, "modified.event.type", eventJson2.EventType)
	assert.NotEqual(t, "modified-source", eventJson2.EventSource)

	// Also verify via getter methods that the Event itself wasn't modified
	assert.Equal(t, originalEventType, event.EventType())
	assert.Equal(t, "test-service", event.EventSource())
}

func TestEvent_Metadata_ReturnsImmutableCopy(t *testing.T) {
	// Create a valid event with metadata
	event, err := NewEvent(
		"test.event",
		"test-service",
		"test-user",
		uuid.New(),
		uuid.New(),
		uuid.New(),
		map[string]string{"key1": "value1", "key2": "value2"},
		"d41d8cd98f00b204e9800998ecf8427e",
	)
	require.NoError(t, err)

	// Get the metadata
	metadata1 := event.Metadata()
	assert.Equal(t, "value1", metadata1["key1"])
	assert.Equal(t, "value2", metadata1["key2"])

	// Modify the returned map
	metadata1["key1"] = "modified"
	metadata1["key3"] = "new_value"
	delete(metadata1, "key2")

	// Get metadata again and verify the original wasn't affected
	metadata2 := event.Metadata()
	assert.Equal(t, "value1", metadata2["key1"], "Original metadata should not be modified")
	assert.Equal(t, "value2", metadata2["key2"], "Original metadata should not be modified")
	assert.NotContains(t, metadata2, "key3", "New key should not appear in original")
}

func TestEvent_Tags_ReturnsImmutableCopy(t *testing.T) {
	// Create a valid event
	event, err := NewEvent(
		"test.event",
		"test-service",
		"test-user",
		uuid.New(),
		uuid.New(),
		uuid.New(),
		map[string]string{"key": "value"},
		"d41d8cd98f00b204e9800998ecf8427e",
	)
	require.NoError(t, err)

	// Set tags
	event.SetTags(map[string]string{"tag1": "value1", "tag2": "value2"})

	// Get the tags
	tags1 := event.Tags()
	require.NotNil(t, tags1)
	assert.Equal(t, "value1", (*tags1)["tag1"])
	assert.Equal(t, "value2", (*tags1)["tag2"])

	// Modify the returned map
	(*tags1)["tag1"] = "modified"
	(*tags1)["tag3"] = "new_value"
	delete(*tags1, "tag2")

	// Get tags again and verify the original wasn't affected
	tags2 := event.Tags()
	require.NotNil(t, tags2)
	assert.Equal(t, "value1", (*tags2)["tag1"], "Original tags should not be modified")
	assert.Equal(t, "value2", (*tags2)["tag2"], "Original tags should not be modified")
	assert.NotContains(t, *tags2, "tag3", "New key should not appear in original")
}

func TestEvent_Tags_ReturnsNilWhenNotSet(t *testing.T) {
	// Create a valid event without setting tags
	event, err := NewEvent(
		"test.event",
		"test-service",
		"test-user",
		uuid.New(),
		uuid.New(),
		uuid.New(),
		map[string]string{"key": "value"},
		"d41d8cd98f00b204e9800998ecf8427e",
	)
	require.NoError(t, err)

	// Get tags - should be nil
	tags := event.Tags()
	assert.Nil(t, tags, "Tags should be nil when not set")
}
