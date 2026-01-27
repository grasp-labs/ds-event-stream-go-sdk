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
	).Build()

	assert.Error(t, err)
	assert.Nil(t, event)
	assert.Contains(t, err.Error(), "md5_hash cannot be empty")
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
			).Build()

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
