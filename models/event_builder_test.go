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
	eventJSON := event.AsJSON()

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
