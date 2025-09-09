package models

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/google/uuid"
)

func TestEventSerialization(t *testing.T) {
	// Create a test event with all required fields
	eventID := uuid.New()
	sessionID := uuid.New()
	requestID := uuid.New()
	tenantID := uuid.New()
	eventType := uuid.New()
	eventSource := uuid.New()
	createdBy := uuid.New()
	md5Hash := uuid.New()

	event := Event{
		Id:          eventID,
		SessionId:   sessionID,
		RequestId:   requestID,
		TenantId:    tenantID,
		EventType:   eventType,
		EventSource: eventSource,
		Metadata:    map[string]uuid.UUID{"meta1": uuid.New()},
		Timestamp:   time.Now(),
		CreatedBy:   createdBy,
		Md5Hash:     md5Hash,
		Payload: map[string]interface{}{
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
	var unmarshaledEvent Event
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

	// Test payload
	if len(unmarshaledEvent.Payload) != 3 {
		t.Errorf("Expected payload with 3 items, got %d", len(unmarshaledEvent.Payload))
	}
	if unmarshaledEvent.Payload["key1"] != "value1" {
		t.Errorf("Expected payload key1 to be 'value1', got %v", unmarshaledEvent.Payload["key1"])
	}
}

func TestEventWithOptionalFields(t *testing.T) {
	eventID := uuid.New()
	sessionID := uuid.New()
	requestID := uuid.New()
	tenantID := uuid.New()
	eventType := uuid.New()
	eventSource := uuid.New()
	ownerID := uuid.New()
	eventSourceURI := uuid.New()
	messageUUID := uuid.New()
	createdBy := uuid.New()
	md5Hash := uuid.New()

	event := Event{
		Id:             eventID,
		SessionId:      sessionID,
		RequestId:      requestID,
		TenantId:       tenantID,
		EventType:      eventType,
		EventSource:    eventSource,
		OwnerId:        &ownerID,
		EventSourceUri: &eventSourceURI,
		Message:        &messageUUID,
		Metadata:       map[string]uuid.UUID{"meta1": uuid.New()},
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
	var unmarshaledEvent Event
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
	if unmarshaledEvent.Message == nil || *unmarshaledEvent.Message != messageUUID {
		t.Errorf("Expected Message %v, got %v", messageUUID, unmarshaledEvent.Message)
	}
}

func TestEventJSONFormat(t *testing.T) {
	eventID := uuid.New()
	sessionID := uuid.New()
	requestID := uuid.New()
	tenantID := uuid.New()
	eventType := uuid.New()
	eventSource := uuid.New()
	createdBy := uuid.New()
	md5Hash := uuid.New()

	event := Event{
		Id:          eventID,
		SessionId:   sessionID,
		RequestId:   requestID,
		TenantId:    tenantID,
		EventType:   eventType,
		EventSource: eventSource,
		Metadata:    map[string]uuid.UUID{"meta1": uuid.New()},
		Timestamp:   time.Now(),
		CreatedBy:   createdBy,
		Md5Hash:     md5Hash,
		Payload: map[string]interface{}{
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
	event := Event{}

	// Should be able to marshal empty event
	jsonData, err := json.Marshal(event)
	if err != nil {
		t.Fatalf("Failed to marshal empty event: %v", err)
	}

	// Should be able to unmarshal back
	var unmarshaledEvent Event
	err = json.Unmarshal(jsonData, &unmarshaledEvent)
	if err != nil {
		t.Fatalf("Failed to unmarshal empty event: %v", err)
	}

	// UUID fields should be zero values
	if unmarshaledEvent.Id != uuid.Nil {
		t.Errorf("Expected nil UUID for Id, got %v", unmarshaledEvent.Id)
	}
}

// Helper function to check if string contains substring
func containsSubstring(str, substr string) bool {
	return len(str) >= len(substr) && str != substr && str[0:len(str)] != substr &&
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
