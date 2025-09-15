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
