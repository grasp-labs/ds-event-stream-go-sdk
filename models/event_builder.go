package models

import (
	"fmt"
	"regexp"
	"time"

	"github.com/google/uuid"
)

// md5HashPattern is a compiled regex for validating MD5 hash format (32-character hex string)
var md5HashPattern = regexp.MustCompile("^[A-Fa-f0-9]{32}$")

// Event wraps EventJson with validated construction.
// The json field is unexported, so Event can only be properly initialized via NewEvent().
type Event struct {
	json EventJson // unexported - cannot be set from other packages
}

// NewEvent creates a validated Event with all required fields.
//
// This is the only way to create a valid Event. The Event struct cannot be
// instantiated directly from other packages due to its unexported fields.
// All validation is performed during construction to ensure data integrity.
//
// Parameters:
//   - eventType: Type identifier for the event (min length: 1). Examples: "user.created.v1", "order.shipped.v1"
//   - eventSource: Source system or service that generated the event (min length: 1). Example: "payment-service"
//   - createdBy: User or system that created the event (min length: 1). Example: "user@example.com", "system"
//   - tenantId: UUID identifying the tenant/organization that owns this event
//   - sessionId: UUID identifying the session in which this event occurred
//   - requestId: UUID for correlating this event with a specific request or operation
//   - metadata: Key-value pairs for additional event metadata (cannot be nil, but can be empty map)
//   - md5Hash: MD5 hash of the event payload (must be a valid 32-character hexadecimal string)
//
// Validation:
//   - eventType, eventSource, and createdBy must have minimum length of 1
//   - metadata must not be nil (use empty map if no metadata needed)
//   - md5Hash must match the pattern ^[A-Fa-f0-9]{32}$ (32-character hex string)
//   - UUIDs are validated by the uuid.UUID type itself
//
// Returns:
//   - *Event: A validated Event with a unique generated ID and current timestamp
//   - error: Validation error if any parameter constraint is violated
//
// Errors:
//   - "event_type cannot be empty" - when eventType is empty string
//   - "event_source cannot be empty" - when eventSource is empty string
//   - "created_by cannot be empty" - when createdBy is empty string
//   - "metadata cannot be nil" - when metadata is nil
//   - "md5_hash cannot be empty" - when md5Hash is empty string
//   - "md5_hash must be a valid 32-character hex string" - when md5Hash format is invalid
//
// Example:
//
//	event, err := models.NewEvent(
//	    "user.registered.v1",
//	    "auth-service",
//	    "system",
//	    uuid.New(),
//	    uuid.New(),
//	    uuid.New(),
//	    map[string]string{"version": "1.0", "environment": "production"},
//	    "d41d8cd98f00b204e9800998ecf8427e",
//	)
//	if err != nil {
//	    log.Fatalf("Failed to create event: %v", err)
//	}
//
//	// Set optional fields using builder methods
//	event.SetPayload(map[string]interface{}{"userId": 123, "email": "user@example.com"})
//	event.SetMessage("User successfully registered")
//	event.SetTags(map[string]string{"team": "auth", "priority": "high"})
//
// After creation, use getter methods to access field values and AsJSON() to
// serialize for transmission. The Event remains immutable; AsJSON() returns a copy.
func NewEvent(
	eventType string,
	eventSource string,
	createdBy string,
	tenantId uuid.UUID,
	sessionId uuid.UUID,
	requestId uuid.UUID,
	metadata map[string]string,
	md5Hash string,
) (*Event, error) {
	// Validate required fields with minLength constraints
	if len(eventType) < 1 {
		return nil, fmt.Errorf("event_type cannot be empty")
	}
	if len(eventSource) < 1 {
		return nil, fmt.Errorf("event_source cannot be empty")
	}
	if len(createdBy) < 1 {
		return nil, fmt.Errorf("created_by cannot be empty")
	}
	if metadata == nil {
		return nil, fmt.Errorf("metadata cannot be nil")
	}
	if len(md5Hash) < 1 {
		return nil, fmt.Errorf("md5_hash cannot be empty")
	}
	// Validate md5Hash matches the required pattern (32-character hex string)
	if !md5HashPattern.MatchString(md5Hash) {
		return nil, fmt.Errorf("md5_hash must be a valid 32-character hex string")
	}

	return &Event{
		json: EventJson{
			Id:          uuid.New(),
			EventType:   eventType,
			EventSource: eventSource,
			CreatedBy:   createdBy,
			TenantId:    tenantId,
			SessionId:   sessionId,
			RequestId:   requestId,
			Metadata:    metadata,
			Md5Hash:     md5Hash,
			Timestamp:   time.Now().UTC(),
		},
	}, nil
}

// AsJSON returns the underlying EventJson for serialization.
// Returns an error if the Event was not properly initialized via NewEvent().
// Returns a copy to maintain immutability of the validated Event.
func (e *Event) AsJSON() (*EventJson, error) {
	// Check for nil pointer to prevent panic
	if e == nil {
		return nil, fmt.Errorf("invalid event: event pointer is nil")
	}
	// Validate that Event was properly initialized (detect zero-value Events)
	// Check timestamp since it's only set in NewEvent() and can't be zero for valid events
	if e.json.Timestamp.IsZero() {
		return nil, fmt.Errorf("invalid event: event must be created using NewEvent()")
	}

	// Return a copy to prevent external modification of the validated Event
	jsonCopy := e.json
	return &jsonCopy, nil
}

// Setters for optional fields

func (e *Event) SetAffectedEntityUri(uri string) {
	e.json.AffectedEntityUri = &uri
}

func (e *Event) SetContext(ctx interface{}) {
	e.json.Context = ctx
}

func (e *Event) SetContextUri(uri string) {
	e.json.ContextUri = &uri
}

func (e *Event) SetEventSourceUri(uri string) {
	e.json.EventSourceUri = &uri
}

func (e *Event) SetMessage(msg string) {
	e.json.Message = &msg
}

func (e *Event) SetOwnerId(ownerId string) {
	e.json.OwnerId = &ownerId
}

func (e *Event) SetPayload(payload interface{}) {
	e.json.Payload = payload
}

func (e *Event) SetPayloadUri(uri string) {
	e.json.PayloadUri = &uri
}

func (e *Event) SetTags(tags map[string]string) {
	e.json.Tags = &tags
}

// Getters for all fields

func (e *Event) Id() uuid.UUID {
	return e.json.Id
}

func (e *Event) EventType() string {
	return e.json.EventType
}

func (e *Event) EventSource() string {
	return e.json.EventSource
}

func (e *Event) CreatedBy() string {
	return e.json.CreatedBy
}

func (e *Event) TenantId() uuid.UUID {
	return e.json.TenantId
}

func (e *Event) SessionId() uuid.UUID {
	return e.json.SessionId
}

func (e *Event) RequestId() uuid.UUID {
	return e.json.RequestId
}

// Metadata returns a copy of the metadata map to prevent external modification.
func (e *Event) Metadata() map[string]string {
	if e.json.Metadata == nil {
		return nil
	}
	copy := make(map[string]string, len(e.json.Metadata))
	for k, v := range e.json.Metadata {
		copy[k] = v
	}
	return copy
}

func (e *Event) Md5Hash() string {
	return e.json.Md5Hash
}

func (e *Event) Timestamp() time.Time {
	return e.json.Timestamp
}

func (e *Event) AffectedEntityUri() *string {
	return e.json.AffectedEntityUri
}

func (e *Event) Context() interface{} {
	return e.json.Context
}

func (e *Event) ContextUri() *string {
	return e.json.ContextUri
}

func (e *Event) EventSourceUri() *string {
	return e.json.EventSourceUri
}

func (e *Event) Message() *string {
	return e.json.Message
}

func (e *Event) OwnerId() *string {
	return e.json.OwnerId
}

func (e *Event) Payload() interface{} {
	return e.json.Payload
}

func (e *Event) PayloadUri() *string {
	return e.json.PayloadUri
}

// Tags returns a copy of the tags map to prevent external modification.
// Returns nil if tags have not been set.
func (e *Event) Tags() *map[string]string {
	if e.json.Tags == nil {
		return nil
	}
	copy := make(map[string]string, len(*e.json.Tags))
	for k, v := range *e.json.Tags {
		copy[k] = v
	}
	return &copy
}
