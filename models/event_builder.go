package models

import (
	"encoding/json"
	"fmt"
	"regexp"
	"time"

	"github.com/google/uuid"
)

// md5HashPattern is a compiled regex for validating MD5 hash format (32-character hex string)
var md5HashPattern = regexp.MustCompile("^[A-Fa-f0-9]{32}$")

// SealedEvent is a validated event ready to be sent to Kafka.
// It wraps EventJson and ensures validation has occurred.
// Use NewEventBuilder() to create sealed events for producers.
// Consumers work directly with EventJson (no sealing needed).
type SealedEvent struct {
	json EventJson
}

// AsJSON returns the JSON representation of the SealedEvent as bytes.
func (e *SealedEvent) AsJSON() ([]byte, error) {
	if e == nil {
		return nil, fmt.Errorf("invalid event: event pointer is nil")
	}
	return json.Marshal(&e.json)
}

// EventBuilder provides a fluent API for constructing validated events.
// Use this for producers. Consumers should work directly with EventJson.
type EventBuilder struct {
	json EventJson
}

// NewEventBuilder creates a new EventBuilder with all required fields.
//
// This builder is intended for producers who need to create validated events.
// Consumers should work directly with EventJson when reading from Kafka.
//
// Parameters:
//   - eventType: Type identifier for the event (min length: 1)
//   - eventSource: Source system that generated the event (min length: 1)
//   - createdBy: User or system that created the event (min length: 1)
//   - tenantId: UUID identifying the tenant (cannot be zero-value)
//   - sessionId: UUID identifying the session (cannot be zero-value)
//   - requestId: UUID for request correlation (cannot be zero-value)
//   - metadata: Key-value metadata (cannot be nil, use empty map if none)
//   - md5Hash: MD5 hash of payload (must be 32-char hex string)
//
// Example:
//
//	event, err := models.NewEventBuilder(
//	    "user.registered.v1",
//	    "auth-service",
//	    "system",
//	    uuid.New(),
//	    uuid.New(),
//	    uuid.New(),
//	    map[string]string{"version": "1.0"},
//	    "d41d8cd98f00b204e9800998ecf8427e",
//	).WithPayload(map[string]interface{}{"userId": 123}).
//	  WithMessage("User registered").
//	  WithTags(map[string]string{"team": "auth"}).
//	  Build()
func NewEventBuilder(
	eventType string,
	eventSource string,
	createdBy string,
	tenantId uuid.UUID,
	sessionId uuid.UUID,
	requestId uuid.UUID,
	metadata map[string]string,
	md5Hash string,
) *EventBuilder {
	return &EventBuilder{
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
	}
}

// WithMessage sets an optional human-readable message.
func (b *EventBuilder) WithMessage(msg string) *EventBuilder {
	b.json.Message = &msg
	return b
}

// WithOwnerId sets an optional owner identifier.
func (b *EventBuilder) WithOwnerId(ownerId string) *EventBuilder {
	b.json.OwnerId = &ownerId
	return b
}

// WithPayload sets the event payload.
func (b *EventBuilder) WithPayload(payload interface{}) *EventBuilder {
	b.json.Payload = payload
	return b
}

// WithAffectedEntityUri sets the URI of the affected entity.
func (b *EventBuilder) WithAffectedEntityUri(uri string) *EventBuilder {
	b.json.AffectedEntityUri = &uri
	return b
}

// WithEventSourceUri sets the URI describing the event source.
func (b *EventBuilder) WithEventSourceUri(uri string) *EventBuilder {
	b.json.EventSourceUri = &uri
	return b
}

// WithPayloadUri sets the URI to external payload.
func (b *EventBuilder) WithPayloadUri(uri string) *EventBuilder {
	b.json.PayloadUri = &uri
	return b
}

// WithContextUri sets the URI to external context.
func (b *EventBuilder) WithContextUri(uri string) *EventBuilder {
	b.json.ContextUri = &uri
	return b
}

// WithContext sets the processing context.
func (b *EventBuilder) WithContext(ctx interface{}) *EventBuilder {
	b.json.Context = ctx
	return b
}

// WithTags sets optional tags for the event.
func (b *EventBuilder) WithTags(tags map[string]string) *EventBuilder {
	b.json.Tags = &tags
	return b
}

// Build validates all fields and returns a validated SealedEvent ready for sending.
//
// Validation rules:
//   - eventType, eventSource, createdBy must have length >= 1
//   - tenantId, sessionId, requestId must not be zero-value UUIDs
//   - metadata must not be nil
//   - md5Hash must match pattern ^[A-Fa-f0-9]{32}$
//
// Returns error if any validation fails.
func (b *EventBuilder) Build() (*SealedEvent, error) {
	// Validate required fields
	if len(b.json.EventType) < 1 {
		return nil, fmt.Errorf("event_type cannot be empty")
	}
	if len(b.json.EventSource) < 1 {
		return nil, fmt.Errorf("event_source cannot be empty")
	}
	if len(b.json.CreatedBy) < 1 {
		return nil, fmt.Errorf("created_by cannot be empty")
	}
	if b.json.TenantId == uuid.Nil {
		return nil, fmt.Errorf("tenant_id cannot be zero-value UUID")
	}
	if b.json.SessionId == uuid.Nil {
		return nil, fmt.Errorf("session_id cannot be zero-value UUID")
	}
	if b.json.RequestId == uuid.Nil {
		return nil, fmt.Errorf("request_id cannot be zero-value UUID")
	}
	if b.json.Metadata == nil {
		return nil, fmt.Errorf("metadata cannot be nil")
	}
	if len(b.json.Md5Hash) < 1 {
		return nil, fmt.Errorf("md5_hash cannot be empty")
	}
	if !md5HashPattern.MatchString(b.json.Md5Hash) {
		return nil, fmt.Errorf("md5_hash must be a valid 32-character hex string")
	}

	return &SealedEvent{json: b.json}, nil
}

// Minimal getters needed for producer (partition key generation)

// Id returns the event ID.
func (e *SealedEvent) Id() uuid.UUID {
	return e.json.Id
}

// SessionId returns the session ID.
func (e *SealedEvent) SessionId() uuid.UUID {
	return e.json.SessionId
}

// EventType returns the event type.
func (e *SealedEvent) EventType() string {
	return e.json.EventType
}

// EventSource returns the event source.
func (e *SealedEvent) EventSource() string {
	return e.json.EventSource
}

// Additional getters for testing purposes

// RequestId returns the request ID.
func (e *SealedEvent) RequestId() uuid.UUID {
	return e.json.RequestId
}

// TenantId returns the tenant ID.
func (e *SealedEvent) TenantId() uuid.UUID {
	return e.json.TenantId
}

// CreatedBy returns the creator identifier.
func (e *SealedEvent) CreatedBy() string {
	return e.json.CreatedBy
}

// Md5Hash returns the MD5 hash.
func (e *SealedEvent) Md5Hash() string {
	return e.json.Md5Hash
}

// Payload returns the event payload.
func (e *SealedEvent) Payload() interface{} {
	return e.json.Payload
}

// Timestamp returns the event timestamp.
func (e *SealedEvent) Timestamp() time.Time {
	return e.json.Timestamp
}
