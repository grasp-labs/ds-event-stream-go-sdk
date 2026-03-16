# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [2.1.0] - 2026-03-16

### Added
- Test coverage for valid MD5 hash with payload validation
- Added ds-go-kit

## [2.0.0] - 2026-01-14

### Breaking Changes

#### Event Validation and Type Safety
- **BREAKING**: `SendEvent()` and `SafeSendEvent()` now accept `*models.SealedEvent` instead of `models.EventJson`
  - Outgoing events must now be created using the `models.NewEventBuilder()` with `.Build()`
  - Direct creation of `EventJson` structs is no longer used by producers
  - Consumers continue to work directly with `EventJson` when reading from Kafka
  - This ensures all produced events are validated before being sent

#### API Signature Changes
- `Producer.SendEvent(ctx, topic, evt *models.SealedEvent, headers...)` (was: `evt models.EventJson`)
- `Producer.SafeSendEvent(ctx, topic, evt *models.SealedEvent, headers...)` (was: `evt models.EventJson`)

### Added

#### New SealedEvent Type (For Producers)
- **`models.SealedEvent`**: A validated, immutable event wrapper
  - Created via `EventBuilder.Build()` - cannot be constructed directly
  - Minimal API surface with only `AsJSON()` for serialization
  - Only exposes getters needed by producer internals (Id, SessionId, EventType, EventSource)
  - Clear separation: `SealedEvent` = validated producer events, `EventJson` = consumer events

#### New Event Builder Pattern (For Producers)
- **`models.NewEventBuilder()`**: Fluent builder for creating validated events
  - Validates all required fields: `eventType`, `eventSource`, `createdBy` (min length: 1)
  - Validates UUIDs: `tenantId`, `sessionId`, `requestId` (must not be zero-value)
  - Validates: `metadata` (cannot be nil)
  - Validates: `md5Hash` (must match pattern `^[A-Fa-f0-9]{32}$`)
  - Automatically generates unique event `Id` and `Timestamp`
  - Validation occurs in `.Build()` method

#### Builder Methods (Fluent API)
- **`WithMessage(msg string)`**: Set optional message
- **`WithOwnerId(ownerId string)`**: Set optional owner ID
- **`WithPayload(payload interface{})`**: Set event payload
- **`WithAffectedEntityUri(uri string)`**: Set affected entity URI
- **`WithEventSourceUri(uri string)`**: Set event source URI
- **`WithPayloadUri(uri string)`**: Set payload URI
- **`WithContextUri(uri string)`**: Set context URI
- **`WithContext(ctx interface{})`**: Set processing context
- **`WithTags(tags map[string]string)`**: Set optional tags
- **`Build()`**: Validates and returns `*Event` or error

#### SealedEvent Methods
- **`AsJSON()`**: Serialize sealed event to JSON bytes for transmission
- **Minimal getters** (used internally by producer for partition key generation):
  - `Id()`, `SessionId()`, `EventType()`, `EventSource()`
  - Additional test-only getters: `RequestId()`, `TenantId()`, `CreatedBy()`, `Md5Hash()`, `Payload()`, `Timestamp()`
- **No getters for optional fields**: Tags, Message, Context, URIs, etc. are sealed within the event
- **Read-only**: Once built, the sealed event cannot be modified or inspected (except via `AsJSON()`)

### Architecture

#### Producer vs Consumer
- **Producers**: Use `NewEventBuilder()` with fluent API and `.Build()` for validated `SealedEvent` creation
  - Set all fields using builder's `WithX()` methods before calling `Build()`
  - Once sealed, the event is opaque - no getters for optional fields like tags, message, payload details
  - Only `AsJSON()` is available for serialization to Kafka
- **Consumers**: Work directly with `EventJson` when reading from Kafka (no validation or sealing needed)
  - Full access to all fields via public struct fields
  - Can inspect, modify, and process all event data
- **SealedEvent**: Validated wrapper around `EventJson` with minimal API surface - can only be created via builder

### Migration Guide

**Before (v1.x):**
```go
event := models.EventJson{
    Id: uuid.New(),
    EventType: "user.created.v1",
    EventSource: "auth-service",
    // ... other fields
}
producer.SendEvent(ctx, topic, event)
```

**After (v2.0):**
```go
// Using builder pattern with method chaining
event, err := models.NewEventBuilder(
    "user.created.v1",  // eventType
    "auth-service",     // eventSource
    "system",           // createdBy
    uuid.New(),         // tenantId
    uuid.New(),         // sessionId
    uuid.New(),         // requestId
    map[string]string{"version": "1.0"},  // metadata
    "d41d8cd98f00b204e9800998ecf8427e",   // md5Hash
).WithPayload(map[string]interface{}{"userId": 123}).
  WithMessage("User created").
  WithTags(map[string]string{"env": "prod"}).
  Build()

if err != nil {
    log.Fatal(err)
}

producer.SendEvent(ctx, topic, event)
```

**Consumers (unchanged):**
```go
// Consumers still work with EventJson directly
msg, err := consumer.FetchMessage(ctx)
var event models.EventJson
json.Unmarshal(msg.Value, &event)
```

#### Enhanced JSON Serialization
- **`SealedEvent.AsJSON()`**: Returns `([]byte, error)` - JSON bytes ready for transmission
  - Validates sealed event pointer is not nil
  - Returns JSON bytes directly (no need for separate marshaling)
  - All validation happens in `Build()` - `AsJSON()` only handles serialization

### Changed

#### Immutability Improvements
- All fields in `SealedEvent` are immutable after `Build()` - the event is sealed and cannot be modified
- No setters or public fields exposed on `SealedEvent`
- Only minimal getters for producer internals (partition key generation)
- Once built, the event can only be serialized via `AsJSON()` - no way to inspect or modify optional fields

#### Producer Behavior
- Producer now uses getter methods (`evt.Id()`, `evt.SessionId()`) for partition key selection
- All validation now happens at `Build()` time - no validation in producer
- `SafeSendEvent()` safely handles nil event pointers without panicking

### Fixed
- Fixed potential panic in `SafeSendEvent()` when nil event pointer is passed
- Removed unnecessary validation logic from producer (now handled by builder)

### Documentation
- Added comprehensive godoc for `NewEventBuilder()` with parameter descriptions, validation rules, and usage examples
- Clear documentation about `SealedEvent` immutability and sealed nature
- Updated all examples to use `models.NewEventBuilder()` with fluent API
- Added documentation about producer vs consumer architecture

### Internal Changes
- Compiled MD5 regex pattern once at package level for better performance
- Removed unused `encoding/json` imports from producer package
- Simplified producer code by removing redundant validation logic

---

## [1.x.x] - Previous versions
See git history for changes in v1.x releases.

[2.0.0]: https://github.com/grasp-labs/ds-event-stream-go-sdk/compare/v1.0.0...v2.0.0
