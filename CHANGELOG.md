# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [2.0.0] - 2026-01-14

### Breaking Changes

#### Event Validation and Type Safety
- **BREAKING**: `SendEvent()` and `SafeSendEvent()` now accept `*models.Event` instead of `models.EventJson`
  - Events must now be created using the validated `models.NewEvent()` constructor
  - Direct creation of `EventJson` structs is no longer supported for sending events
  - This ensures all events are validated before being sent to Kafka

#### API Signature Changes
- `Producer.SendEvent(ctx, topic, evt *models.Event, headers...)` (was: `evt models.EventJson`)
- `Producer.SafeSendEvent(ctx, topic, evt *models.Event, headers...)` (was: `evt models.EventJson`)

### Added

#### New Event Validation System
- **`models.NewEvent()`**: Validated constructor for creating events with required fields
  - Validates: `eventType`, `eventSource`, `createdBy` (min length: 1)
  - Validates: `tenantId`, `sessionId`, `requestId` (must not be zero-value UUIDs)
  - Validates: `metadata` (cannot be nil)
  - Validates: `md5Hash` (must match pattern `^[A-Fa-f0-9]{32}$`)
  - Automatically generates unique event `Id` and `Timestamp`

#### Event Builder Pattern
- **Setter methods** for optional fields:
  - `SetMessage(msg string)`
  - `SetOwnerId(ownerId string)`
  - `SetPayload(payload interface{})`
  - `SetAffectedEntityUri(uri string)`
  - `SetEventSourceUri(uri string)`
  - `SetPayloadUri(uri string)`
  - `SetContextUri(uri string)`
  - `SetContext(ctx interface{})`
  - `SetTags(tags map[string]string)`

#### Getter Methods
- **All fields** now have getter methods on `Event`:
  - Immutable required fields: `Id()`, `EventType()`, `EventSource()`, `CreatedBy()`, `TenantId()`, `SessionId()`, `RequestId()`, `Md5Hash()`, `Timestamp()`
  - Optional string pointers: `Message()`, `OwnerId()`, `AffectedEntityUri()`, `EventSourceUri()`, `PayloadUri()`, `ContextUri()`
  - Mutable interface fields (no defensive copy): `Context()`, `Payload()` - see documentation warnings
  - Defensive copies: `Metadata()` returns a copy to prevent external modification
  - Defensive copies: `Tags()` returns a copy to prevent external modification

#### Enhanced JSON Serialization
- **`Event.AsJSON()`**: Returns `([]byte, error)` - JSON bytes ready for transmission
  - Validates event was created via `NewEvent()` (rejects zero-value events)
  - Validates event pointer is not nil
  - Returns JSON bytes directly (no need for separate marshaling)
  - Ensures immutability by serializing a fresh copy each time

### Changed

#### Immutability Improvements
- Validated required fields are immutable after creation (`Id`, `EventType`, `EventSource`, `CreatedBy`, `TenantId`, `SessionId`, `RequestId`, `Metadata`, `Md5Hash`, `Timestamp`)
- `Metadata` getter returns a copy to prevent external modification
- `Tags` getter returns a copy to prevent external modification
- `Context` and `Payload` fields are **not** defensively copied (see documentation for mutability warnings)
- `AsJSON()` validates proper initialization before serialization

#### Producer Behavior
- Producer now uses getter methods (`evt.Id()`, `evt.SessionId()`) for partition key selection
- Removed internal `validateEvent()` function - validation now happens at construction
- `SafeSendEvent()` now safely handles nil event pointers without panicking

### Fixed
- Fixed potential panic in `SafeSendEvent()` when nil event pointer is passed
- Fixed JSON marshaling in tests to use `AsJSON()` directly
- Fixed mutability issues with map fields through defensive copying

### Documentation
- Added comprehensive godoc for `NewEvent()` with parameter descriptions, validation rules, error messages, and usage examples
- Added warnings about `Context` and `Payload` mutability concerns
- Updated all examples to use `models.NewEvent()` constructor
- Added test demonstrating best practices for handling mutable payload data

### Migration Guide

#### Before (v1.x):
```go
event := models.EventJson{
    EventType:   "user.created",
    EventSource: "auth-service",
    CreatedBy:   "system",
    // ... other fields
}
producer.SendEvent(ctx, topic, event)
```

#### After (v2.0.0):
```go
event, err := models.NewEvent(
    "user.created",           // eventType
    "auth-service",           // eventSource
    "system",                 // createdBy
    uuid.New(),               // tenantId
    uuid.New(),               // sessionId
    uuid.New(),               // requestId
    map[string]string{},      // metadata
    "d41d8cd98f00b204e9800998ecf8427e", // md5Hash
)
if err != nil {
    log.Fatal(err)
}

// Set optional fields
event.SetPayload(map[string]interface{}{"userId": 123})
event.SetMessage("User created successfully")

producer.SendEvent(ctx, topic, event)
```

### Internal Changes
- Compiled MD5 regex pattern once at package level for better performance
- Removed unused `encoding/json` imports from producer package
- Simplified producer code by removing redundant validation logic

---

## [1.x.x] - Previous versions
See git history for changes in v1.x releases.

[2.0.0]: https://github.com/grasp-labs/ds-event-stream-go-sdk/compare/v1.0.0...v2.0.0
