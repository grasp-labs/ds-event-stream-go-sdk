# ds-event-stream-go-sdk
![Build](https://github.com/grasp-labs/ds-event-stream-go-sdk/actions/workflows/ci.yml/badge.svg)
[![Go Report Card](https://goreportcard.com/badge/github.com/grasp-labs/ds-event-stream-go-sdk/v2)](https://goreportcard.com/report/github.com/grasp-labs/ds-event-stream-go-sdk/v2)
[![codecov](https://codecov.io/gh/grasp-labs/ds-event-stream-go-sdk/branch/main/graph/badge.svg)](https://codecov.io/gh/grasp-labs/ds-event-stream-go-sdk)
[![Latest tag](https://img.shields.io/github/v/tag/grasp-labs/ds-event-stream-go-sdk?sort=semver)](https://github.com/grasp-labs/ds-event-stream-go-sdk/tags)
![License](https://img.shields.io/github/license/grasp-labs/ds-event-stream-go-sdk?cacheSeconds=60)

Go SDK to communicate with DS Kafka Server

## Overview

This SDK provides strongly-typed Kafka producer and consumer classes for communicating with the DS Kafka Server. It supports both development and production environments with automatic configuration for bootstrap servers.

## Installation

```bash
go get github.com/grasp-labs/ds-event-stream-go-sdk/v2
```

## Quick Start

### Creating a Producer

```go
package main

import (
    "context"
    "log"

    "github.com/google/uuid"
    "github.com/grasp-labs/ds-event-stream-go-sdk/v2/dskafka"
    "github.com/grasp-labs/ds-event-stream-go-sdk/v2/models"
)

func main() {
    // Setup credentials
    credentials := dskafka.ClientCredentials{
        Username: "your-kafka-username",
        Password: "your-kafka-password",
    }
    
    // Get bootstrap servers for your environment
    bootstrapServers := dskafka.GetBootstrapServers(dskafka.Dev, false) // or dskafka.Prod
    
    // Create producer configuration
    config := dskafka.DefaultProducerConfig(credentials, bootstrapServers)
    
    // Create producer
    producer, err := dskafka.NewProducer(config)
    if err != nil {
        log.Panic("Failed to create producer:", err)
    }
    defer producer.Close()
    
    // Create an event using the builder pattern (required for producers)
    event, err := models.NewEventBuilder(
        "user.created.v1",                                // eventType
        "user-service",                                   // eventSource
        "system",                                         // createdBy
        uuid.New(),                                       // tenantId
        uuid.New(),                                       // sessionId
        uuid.New(),                                       // requestId
        map[string]string{"version": "1.0"},            // metadata
        "5d41402abc4b2a76b9719d911017c592",             // md5Hash (MD5 of payload)
    ).WithPayload(map[string]interface{}{
        "userId": 123,
        "email":  "user@example.com",
    }).WithMessage("User account created").Build()
    
    if err != nil {
        log.Panic("Failed to build event:", err)
    }
    
    // Send single event
    err = producer.SendEvent(context.Background(), "user-events", event)
    if err != nil {
        log.Printf("Failed to send event: %v", err)
    }
    
    // Send with custom headers
    headers := []dskafka.Header{
        {Key: "source", Value: "my-service"},
        {Key: "version", Value: "1.0"},
    }
    err = producer.SendEvent(context.Background(), "user-events", event, headers...)
    if err != nil {
        log.Printf("Failed to send event with headers: %v", err)
    }
}
```

### Creating a Consumer

```go
package main

import (
    "context"
    "log"

    "github.com/grasp-labs/ds-event-stream-go-sdk/v2/dskafka"
)

func main() {
    // Setup credentials
    credentials := dskafka.ClientCredentials{
        Username: "your-kafka-username",
        Password: "your-kafka-password",
    }
    
    // Get bootstrap servers for your environment
    bootstrapServers := dskafka.GetBootstrapServers(dskafka.Prod, false) // or dskafka.Dev

    // Create consumer configuration
    config := dskafka.DefaultConsumerConfig(credentials, bootstrapServers, "my-consumer-group")

    // Create consumer
    consumer, err := dskafka.NewConsumer(config)
    if err != nil {
        log.Panic("Failed to create consumer:", err)
    }
    defer consumer.Close()
    
    // Read single event
    event, err := consumer.ReadEvent(context.Background(), "user-events")
    if err != nil {
        log.Printf("Failed to read event: %v", err)
    } else if event != nil {
        log.Printf("Received event: %s from %s", event.EventType, event.EventSource)
    }
    
    // Read with specific consumer group
    event, err = consumer.ReadEvent(context.Background(), "user-events", "my-group")
    if err != nil {
        log.Printf("Failed to read event: %v", err)
    }
       
    // Continuous consumption
    for {
        event, err := consumer.ReadEvent(context.Background(), "user-events", "my-group")
        if err != nil {
            log.Printf("Error reading event: %v", err)
            continue
        }
        
        if event != nil {
            log.Printf("Processing event: %s", event.EventType)
            // Process your event here
        }
    }
}
```

## Configuration

### Environment Setup

The SDK supports two environments and two hostname types (internal/external), creating 4 different of options:

| Environment | Hostname Type | Bootstrap Servers |
|-------------|----------------|-------------------|
| Development | External       | `b0.dev.kafka.ds.local:9095` |
| Development | Internal       | `kafka.kafka-dev.svc.cluster.local:9092` |
| Production  | External       | `b0.kafka.ds.local:9095`, `b1.kafka.ds.local:9095`, `b2.kafka.ds.local:9095` |
| Production  | Internal       | `kafka.kafka.svc.cluster.local:9092` |

Internal hostnames are used for in-cluster communication (e.g. when running consumer/producer inside Kubernetes).

External hostnames are used for communication from outside the cluster (e.g. local development machine).

You can get the appropriate bootstrap servers using the helper function:

```go
// Development environment

// Use external hostnames
bootstrapServers := dskafka.GetBootstrapServers(dskafka.Dev, false)
// Returns: ["b0.dev.kafka.ds.local:9095"]

// Use internal hostnames (for in-cluster communication)
bootstrapServers := dskafka.GetBootstrapServers(dskafka.Dev, true)
// Returns: ["kafka-dev.kafka.svc.cluster.local:9092"]

// Production environment

// Use external hostnames
bootstrapServers := dskafka.GetBootstrapServers(dskafka.Prod, false)
// Returns: ["b0.kafka.ds.local:9095", "b1.kafka.ds.local:9095", "b2.kafka.ds.local:9095"]

// Use internal hostnames (for in-cluster communication)
bootstrapServers := dskafka.GetBootstrapServers(dskafka.Prod, true)
// Returns: ["kafka.kafka.svc.cluster.local:9092"]
```

### Custom Configuration

You can customize the configuration instead of using defaults:

```go
config := dskafka.Config{
    Brokers: []string{"localhost:9092", "localhost:9093"},
    ClientCredentials: dskafka.ClientCredentials{
        Username: "user",
        Password: "pass",
    },
    BatchSize:              50,
    BatchBytes:             512 * 1024, // 512 KB
    BatchTimeout:           100 * time.Millisecond,
    Compression:            kafka.Gzip,
    RequiredAcks:           kafka.RequireAll,
    AllowAutoTopicCreation: false,
    WriteTimeout:           15 * time.Second,
    
    // Consumer-specific
    GroupID:        "my-group",
    MinBytes:       1,
    MaxBytes:       1 << 20, // 1 MB
    MaxWait:        1 * time.Second,
    ReadTimeout:    15 * time.Second,
    CommitInterval: 2 * time.Second,
    StartOffset:    kafka.LastOffset,
}
```

## API Reference

### Producer Methods

- `NewProducer(config Config) (*Producer, error)` - Create a new producer
- `SendEvent(ctx, topic, event *models.SealedEvent, headers...)` - Send a single validated event
- `Close()` - Close the producer and free resources

**Note:** Producers must use `*models.SealedEvent` created via `models.NewEventBuilder(eventType, eventSource, createdBy, tenantId, sessionId, requestId, metadata, md5Hash).Build()`. This ensures all events are validated before sending. See the [Quick Start](#creating-a-producer) section for a complete example.

### Consumer Methods

- `NewConsumer(config Config) (*Consumer, error)` - Create a new consumer
- `ReadEvent(ctx, topic, groupID...)` - Read a single event
- `Close()` - Close the consumer and free resources

### Event Structure

#### For Producers: Use EventBuilder

Producers must create validated events using the builder pattern:

```go
event, err := models.NewEventBuilder(
    eventType,      // string (min length: 1)
    eventSource,    // string (min length: 1)
    createdBy,      // string (min length: 1)
    tenantId,       // uuid.UUID (non-zero)
    sessionId,      // uuid.UUID (non-zero)
    requestId,      // uuid.UUID (non-zero)
    metadata,       // map[string]string (non-nil)
    md5Hash,        // string (32-char hex, required if payload is set)
).WithPayload(payload).        // optional
  WithMessage("msg").          // optional
  WithTags(tags).              // optional
  Build()
```

#### For Consumers: EventJson Structure

Consumers receive `models.EventJson` directly from Kafka:

```go
type EventJson struct {
    Id          uuid.UUID                  `json:"id"`
    SessionId   uuid.UUID                  `json:"session_id"`
    RequestId   uuid.UUID                  `json:"request_id"`
    TenantId    uuid.UUID                  `json:"tenant_id"`
    EventType   string                     `json:"event_type"`
    EventSource string                     `json:"event_source"`
    CreatedBy   string                     `json:"created_by"`
    Md5Hash     string                     `json:"md5_hash"`
    Metadata    map[string]string          `json:"metadata"`
    Timestamp   time.Time                  `json:"timestamp"`
    Payload     interface{}                `json:"payload,omitempty"`
    Message     *string                    `json:"message,omitempty"`
    Tags        *map[string]string         `json:"tags,omitempty"`
    // ... other fields
}
```

## Development

### Building

```bash
# Build all packages
make build

# Run tests
make test

# Run tests with coverage
make test-coverage

# Run integration tests (requires real Kafka)
make integration-test

# Full validation
make check
```

### Code Generation

Models are generated from JSON schemas:

```bash
# Generate Go types from schemas
make generate-types
```

**Note**: After generation, manually update `models/event.go` to use `uuid.UUID` types for ID fields.

## Requirements

- Go 1.25+
- Access to DS Kafka environment
- Valid Kafka credentials
