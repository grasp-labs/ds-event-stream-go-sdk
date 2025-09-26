# ds-event-stream-go-sdk
Go SDK to communicate with DS Kafka Server

## Overview

This SDK provides strongly-typed Kafka producer and consumer classes for communicating with the DS Kafka Server. It supports both development and production environments with automatic configuration for bootstrap servers.

## Installation

```bash
go get github.com/grasp-labs/ds-event-stream-go-sdk
```

## Quick Start

### Creating a Producer

```go
package main

import (
    "context"
    "log"
    "time"

    "github.com/google/uuid"
    "github.com/grasp-labs/ds-event-stream-go-sdk/dskafka"
    "github.com/grasp-labs/ds-event-stream-go-sdk/models"
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
    
    // Create an event
    event := models.EventJson{
        Id:          uuid.New(),
        SessionId:   uuid.New(),
        RequestId:   uuid.New(),
        TenantId:    uuid.New(),
        EventType:   "user.created.v1",
        EventSource: "user-service",
        CreatedBy:   "system",
        Md5Hash:     "abcd1234567890abcd1234567890abcd",
        Metadata:    map[string]string{"version": "1.0"},
        Timestamp:   time.Now(),
        Payload:     &map[string]interface{}{"userId": 123, "email": "user@example.com"},
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

    "github.com/grasp-labs/ds-event-stream-go-sdk/dskafka"
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
- `SendEvent(ctx, topic, event, headers...)` - Send a single event
- `Close()` - Close the producer and free resources

### Consumer Methods

- `NewConsumer(config Config) (*Consumer, error)` - Create a new consumer
- `ReadEvent(ctx, topic, groupID...)` - Read a single event
- `Close()` - Close the consumer and free resources

### Event Structure

Events must follow the `models.EventJson` structure with required fields:

```go
type EventJson struct {
    Id          uuid.UUID                  `json:"id"`          // Required
    SessionId   uuid.UUID                  `json:"session_id"`  // Required
    RequestId   uuid.UUID                  `json:"request_id"`  // Required
    TenantId    uuid.UUID                  `json:"tenant_id"`   // Required
    EventType   string                     `json:"event_type"`  // Required
    EventSource string                     `json:"event_source"` // Required
    CreatedBy   string                     `json:"created_by"`  // Required
    Md5Hash     string                     `json:"md5_hash"`    // Required
    Metadata    map[string]string          `json:"metadata"`    // Required
    Timestamp   time.Time                  `json:"timestamp"`   // Required
    
    // Optional fields
    Payload     *map[string]interface{}    `json:"payload,omitempty"`
    Context     *map[string]interface{}    `json:"context,omitempty"`
    Tags        *map[string]string         `json:"tags,omitempty"`
    Message     *string                    `json:"message,omitempty"`
    // ... other optional fields
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

- Go 1.23+
- Access to DS Kafka environment
- Valid Kafka credentials
