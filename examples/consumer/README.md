# Simple Kafka Consumer Example

This example demonstrates how to use the DS Event Stream Go SDK to consume events from a Kafka topic. It loops until it finds at least one message or reaches the configured limits.

## Features

- **Persistent message search**: Loops until finding at least one message
- **Configurable retry behavior**: Set maximum attempts and total timeout
- **Flexible topic selection**: Choose which topic to consume from
- **Event details display**: Shows formatted event information
- **Enhanced error handling**: Detailed explanations for common connection issues
- **Connection diagnostics**: Shows bootstrap servers for debugging
- **Smart timeout handling**: Uses short timeouts per attempt with overall limit
- **Progress feedback**: Shows real-time progress of consumption attempts

> ✅ **Status**: Recently updated with robust looping and retry logic

## Usage

### Quick Start

**Option 1: Using AWS SSM Parameter Store (Recommended)**
```bash
go run main.go -use-ssm
```

**Option 2: Using command line password**
```bash
go run main.go -password=your-kafka-password
```

**Option 3: With custom username and SSM**
```bash
go run main.go -username=your-username -use-ssm
```

### Command Line Options

| Flag | Type | Default | Description |
|------|------|---------|-------------|
| `-username` | string | `ds.test.consumer.v1` | Kafka SASL username |
| `-password` | string | - | Kafka SASL password (optional if using SSM) |
| `-use-ssm` | bool | `false` | Get password from AWS SSM Parameter Store |
| `-group` | string | `example-consumer-group` | Consumer group ID |
| `-topic` | string | `ds.workflow.pipeline.job.requested.v1` | Kafka topic to consume from |
| `-timeout` | duration | `30s` | Total timeout for finding a message |
| `-max-attempts` | int | `10` | Maximum number of read attempts |
| `-from-end` | bool | `false` | Start consuming from the end of the topic (skip existing messages) |

### Examples

1. **Using AWS SSM Parameter Store (Recommended)**:
```bash
go run main.go -use-ssm
```
This will automatically fetch the password from `/ds/kafka/dev/principals/ds.test.consumer.v1`

2. **Basic usage with command line password**:
```bash
go run main.go -password=supersecret
```

2. **Quick test with shorter timeout**:
```bash
go run main.go -password=supersecret -timeout=10s -max-attempts=3
```

3. **Custom username and password**:
```bash
go run main.go -username=myuser -password=supersecret
```

3. **Custom topic and timeout**:
```bash
go run main.go -password=supersecret -topic=user-events -timeout=60s
```

4. **Custom consumer group**:
```bash
go run main.go -password=supersecret -group=my-consumer-group
```

### Expected Output

When consumer runs successfully (recent run):
```
Starting simple consumer example...
Group ID: example-consumer-group
Topic: ds.workflow.pipeline.job.requested.v1
Bootstrap servers: [b0.dev.kafka.ds.local:9095]
Reading one event from topic 'ds.workflow.pipeline.job.requested.v1' with 30s timeout...
No message received within 30s timeout
✅ Consumer example completed successfully
```

When an event is available:
```
Starting simple consumer example...
Group ID: example-consumer-group
Topic: ds.workflow.pipeline.job.requested.v1
Bootstrap servers: [b0.dev.kafka.ds.local:9095]
Reading one event from topic 'ds.workflow.pipeline.job.requested.v1' with 30s timeout...
📨 Received event:
  🆔 ID: 550e8400-e29b-41d4-a716-446655440000
  📋 Type: ds.workflow.pipeline.job.requested.v1
  🏭 Source: pipeline-service
  👤 Created By: system
  🕐 Timestamp: 2024-01-15T10:30:00Z
  💬 Message: Pipeline job requested for processing
  📦 Payload: map[jobId:job_123 priority:high]
✅ Consumer example completed successfully
```

## How It Works

1. **Setup**: Creates Kafka consumer with SASL authentication
2. **Read**: Attempts to read one event from the specified topic
3. **Display**: Shows formatted event details if received
4. **Exit**: Cleanly shuts down after processing one event or timeout

## Configuration

The example uses:
- **Username**: `ds.consumption.ingress.v1` (default, can be overridden with `-username`)
- **Environment**: Development environment with internal bootstrap servers
- **Authentication**: SASL SCRAM-SHA-512

## Dependencies

This example has its own `go.mod` file with the following dependencies:
- Go 1.23+
- github.com/grasp-labs/ds-event-stream-go-sdk (via replace directive to main module)
- github.com/segmentio/kafka-go (for additional Kafka types)
- github.com/aws/aws-sdk-go-v2/config (for SSM functionality)
- github.com/aws/aws-sdk-go-v2/service/ssm (for SSM functionality)
- Access to Kafka cluster with proper credentials
- AWS credentials configured (when using `-use-ssm`)

The AWS SDK dependencies are isolated to this example and do not affect the main SDK module.

## Module Structure

This example uses its own Go module (`consumer-example`) with a replace directive that points to the main SDK:

```go.mod
module consumer-example

replace github.com/grasp-labs/ds-event-stream-go-sdk => ../..
```

This allows the example to have AWS dependencies while keeping the main SDK clean and focused.

## Troubleshooting

### Common Issues

1. **"Password is required"**: Use the `-password` flag
2. **EOF Error**: This usually means Kafka brokers are not accessible
   ```
   ❌ Connection Error (EOF): error dialing all brokers, one of the errors: EOF
   ```
   **Solutions:**
   - Check if Kafka cluster is running
   - Verify network connectivity to brokers (`b0.dev.kafka.ds.local:9095`)
   - Check firewall and security group settings
   - Ensure you're on the correct network (VPN if required)

3. **Connection Refused**: Kafka is not running on the specified ports
4. **Authentication failed**: Verify the password is correct
5. **Topic not found**: Ensure the topic exists or use a different topic name

### Debug Tips

- The consumer shows bootstrap servers on startup: `Bootstrap servers: [b0.dev.kafka.ds.local:9095]`
- Increase timeout with `-timeout=60s` for slower networks
- Try different topics with `-topic=your-topic-name`
- Check Kafka cluster connectivity before running

### What does EOF mean?

**EOF (End of File)** in Kafka context means the connection to the broker was closed unexpectedly. This is **NOT** about empty queues - it's a connectivity issue. Common causes:

- 🔌 **No connection**: Brokers are down or unreachable
- 🌐 **Network issues**: DNS resolution, routing, or firewall problems  
- 🏠 **Wrong address**: Incorrect broker hostnames or ports
- 🔒 **Security**: Network policies blocking the connection

If you want to check for empty queues, you'll get a timeout instead of EOF.