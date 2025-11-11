# Kafka Producer Example

This example demonstrates how to use the DS Event Stream Go SDK to produce events to Kafka topics.

## Features

- **Simple event production**: Creates and sends structured events to Kafka
- **Automatic event generation**: Generates UUIDs and timestamps automatically
- **Custom headers support**: Send events with additional metadata headers
- **Multiple send examples**: Shows different ways to send events
- **Connection diagnostics**: Shows bootstrap servers for debugging
- **Error handling**: Comprehensive error handling and reporting

> ✅ **Status**: Active and working - sends events to production topics

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
| `-username` | string | `ds.test.producer.v1` | Kafka SASL username |
| `-password` | string | - | Kafka SASL password (optional if using SSM) |
| `-use-ssm` | bool | `false` | Get password from AWS SSM Parameter Store |
| `-topic` | string | `ds.test.message.created.v1` | Topic to produce to |

### Examples

1. **Using AWS SSM Parameter Store (Recommended)**:
```bash
go run main.go -use-ssm
```
This will automatically fetch the password from `/ds/kafka/dev/principals/ds.test.producer.v1`

2. **Using command line password**:
```bash
go run main.go -password=supersecret
```

3. **Custom username with SSM**:
```bash
go run main.go -username=my.custom.producer.v1 -use-ssm
```
This will fetch the password from `/ds/kafka/dev/principals/my.custom.producer.v1`

4. **Custom username and password**:
```bash
go run main.go -username=myuser -password=supersecret
```

### Expected Output

When producer runs successfully:
```
Fetching password from command line arguments
Setting up credentials
Getting bootstrap servers
Creating producer configuration
Creating producer
Creating event
Sending event
Sending event with custom headers
Done
```

## How It Works

1. **Setup**: Creates Kafka producer with SASL authentication
2. **Event Creation**: Generates a structured event with all required fields
3. **Send Event**: Sends the event to the specified topic
4. **Send with Headers**: Demonstrates sending with custom headers
5. **Cleanup**: Properly closes the producer connection

## Event Structure

The producer creates events with the following structure:

```go
event := models.EventJson{
    Id:          uuid.New(),                    // Unique event ID
    SessionId:   uuid.New(),                    // Session identifier
    RequestId:   uuid.New(),                    // Request identifier  
    TenantId:    uuid.New(),                    // Tenant identifier
    EventType:   "user.created.v1",             // Event type
    EventSource: "user-service",                // Source service
    CreatedBy:   "system",                      // Creator
    Md5Hash:     "abcd1234567890abcd1234567890abcd", // Hash
    Metadata:    map[string]string{"version": "1.0"}, // Metadata
    Timestamp:   time.Now(),                    // Current timestamp
    Payload:     &map[string]interface{}{       // Event payload
        "userId": 123, 
        "email": "user@example.com"
    },
}
```

## Custom Headers

The example shows how to send events with additional headers:

```go
headers := []dskafka.Header{
    {Key: "source", Value: "my-service"},
    {Key: "version", Value: "1.0"},
}
err = producer.SendEvent(context.Background(), "topic-name", event, headers...)
```

## Configuration

### Kafka Configuration
The example uses:
- **Username**: `ds.test.producer.v1` (default, can be overridden with `-username`)
- **Environment**: Development environment with internal bootstrap servers
- **Authentication**: SASL SCRAM-SHA-512
- **Target Topic**: `ds.test.message.created.v1` (can be overridden with `-topic`)

### AWS SSM Configuration
When using `-use-ssm`, the application:
- **Parameter Path**: `/ds/kafka/dev/principals/{username}` (e.g., `/ds/kafka/dev/principals/ds.test.producer.v1`)
- **Authentication**: Uses AWS SDK default credential chain (IAM role, profile, environment variables)
- **Permissions Required**: `ssm:GetParameter` on the specified parameter path
- **Parameter Type**: Supports both `String` and `SecureString` parameter types

## Dependencies

This example has its own `go.mod` file with the following dependencies:
- Go 1.23+
- github.com/grasp-labs/ds-event-stream-go-sdk (via replace directive to main module)
- github.com/google/uuid
- github.com/aws/aws-sdk-go-v2/config (for SSM functionality)
- github.com/aws/aws-sdk-go-v2/service/ssm (for SSM functionality)
- Access to Kafka cluster with proper credentials
- AWS credentials configured (when using `-use-ssm`)

The AWS SDK dependencies are isolated to this example and do not affect the main SDK module.

## Module Structure

This example uses its own Go module (`producer-example`) with a replace directive that points to the main SDK:

```go.mod
module producer-example

replace github.com/grasp-labs/ds-event-stream-go-sdk => ../..
```

This allows the example to have AWS dependencies while keeping the main SDK clean and focused.

## Code Walkthrough

### 1. Authentication Setup
```go
credentials := dskafka.ClientCredentials{
    Username: "ds.consumption.ingress.v1",
    Password: *password,
}
```

### 2. Producer Configuration
```go
bootstrapServers := dskafka.GetBootstrapServers(dskafka.Dev, false)
config := dskafka.DefaultProducerConfig(credentials, bootstrapServers)
producer, err := dskafka.NewProducer(config)
```

### 3. Event Creation
```go
event := models.EventJson{
    // All required fields populated automatically
    Id: uuid.New(),
    EventType: "user.created.v1",
    // ... other fields
}
```

### 4. Sending Events
```go
// Simple send
err = producer.SendEvent(context.Background(), "topic-name", event)

// Send with headers
err = producer.SendEvent(context.Background(), "topic-name", event, headers...)
```

## Troubleshooting

### Common Issues

1. **"Password is required"**: Use the `-password` flag or `-use-ssm=true`
2. **AWS SSM Errors**: 
   - **"NoCredentialProviders"**: Configure AWS credentials using AWS CLI, environment variables, or IAM role
   - **"ParameterNotFound"**: Verify the SSM parameter exists at the expected path
   - **"AccessDenied"**: Ensure your AWS credentials have `ssm:GetParameter` permissions
3. **EOF Error**: Kafka brokers are not accessible
   ```
   Failed to create producer: error dialing all brokers, one of the errors: EOF
   ```
   **Solutions:**
   - Check if Kafka cluster is running
   - Verify network connectivity to brokers
   - Ensure you're on the correct network (VPN if required)

3. **Connection Refused**: Kafka is not running on the specified ports
4. **Authentication failed**: Verify the password is correct
5. **Send failures**: Check topic permissions and existence

### Debug Tips

- Check the bootstrap servers configuration
- Verify Kafka cluster connectivity: `telnet b0.dev.kafka.ds.local 9095`
- Ensure you have write permissions to the target topic
- Check producer logs for detailed error messages

### Production Considerations

- **Error Handling**: Add proper error handling for production use
- **Retries**: Implement retry logic for failed sends
- **Monitoring**: Add metrics and logging for production monitoring
- **Batching**: Consider batching multiple events for better performance
- **Async Sends**: Use async sends for high-throughput scenarios

## Related Examples

- **Consumer Example**: See `../consumer/` for consuming events
- **Enhanced Producer**: Check the main SDK for advanced producer features like batching and async operations

## Next Steps

1. Modify the event structure for your use case
2. Change the target topic name
3. Add custom headers specific to your application
4. Implement proper error handling and retry logic
5. Add logging and monitoring for production use