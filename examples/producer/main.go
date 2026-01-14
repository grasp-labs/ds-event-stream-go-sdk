package main

import (
	"context"
	"flag"
	"log"
	"time"

	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/ssm"
	"github.com/google/uuid"
	"github.com/grasp-labs/ds-event-stream-go-sdk/dskafka"
	"github.com/grasp-labs/ds-event-stream-go-sdk/models"
)

// getPasswordFromSSM retrieves a password from AWS Systems Manager Parameter Store
func getPasswordFromSSM(ctx context.Context, parameterName string) (string, error) {
	// Load the AWS configuration
	cfg, err := config.LoadDefaultConfig(ctx)
	if err != nil {
		return "", err
	}

	// Create SSM client
	ssmClient := ssm.NewFromConfig(cfg)

	// Get the parameter with decryption enabled
	input := &ssm.GetParameterInput{
		Name:           &parameterName,
		WithDecryption: &[]bool{true}[0], // Enable decryption for SecureString parameters
	}

	result, err := ssmClient.GetParameter(ctx, input)
	if err != nil {
		return "", err
	}

	return *result.Parameter.Value, nil
}

// pass password as argument or get from SSM
// go run main.go -password=supersecret
// go run main.go -use-ssm (gets password from SSM)
func main() {
	// Get configuration from command line arguments
	username := flag.String("username", "ds.test.producer.v1", "Kafka username")
	password := flag.String("password", "", "Kafka password (optional if using SSM)")
	useSSM := flag.Bool("use-ssm", false, "Get password from AWS SSM Parameter Store")
	topic := flag.String("topic", "ds.test.message.created.v1", "Topic to produce to")
	flag.Parse()

	var actualPassword string
	var err error

	if *useSSM {
		log.Println("Fetching password from AWS SSM Parameter Store")
		// Construct SSM parameter name based on username
		parameterName := "/ds/kafka/dev/principals/" + *username
		log.Printf("Getting password from SSM parameter: %s", parameterName)

		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		actualPassword, err = getPasswordFromSSM(ctx, parameterName)
		if err != nil {
			log.Fatalf("Failed to get password from SSM: %v", err)
		}
		log.Println("Successfully retrieved password from SSM")
	} else if *password != "" {
		log.Println("Using password from command line argument")
		actualPassword = *password
	} else {
		log.Fatal("Password is required. Use -password=your-kafka-password or -use-ssm=true")
	}

	log.Println("Setting up credentials")
	// Setup credentials
	credentials := dskafka.ClientCredentials{
		Username: *username,
		Password: actualPassword,
	}

	log.Println("Getting bootstrap servers")
	// Get bootstrap servers for your environment
	bootstrapServers := dskafka.GetBootstrapServers(dskafka.Dev, false) // or dskafka.Dev

	log.Println("Creating producer configuration")
	// Create producer configuration
	config := dskafka.DefaultProducerConfig(credentials, bootstrapServers)

	log.Println("Creating producer")
	// Create producer
	producer, err := dskafka.NewProducer(config)
	if err != nil {
		log.Fatal("Failed to create producer:", err)
	}
	defer func() {
		if err := producer.Close(); err != nil {
			log.Printf("Failed to close producer: %v", err)
		}
	}()

	log.Println("Creating event with object payload")
	// Create an event with object payload using builder pattern
	eventWithObject, err := models.NewEventBuilder(
		"test.object.v1",                    // eventType
		"TEST-PRODUCER-GO",                  // eventSource
		"system",                            // createdBy
		uuid.New(),                          // tenantId
		uuid.New(),                          // sessionId
		uuid.New(),                          // requestId
		map[string]string{"version": "1.0"}, // metadata
		"abcd1234567890abcd1234567890abcd",  // md5Hash
	).WithPayload(map[string]interface{}{"userId": 123, "email": "user@example.com"}).
		Build()

	if err != nil {
		log.Fatalf("Failed to create event: %v", err)
	}

	log.Println("Sending event with object payload")
	err = producer.SendEvent(context.Background(), *topic, eventWithObject)
	if err != nil {
		log.Printf("Failed to send event with object payload: %v", err)
	} else {
		log.Println("✅ Successfully sent event with object payload")
	}

	log.Println("Creating event with array payload")
	// Create an event with array payload using builder pattern
	eventWithArray, err := models.NewEventBuilder(
		"test.array.v1",                     // eventType
		"TEST-PRODUCER-GO",                  // eventSource
		"system",                            // createdBy
		uuid.New(),                          // tenantId
		uuid.New(),                          // sessionId
		uuid.New(),                          // requestId
		map[string]string{"version": "1.0"}, // metadata
		"abcd1234567890abcd1234567890abcd",  // md5Hash
	).WithPayload([]interface{}{
		"item1",
		"item2",
		map[string]interface{}{"nested": "value"},
		42,
	}).Build()

	if err != nil {
		log.Fatalf("Failed to create event: %v", err)
	}

	log.Println("Sending event with array payload")
	err = producer.SendEvent(context.Background(), *topic, eventWithArray)
	if err != nil {
		log.Printf("Failed to send event with array payload: %v", err)
	} else {
		log.Println("✅ Successfully sent event with array payload")
	}

	log.Println("Creating event with optional fields using builder pattern")
	// Create an event demonstrating all optional fields with chaining
	eventWithOptionalFields, err := models.NewEventBuilder(
		"test.complete.v1",                  // eventType
		"TEST-PRODUCER-GO",                  // eventSource
		"system",                            // createdBy
		uuid.New(),                          // tenantId
		uuid.New(),                          // sessionId
		uuid.New(),                          // requestId
		map[string]string{"version": "1.0"}, // metadata
		"abcd1234567890abcd1234567890abcd",  // md5Hash
	).WithMessage("This is a comprehensive event example").
		WithOwnerId("owner-12345").
		WithAffectedEntityUri("urn:example:resource:12345").
		WithEventSourceUri("https://my-service.example.com").
		WithPayloadUri("s3://my-bucket/payloads/payload-12345.json").
		WithContextUri("https://my-service.example.com/context/abc123").
		WithContext(map[string]interface{}{
			"trace_id":      "trace-abc-123",
			"span_id":       "span-xyz-789",
			"retry_count":   0,
			"execution_env": "production",
		}).WithPayload(map[string]interface{}{
		"action":      "create",
		"resource":    "document",
		"document_id": 12345,
		"size_bytes":  1048576,
	}).WithTags(map[string]string{
		"env":        "production",
		"region":     "us-west-2",
		"datacenter": "dc1",
		"team":       "platform",
	}).Build()

	if err != nil {
		log.Fatalf("Failed to create event: %v", err)
	}

	log.Println("Sending event with all optional fields set")
	err = producer.SendEvent(context.Background(), *topic, eventWithOptionalFields)
	if err != nil {
		log.Printf("Failed to send event with optional fields: %v", err)
	} else {
		log.Println("✅ Successfully sent event with all optional fields")
	}

	log.Println("Sending event with custom headers")
	// Send a new event with custom headers
	eventWithHeaders, err := models.NewEventBuilder(
		"test.object.v1",                    // eventType
		"TEST-PRODUCER-GO",                  // eventSource
		"system",                            // createdBy
		uuid.New(),                          // tenantId
		uuid.New(),                          // sessionId
		uuid.New(),                          // requestId
		map[string]string{"version": "1.0"}, // metadata
		"abcd1234567890abcd1234567890abcd",  // md5Hash
	).WithPayload(map[string]interface{}{"userId": 789, "email": "headers@example.com"}).Build()

	if err != nil {
		log.Fatalf("Failed to create event: %v", err)
	}

	headers := []dskafka.Header{
		{Key: "source", Value: "my-service"},
		{Key: "version", Value: "1.0"},
	}
	err = producer.SendEvent(context.Background(), *topic, eventWithHeaders, headers...)
	if err != nil {
		log.Printf("Failed to send event with headers: %v", err)
	} else {
		log.Println("✅ Successfully sent event with custom headers")
	}

	log.Println("Testing SafeSendEvent (fire-and-forget)")
	// Test SafeSendEvent - errors are logged but execution continues
	// Create another event for safe send
	eventForSafeSend, err := models.NewEventBuilder(
		"test.object.v1",                    // eventType
		"TEST-PRODUCER-GO",                  // eventSource
		"system",                            // createdBy
		uuid.New(),                          // tenantId
		uuid.New(),                          // sessionId
		uuid.New(),                          // requestId
		map[string]string{"version": "1.0"}, // metadata
		"abcd1234567890abcd1234567890abcd",  // md5Hash
	).WithPayload(map[string]interface{}{"userId": 456, "email": "another@example.com"}).Build()

	if err != nil {
		log.Fatalf("Failed to create event: %v", err)
	}

	producer.SafeSendEvent(context.Background(), *topic, eventForSafeSend)
	log.Println("✅ Successfully sent event with SafeSendEvent - any errors were logged automatically")

	log.Println("Done - sent 5 events total (object, array, complete with optional fields, object with headers, and safe send)")
}
