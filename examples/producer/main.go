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

	log.Println("Creating event")
	// Create an event
	event := models.EventJson{
		Id:          uuid.New(),
		SessionId:   uuid.New(),
		RequestId:   uuid.New(),
		TenantId:    uuid.New(),
		EventType:   "test.test.v1",
		EventSource: "TEST-PRODUCER-GO",
		CreatedBy:   "system",
		Md5Hash:     "abcd1234567890abcd1234567890abcd",
		Metadata:    map[string]string{"version": "1.0"},
		Timestamp:   time.Now(),
		Payload:     &map[string]interface{}{"userId": 123, "email": "user@example.com"},
	}

	log.Println("Sending event")
	// Send single event
	err = producer.SendEvent(context.Background(), *topic, event)
	if err != nil {
		log.Printf("Failed to send event: %v", err)
	}

	log.Println("Sending event with custom headers")
	// Send with custom headers
	headers := []dskafka.Header{
		{Key: "source", Value: "my-service"},
		{Key: "version", Value: "1.0"},
	}
	event.Id = uuid.New() // new ID for the new event
	err = producer.SendEvent(context.Background(), *topic, event, headers...)
	if err != nil {
		log.Printf("Failed to send event with headers: %v", err)
	}

	log.Println("Testing SafeSendEvent (fire-and-forget)")
	// Test SafeSendEvent - errors are logged but execution continues
	event.Id = uuid.New() // new ID for the safe send test
	producer.SafeSendEvent(context.Background(), "ds.workflow.pipeline.job.requested.v1", event)
	log.Println("SafeSendEvent completed - any errors were logged automatically")

	log.Println("Done")
}
