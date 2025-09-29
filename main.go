package main

import (
	"context"
	"flag"
	"log"
	"time"

	"github.com/google/uuid"
	"github.com/grasp-labs/ds-event-stream-go-sdk/dskafka"
	"github.com/grasp-labs/ds-event-stream-go-sdk/models"
)

// pass password as argument
// go run main.go -password=supersecret
func main() {
	// Get password from command line arguments
	password := flag.String("password", "", "Kafka password")
	flag.Parse()

	// Setup credentials
	credentials := dskafka.ClientCredentials{
		Username: "ds.consumption.ingress.v1",
		Password: *password,
	}

	// Get bootstrap servers for your environment
	bootstrapServers := dskafka.GetBootstrapServers(dskafka.Dev, false) // or dskafka.Dev

	// Create producer configuration
	config := dskafka.DefaultProducerConfig(credentials, bootstrapServers)

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
	err = producer.SendEvent(context.Background(), "ds.workflow.pipeline.job.requested.v1", event)
	if err != nil {
		log.Printf("Failed to send event: %v", err)
	}

	// Send with custom headers
	headers := []dskafka.Header{
		{Key: "source", Value: "my-service"},
		{Key: "version", Value: "1.0"},
	}
	err = producer.SendEvent(context.Background(), "ds.workflow.pipeline.job.requested.v1", event, headers...)
	if err != nil {
		log.Printf("Failed to send event with headers: %v", err)
	}
}
