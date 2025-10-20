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
	log.Println("Fetching password from command line arguments")
	password := flag.String("password", "", "Kafka password")
	flag.Parse()

	log.Println("Setting up credentials")
	// Setup credentials
	credentials := dskafka.ClientCredentials{
		Username: "ds.consumption.ingress.v1",
		Password: *password,
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
	err = producer.SendEvent(context.Background(), "ds.workflow.pipeline.job.requested.v1", event)
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
	err = producer.SendEvent(context.Background(), "ds.workflow.pipeline.job.requested.v1", event, headers...)
	if err != nil {
		log.Printf("Failed to send event with headers: %v", err)
	}
	log.Println("Done")
}
