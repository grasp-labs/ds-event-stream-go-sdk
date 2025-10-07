package main

import (
	"context"
	"flag"
	"log"
	"strings"
	"time"

	"github.com/grasp-labs/ds-event-stream-go-sdk/dskafka"
	"github.com/grasp-labs/ds-event-stream-go-sdk/models"
)

// Simple consumer example that reads one event and exits
// Usage: go run main.go -password=supersecret
// Usage: go run main.go -username=myuser -password=supersecret
func main() {
	// Command line arguments
	username := flag.String("username", "ds.consumption.ingress.v1", "Kafka username")
	password := flag.String("password", "", "Kafka password (required)")
	groupID := flag.String("group", "example-consumer-group", "Consumer group ID")
	topic := flag.String("topic", "ds.workflow.pipeline.job.requested.v1", "Topic to consume from")
	timeout := flag.Duration("timeout", 30*time.Second, "Timeout for reading message")
	flag.Parse()

	if *password == "" {
		log.Fatal("Password is required. Use -password=your-kafka-password")
	}

	log.Printf("Starting simple consumer example...")
	log.Printf("Username: %s", *username)
	log.Printf("Group ID: %s", *groupID)
	log.Printf("Topic: %s", *topic)

	// Setup credentials
	credentials := dskafka.ClientCredentials{
		Username: *username,
		Password: *password,
	}

	// Get bootstrap servers for dev environment
	bootstrapServers := dskafka.GetBootstrapServers(dskafka.Dev, false)
	log.Printf("Bootstrap servers: %v", bootstrapServers)

	// Create consumer configuration
	config := dskafka.DefaultConsumerConfig(credentials, bootstrapServers, *groupID)

	// Create consumer
	consumer, err := dskafka.NewConsumer(config)
	if err != nil {
		log.Fatal("Failed to create consumer:", err)
	}
	defer func() {
		if err := consumer.Close(); err != nil {
			log.Printf("Failed to close consumer: %v", err)
		}
	}()

	// Create context with timeout
	ctx, cancel := context.WithTimeout(context.Background(), *timeout)
	defer cancel()

	// Read a single event
	log.Printf("Reading one event from topic '%s' with %v timeout...", *topic, *timeout)
	event, err := consumer.ReadEvent(ctx, *topic)
	if err != nil {
		if ctx.Err() == context.DeadlineExceeded {
			log.Printf("No message received within %v timeout", *timeout)
			return
		}

		// Provide helpful error explanations
		errorMsg := err.Error()
		if strings.Contains(errorMsg, "EOF") {
			log.Printf("❌ Connection Error (EOF): %v", err)
			log.Println("")
			log.Println("🔧 This usually means:")
			log.Println("   • Kafka brokers are not running or accessible")
			log.Println("   • Network connectivity issues")
			log.Println("   • Wrong broker addresses in configuration")
			log.Println("   • Firewall blocking the connection")
			log.Println("")
			log.Println("💡 Try:")
			log.Println("   • Check if Kafka cluster is running")
			log.Println("   • Verify network connectivity to brokers")
			log.Println("   • Check firewall and security group settings")
		} else if strings.Contains(errorMsg, "no such host") {
			log.Printf("❌ DNS/Host Error: %v", err)
			log.Println("")
			log.Println("🔧 This means the broker hostnames cannot be resolved")
			log.Println("💡 Check the broker addresses in your configuration")
		} else if strings.Contains(errorMsg, "connection refused") {
			log.Printf("❌ Connection Refused: %v", err)
			log.Println("")
			log.Println("🔧 This means the brokers are not accepting connections")
			log.Println("💡 Check if Kafka is running on the specified ports")
		} else if strings.Contains(errorMsg, "authentication") || strings.Contains(errorMsg, "sasl") {
			log.Printf("❌ Authentication Error: %v", err)
			log.Println("")
			log.Println("🔧 Check your username and password")
		} else {
			log.Printf("❌ Kafka Error: %v", err)
		}
		return
	}

	if event != nil {
		log.Println("📨 Received event:")
		printEventDetails(event)
	} else {
		log.Println("No event received")
	}

	log.Println("✅ Consumer example completed successfully")
}

// printEventDetails prints formatted event information
func printEventDetails(event *models.EventJson) {
	log.Printf("  🆔 ID: %s", event.Id)
	log.Printf("  📋 Type: %s", event.EventType)
	log.Printf("  🏭 Source: %s", event.EventSource)
	log.Printf("  👤 Created By: %s", event.CreatedBy)
	log.Printf("  🕐 Timestamp: %s", event.Timestamp.Format(time.RFC3339))

	if event.Message != nil {
		log.Printf("  💬 Message: %s", *event.Message)
	}

	if event.Payload != nil {
		log.Printf("  📦 Payload: %+v", *event.Payload)
	}
}
