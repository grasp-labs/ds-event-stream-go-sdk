package main

import (
	"context"
	"flag"
	"log"
	"strings"
	"time"

	"github.com/grasp-labs/ds-event-stream-go-sdk/dskafka"
	"github.com/grasp-labs/ds-event-stream-go-sdk/models"
	"github.com/segmentio/kafka-go"
)

// Simple consumer example that loops until it finds at least one message
// Usage: go run main.go -password=supersecret
// Usage: go run main.go -username=myuser -password=supersecret
func main() {
	// Command line arguments
	username := flag.String("username", "ds.consumption.ingress.v1", "Kafka username")
	password := flag.String("password", "", "Kafka password (required)")
	groupID := flag.String("group", "example-consumer-group", "Consumer group ID")
	topic := flag.String("topic", "ds.workflow.pipeline.job.requested.v1", "Topic to consume from")
	fromEnd := flag.Bool("from-end", false, "Start reading from the end (latest) for new consumer groups without committed offsets")
	timeout := flag.Duration("timeout", 30*time.Second, "Total timeout for finding a message")
	maxAttempts := flag.Int("max-attempts", 10, "Maximum number of read attempts")
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

	// Override start offset if requested
	if *fromEnd {
		config.StartOffset = kafka.LastOffset
		log.Printf("Configured to start from LATEST offset")
	} else {
		log.Printf("Configured to start from FIRST offset")
	}

	// Add debugging information about the config
	log.Printf("Consumer config - StartOffset: %d, GroupID: '%s', Partition: %d", config.StartOffset, config.GroupID, config.Partition)
	log.Printf("Consumer config - MaxBytes: %d, MaxWait: %v", config.MaxBytes, config.MaxWait)

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

	// Create context with total timeout
	ctx, cancel := context.WithTimeout(context.Background(), *timeout)
	defer cancel()

	// Loop until we find a message or reach max attempts
	log.Printf("Looking for messages on topic '%s'...", *topic)
	log.Printf("Will try up to %d attempts within %v total timeout", *maxAttempts, *timeout)

	for attempt := 1; attempt <= *maxAttempts; attempt++ {
		// Create a shorter context for each individual read attempt
		readCtx, readCancel := context.WithTimeout(ctx, 5*time.Second)

		log.Printf("📡 Attempt %d/%d: Reading from topic '%s'...", attempt, *maxAttempts, *topic)

		event, msg, err := consumer.ReadEventWithMessage(readCtx, *topic)
		readCancel() // Clean up the read context

		if err != nil {
			// Check if the overall timeout has been exceeded
			if ctx.Err() == context.DeadlineExceeded {
				log.Printf("⏰ Overall timeout of %v exceeded after %d attempts", *timeout, attempt)
				return
			}

			// Handle timeout for individual read (expected when no messages)
			if readCtx.Err() == context.DeadlineExceeded {
				log.Printf("   ⏳ No message found in this attempt (timeout after 5s)")
				if attempt < *maxAttempts {
					log.Printf("   🔄 Retrying in 1 second...")
					time.Sleep(1 * time.Second)
				}
				continue
			}

			// Handle errors - some can be retried, others are fatal
			errorMsg := err.Error()

			// EOF errors during retries might be transient - try to continue
			if strings.Contains(errorMsg, "EOF") && attempt < *maxAttempts {
				log.Printf("   ⚠️  Connection issue (EOF) on attempt %d, will retry: %v", attempt, err)
				log.Printf("   🔄 Retrying in 2 seconds...")
				time.Sleep(2 * time.Second)
				continue
			} else if strings.Contains(errorMsg, "EOF") {
				log.Printf("❌ Persistent Connection Error (EOF): %v", err)
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

		// Success! We found a message
		if event != nil {
			log.Printf("🎉 SUCCESS! Found message after %d attempt(s):", attempt)
			printEventDetails(event)

			// Commit the message to acknowledge successful processing
			log.Printf("💾 Committing message offset...")
			commitCtx, commitCancel := context.WithTimeout(context.Background(), 5*time.Second)
			err := consumer.CommitEvent(commitCtx, msg)
			commitCancel()

			if err != nil {
				log.Printf("⚠️  Failed to commit message: %v", err)
			} else {
				log.Printf("✅ Message committed successfully")
			}

			log.Println("✅ Consumer example completed successfully")
			return
		}
	}

	// If we get here, we exhausted all attempts without finding a message
	log.Printf("🚫 No messages found after %d attempts within %v timeout", *maxAttempts, *timeout)
	log.Println("💡 This might mean:")
	log.Println("   • The topic exists but has no messages")
	log.Println("   • All messages are older than your consumer group's committed offset")
	log.Println("   • You might want to try with a different topic or reset your consumer group")
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
