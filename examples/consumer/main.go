package main

import (
	"context"
	"flag"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/ssm"
	"github.com/grasp-labs/ds-event-stream-go-sdk/v2/dskafka"
	"github.com/grasp-labs/ds-event-stream-go-sdk/v2/models"
	"github.com/grasp-labs/ds-go-kit/x/log"
	"github.com/segmentio/kafka-go"
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

// Simple consumer example that loops until it finds at least one message
// Usage: go run main.go -password=supersecret
// Usage: go run main.go -username=myuser -password=supersecret
// Usage: go run main.go -use-ssm (gets password from SSM)
func main() {
	ctx := context.Background()

	// Command line arguments
	username := flag.String("username", "ds.test.consumer.v1", "Kafka username")
	password := flag.String("password", "", "Kafka password (optional if using SSM)")
	useSSM := flag.Bool("use-ssm", false, "Get password from AWS SSM Parameter Store")
	groupID := flag.String("group", "example-consumer-group", "Consumer group ID")
	topic := flag.String("topic", "ds.workflow.pipeline.job.requested.v1", "Topic to consume from")
	fromEnd := flag.Bool("from-end", false, "Start reading from the end (latest) for new consumer groups without committed offsets")
	timeout := flag.Duration("timeout", 30*time.Second, "Total timeout for finding a message")
	maxAttempts := flag.Int("max-attempts", 10, "Maximum number of read attempts")
	flag.Parse()

	var actualPassword string
	var err error

	if *useSSM {
		log.Info(ctx, "Fetching password from AWS SSM Parameter Store")
		// Construct SSM parameter name based on username
		parameterName := "/ds/kafka/dev/principals/" + *username
		log.Info(ctx, "Getting password from SSM parameter: %s", parameterName)

		ssmCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
		defer cancel()

		actualPassword, err = getPasswordFromSSM(ssmCtx, parameterName)
		if err != nil {
			log.StackError(ctx, "Failed to get password from SSM: %v", err)
			return
		}
		log.Info(ctx, "Successfully retrieved password from SSM")
	} else if *password != "" {
		log.Info(ctx, "Using password from command line argument")
		actualPassword = *password
	} else {
		log.Error(ctx, "Password is required. Use -password=your-kafka-password or -use-ssm=true")
		return
	}

	log.Info(ctx, "Starting simple consumer example...")
	log.Info(ctx, "Username: %s", *username)
	log.Info(ctx, "Group ID: %s", *groupID)
	log.Info(ctx, "Topic: %s", *topic)

	// Setup credentials
	credentials := dskafka.ClientCredentials{
		Username: *username,
		Password: actualPassword,
	}

	// Get bootstrap servers for dev environment
	bootstrapServers := dskafka.GetBootstrapServers(dskafka.Dev, false)
	log.Info(ctx, "Bootstrap servers: %v", bootstrapServers)

	// Create consumer configuration
	config := dskafka.DefaultConsumerConfig(credentials, bootstrapServers, *groupID)

	// Override start offset if requested
	if *fromEnd {
		config.StartOffset = kafka.LastOffset
		log.Info(ctx, "Configured to start from LATEST offset")
	} else {
		log.Info(ctx, "Configured to start from FIRST offset")
	}

	// Add debugging information about the config
	log.Info(ctx, "Consumer config - StartOffset: %d, GroupID: '%s', Partition: %d", config.StartOffset, config.GroupID, config.Partition)
	log.Info(ctx, "Consumer config - MaxBytes: %d, MaxWait: %v", config.MaxBytes, config.MaxWait)

	// Create consumer
	consumer, err := dskafka.NewConsumer(config)
	if err != nil {
		log.StackError(ctx, "Failed to create consumer: %v", err)
		return
	}
	defer func() {
		if err := consumer.Close(); err != nil {
			log.Error(ctx, "Failed to close consumer: %v", err)
		}
	}()

	// Create context with total timeout
	timeoutCtx, cancel := context.WithTimeout(ctx, *timeout)
	defer cancel()

	// Loop until we find a message or reach max attempts
	log.Info(ctx, "Looking for messages on topic '%s'...", *topic)
	log.Info(ctx, "Will try up to %d attempts within %v total timeout", *maxAttempts, *timeout)

	for attempt := 1; attempt <= *maxAttempts; attempt++ {
		// Create a shorter context for each individual read attempt
		readCtx, readCancel := context.WithTimeout(timeoutCtx, 5*time.Second)

		log.Info(ctx, "📡 Attempt %d/%d: Reading from topic '%s'...", attempt, *maxAttempts, *topic)

		event, msg, err := consumer.ReadEventWithMessage(readCtx, *topic)
		readCancel() // Clean up the read context

		if err != nil {
			// Check if the overall timeout has been exceeded
			if timeoutCtx.Err() == context.DeadlineExceeded {
				log.Warning(ctx, "⏰ Overall timeout of %v exceeded after %d attempts", *timeout, attempt)
				return
			}

			// Handle timeout for individual read (expected when no messages)
			if readCtx.Err() == context.DeadlineExceeded {
				log.Info(ctx, "   ⏳ No message found in this attempt (timeout after 5s)")
				if attempt < *maxAttempts {
					log.Info(ctx, "   🔄 Retrying in 1 second...")
					time.Sleep(1 * time.Second)
				}
				continue
			}

			// Handle errors - some can be retried, others are fatal
			errorMsg := err.Error()

			// EOF errors during retries might be transient - try to continue
			if strings.Contains(errorMsg, "EOF") && attempt < *maxAttempts {
				log.Warning(ctx, "   ⚠️  Connection issue (EOF) on attempt %d, will retry: %v", attempt, err)
				log.Info(ctx, "   🔄 Retrying in 2 seconds...")
				time.Sleep(2 * time.Second)
				continue
			} else if strings.Contains(errorMsg, "EOF") {
				log.Error(ctx, "❌ Persistent Connection Error (EOF): %v", err)
				log.Info(ctx, "")
				log.Info(ctx, "🔧 This usually means:")
				log.Info(ctx, "   • Kafka brokers are not running or accessible")
				log.Info(ctx, "   • Network connectivity issues")
				log.Info(ctx, "   • Wrong broker addresses in configuration")
				log.Info(ctx, "   • Firewall blocking the connection")
				log.Info(ctx, "")
				log.Info(ctx, "💡 Try:")
				log.Info(ctx, "   • Check if Kafka cluster is running")
				log.Info(ctx, "   • Verify network connectivity to brokers")
				log.Info(ctx, "   • Check firewall and security group settings")
			} else if strings.Contains(errorMsg, "no such host") {
				log.Error(ctx, "❌ DNS/Host Error: %v", err)
				log.Info(ctx, "")
				log.Info(ctx, "🔧 This means the broker hostnames cannot be resolved")
				log.Info(ctx, "💡 Check the broker addresses in your configuration")
			} else if strings.Contains(errorMsg, "connection refused") {
				log.Error(ctx, "❌ Connection Refused: %v", err)
				log.Info(ctx, "")
				log.Info(ctx, "🔧 This means the brokers are not accepting connections")
				log.Info(ctx, "💡 Check if Kafka is running on the specified ports")
			} else if strings.Contains(errorMsg, "authentication") || strings.Contains(errorMsg, "sasl") {
				log.Error(ctx, "❌ Authentication Error: %v", err)
				log.Info(ctx, "")
				log.Info(ctx, "🔧 Check your username and password")
			} else {
				log.Error(ctx, "❌ Kafka Error: %v", err)
			}
			return
		}

		// Success! We found a message
		if event != nil {
			log.Info(ctx, "🎉 SUCCESS! Found message after %d attempt(s):", attempt)
			printEventDetails(ctx, event)

			// Commit the message to acknowledge successful processing
			log.Info(ctx, "💾 Committing message offset...")
			commitCtx, commitCancel := context.WithTimeout(context.Background(), 5*time.Second)
			err := consumer.CommitEvent(commitCtx, msg)
			commitCancel()

			if err != nil {
				log.Warning(ctx, "⚠️  Failed to commit message: %v", err)
			} else {
				log.Info(ctx, "✅ Message committed successfully")
			}

			log.Info(ctx, "✅ Consumer example completed successfully")
			return
		}
	}

	// If we get here, we exhausted all attempts without finding a message
	log.Warning(ctx, "🚫 No messages found after %d attempts within %v timeout", *maxAttempts, *timeout)
	log.Info(ctx, "💡 This might mean:")
	log.Info(ctx, "   • The topic exists but has no messages")
	log.Info(ctx, "   • All messages are older than your consumer group's committed offset")
	log.Info(ctx, "   • You might want to try with a different topic or reset your consumer group")
}

// printEventDetails prints formatted event information
func printEventDetails(ctx context.Context, event *models.EventJson) {
	log.Info(ctx, "  🆔 ID: %s", event.Id)
	log.Info(ctx, "  📋 Type: %s", event.EventType)
	log.Info(ctx, "  🏭 Source: %s", event.EventSource)
	log.Info(ctx, "  👤 Created By: %s", event.CreatedBy)
	log.Info(ctx, "  🕐 Timestamp: %s", event.Timestamp.Format(time.RFC3339))

	if event.Message != nil {
		log.Info(ctx, "  💬 Message: %s", *event.Message)
	}

	if event.Payload != nil {
		log.Info(ctx, "  📦 Payload: %+v", event.Payload)
	}
}
