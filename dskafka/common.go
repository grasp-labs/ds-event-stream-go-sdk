package dskafka

import (
	"time"

	"github.com/segmentio/kafka-go"
)

// ClientCredentials holds the username and password for Kafka authentication.
type ClientCredentials struct {
	Username string
	Password string
}

//go:generate stringer -type=Environment
type Environment int

const (
	Dev Environment = iota
	Prod
)

// Header is a simple string header for Kafka messages.
type Header struct {
	Key   string
	Value string
}

// Config controls producer and consumer behavior. Tune as needed.
type Config struct {
	Brokers                []string // e.g. []string{"broker1:9092","broker2:9092"}
	ClientCredentials      ClientCredentials
	RequiredAcks           kafka.RequiredAcks
	Balancer               kafka.Balancer
	BatchSize              int
	BatchBytes             int64
	BatchTimeout           time.Duration
	Compression            kafka.Compression
	Async                  bool
	AllowAutoTopicCreation bool
	WriteTimeout           time.Duration // per-message write timeout

	// Consumer-specific fields (optional)
	GroupID        string        // Consumer group ID (optional, can be set per read operation)
	Partition      int           // Partition to read from (use -1 for all partitions)
	MinBytes       int           // Minimum number of bytes to fetch in each request
	MaxBytes       int           // Maximum number of bytes to fetch in each request
	MaxWait        time.Duration // Maximum amount of time to wait for messages
	ReadTimeout    time.Duration // per-message read timeout
	CommitInterval time.Duration // How often to commit offsets
	StartOffset    int64         // Where to start reading (kafka.FirstOffset or kafka.LastOffset)
}

// GetBootstrapServers returns the appropriate Kafka bootstrap servers for the specified environment.
// For Dev environment:
//   - External: ["b0.dev.kafka.ds.local:9095"]
//   - Internal: ["kafka-dev.kafka.svc.cluster.local:9092"]
//
// For Prod environment:
//   - External: ["b0.kafka.ds.local:9095", "b1.kafka.ds.local:9095", "b2.kafka.ds.local:9095"]
//   - Internal: ["kafka.kafka.svc.cluster.local:9092"]
//
// Parameters:
//   - environment: The target environment (Dev or Prod)
//   - useInternalHostnames: If true, returns internal cluster hostnames for in-cluster communication
func GetBootstrapServers(environment Environment, useInternalHostnames bool) []string {
	switch environment {
	case Prod:
		if useInternalHostnames {
			return []string{"kafka.kafka.svc.cluster.local:9092"}
		} else {
			return []string{"b0.kafka.ds.local:9095", "b1.kafka.ds.local:9095", "b2.kafka.ds.local:9095"}
		}
	default: // Dev
		if useInternalHostnames {
			return []string{"kafka.kafka-dev.svc.cluster.local:9092"}
		} else {
			return []string{"b0.dev.kafka.ds.local:9095"}
		}
	}
}