package dskafka

import (
	"context"
	"encoding/json"
	"time"

	"github.com/google/uuid"
	"github.com/grasp-labs/ds-event-stream-go-sdk/v2/models"
	"github.com/segmentio/kafka-go"
)

// mockWriter implements kafkaWriter for testing
type mockWriter struct {
	messages []kafka.Message
	writeErr error
	closeErr error
}

func newMockWriter() *mockWriter {
	return &mockWriter{
		messages: make([]kafka.Message, 0),
	}
}

func (m *mockWriter) WriteMessages(ctx context.Context, msgs ...kafka.Message) error {
	if m.writeErr != nil {
		return m.writeErr
	}
	m.messages = append(m.messages, msgs...)
	return nil
}

func (m *mockWriter) Close() error {
	return m.closeErr
}

// mockReader implements kafkaReader for testing
type mockReader struct {
	messages   []kafka.Message
	messageIdx int
	readErr    error
	commitErr  error
	closeErr   error
	stats      kafka.ReaderStats
}

func newMockReader(messages ...kafka.Message) *mockReader {
	return &mockReader{
		messages:   messages,
		messageIdx: 0,
		stats:      kafka.ReaderStats{},
	}
}

func (m *mockReader) ReadMessage(ctx context.Context) (kafka.Message, error) {
	if m.readErr != nil {
		return kafka.Message{}, m.readErr
	}
	if m.messageIdx >= len(m.messages) {
		return kafka.Message{}, context.DeadlineExceeded
	}
	msg := m.messages[m.messageIdx]
	m.messageIdx++
	return msg, nil
}

func (m *mockReader) CommitMessages(ctx context.Context, msgs ...kafka.Message) error {
	return m.commitErr
}

func (m *mockReader) Stats() kafka.ReaderStats {
	return m.stats
}

func (m *mockReader) Close() error {
	return m.closeErr
}

// createMockEvent creates a realistic mock EventJson for testing
func createMockEvent() *models.EventJson {
	now := time.Now().UTC()
	return &models.EventJson{
		Id:          uuid.New(),
		SessionId:   uuid.New(),
		RequestId:   uuid.New(),
		TenantId:    uuid.New(),
		EventType:   "mock.event.test.v1",
		EventSource: "mock-service",
		CreatedBy:   "mock-test",
		Metadata:    map[string]string{"mock": "true"},
		Timestamp:   now,
	}
}

// createMockKafkaMessage creates a mock kafka.Message for testing
func createMockKafkaMessage(topic string, event *models.EventJson) kafka.Message {
	jsonBytes, _ := json.Marshal(event)
	return kafka.Message{
		Topic:     topic,
		Partition: 0,
		Offset:    1,
		Key:       []byte(event.Id.String()),
		Value:     jsonBytes,
		Time:      event.Timestamp,
	}
}
