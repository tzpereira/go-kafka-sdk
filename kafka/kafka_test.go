package kafka

import (
	"context"
	"os"
	"testing"
	"time"
)

func TestNewConfigFromEnv(t *testing.T) {
	os.Setenv("KAFKA_BROKERS", "localhost:9092,localhost:9093")
	os.Setenv("KAFKA_GROUP_ID", "test-group")
	os.Setenv("KAFKA_TOPICS", "topic1,topic2")

	cfg := NewConfigFromEnv()
	if len(cfg.Brokers) != 2 || cfg.Brokers[0] != "localhost:9092" {
		t.Errorf("expected parsed brokers, got: %v", cfg.Brokers)
	}
	if cfg.GroupID != "test-group" {
		t.Errorf("expected group id 'test-group', got: %s", cfg.GroupID)
	}
	if len(cfg.Topics) != 2 || cfg.Topics[1] != "topic2" {
		t.Errorf("expected parsed topics, got: %v", cfg.Topics)
	}
}

func TestProducerConsumerIntegration(t *testing.T) {
	brokers := []string{"localhost:9092"}
	topic := "test-integration-topic"
	groupID := "test-integration-group"
	testValue := []byte("test message")

	// --- Step 1: Produce a message ---
	producer, err := NewProducer(brokers, nil)
	if err != nil {
		t.Fatalf("Failed to create producer: %v", err)
	}
	defer producer.Close()

	msg := &Message{
		Topic: topic,
		Value: testValue,
	}
	if err := producer.Produce(context.Background(), msg); err != nil {
		t.Fatalf("Failed to produce message: %v", err)
	}
	producer.Flush(5000)

	// --- Step 2: Consume the message ---
	consumer, err := NewConsumer(brokers, groupID, []string{topic}, nil)
	if err != nil {
		t.Fatalf("Failed to create consumer: %v", err)
	}
	defer consumer.Close()

	found := false
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	consumeErr := consumer.Consume(ctx, func(received *Message) {
		// Log every message received for debugging
		t.Logf("Consumed message: topic=%s partition=%d value=%s", received.Topic, received.Partition, string(received.Value))
		if string(received.Value) == string(testValue) {
			found = true
			cancel() // Stop consuming once found
		}
	})

	if consumeErr != nil && consumeErr != context.Canceled && consumeErr != context.DeadlineExceeded {
		t.Fatalf("Consumer error: %v", consumeErr)
	}
	if !found {
		t.Errorf("Consumer did not receive the produced message")
	}
}
