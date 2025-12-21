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

	testKey := []byte("test-key")
	testValue := []byte("test message")

	// --- Step 1: Produce ---
	producer, err := NewProducer(brokers, nil)
	if err != nil {
		t.Fatalf("failed to create producer: %v", err)
	}
	defer producer.Close()

	err = producer.Produce(context.Background(), &Message{
		Topic: topic,
		Key:   testKey,
		Value: testValue,
	})
	if err != nil {
		t.Fatalf("failed to produce message: %v", err)
	}

	producer.Flush(5000)

	// --- Step 2: Consume ---
	consumer, err := NewConsumer(brokers, groupID, []string{topic}, nil)
	if err != nil {
		t.Fatalf("failed to create consumer: %v", err)
	}
	defer consumer.Close()

	var received *Message

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	err = consumer.Consume(ctx, func(msg *Message) {
		t.Logf(
			"Consumed message | topic=%s partition=%d offset=%d key=%s value=%s timestamp=%d",
			msg.Topic,
			msg.Partition,
			msg.Offset,
			string(msg.Key),
			string(msg.Value),
			msg.Timestamp,
		)

		if string(msg.Value) == string(testValue) {
			received = msg
			cancel()
		}
	})

	if err != nil && err != context.Canceled && err != context.DeadlineExceeded {
		t.Fatalf("consumer error: %v", err)
	}

	if received == nil {
		t.Fatal("consumer did not receive the produced message")
	}

	// --- Assertions ---
	if received.Topic != topic {
		t.Errorf("expected topic %s, got %s", topic, received.Topic)
	}

	if received.Offset < 0 {
		t.Errorf("expected offset >= 0, got %d", received.Offset)
	}

	if received.Timestamp <= 0 {
		t.Errorf("expected valid timestamp, got %d", received.Timestamp)
	}

	// sanity check: recent timestamp
	now := time.Now().UnixMilli()
	if diff := now - received.Timestamp; diff < 0 || diff > 10_000 {
		t.Errorf("unexpected timestamp drift: now=%d msg=%d diff=%dms", now, received.Timestamp, diff)
	}

	if string(received.Key) != string(testKey) {
		t.Errorf("expected key %s, got %s", testKey, received.Key)
	}

	if string(received.Value) != string(testValue) {
		t.Errorf("expected value %s, got %s", testValue, received.Value)
	}
}
