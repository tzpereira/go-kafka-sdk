package kafka

import (
	"context"
	"os"
	"testing"
	"time"

	ckafka "github.com/confluentinc/confluent-kafka-go/kafka"
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

	// Produce a message
	producer, err := NewProducer(brokers, nil)
	if err != nil {
		t.Fatalf("failed to create producer: %v", err)
	}
	defer producer.Close()

	testValue := []byte("test message")
	err = Produce(context.Background(), producer, topic, testValue)
	if err != nil {
		t.Fatalf("failed to produce message: %v", err)
	}
	producer.Flush(5000)

	// Consume the message
	consumer, err := NewConsumer(brokers, groupID, []string{topic}, nil)
	if err != nil {
		t.Fatalf("failed to create consumer: %v", err)
	}
	defer consumer.Close()

	found := false
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	err = Consume(ctx, consumer, func(msg *ckafka.Message) {
		if string(msg.Value) == string(testValue) {
			found = true
			cancel()
		}
	})
	if err != nil && err != context.Canceled && err != context.DeadlineExceeded {
		t.Fatalf("consumer error: %v", err)
	}
	if !found {
		t.Errorf("consumer did not receive the produced message")
	}
}
