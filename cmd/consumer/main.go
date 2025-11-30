// Command consumer is an example Kafka consumer using go-kafka-sdk.
//
// Reads configuration from .env or environment variables:
//
//	KAFKA_BROKERS: list of brokers (e.g., localhost:9092)
//	KAFKA_TOPICS:  topic name (e.g., test-topic)
//	KAFKA_GROUP_ID: consumer group name (e.g., test-group)
//
// Usage:
//
//	go run ./cmd/consumer
//
// The program consumes messages from the specified topic and prints them to the terminal.
package main

import (
	"context"
	"fmt"
	"go_kafka_sdk/internal/kafka"
	"os"
	"os/signal"
	"syscall"

	_ "github.com/joho/godotenv/autoload"

	ckafka "github.com/confluentinc/confluent-kafka-go/kafka"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	brokers := []string{"localhost:9092"}
	topics := []string{"test-topic"}
	groupID := "test-group"

	c, err := kafka.NewConsumer(brokers, groupID, topics, nil)
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to create consumer: %v\n", err)
		os.Exit(1)
	}
	defer c.Close()

	kafka.Consume(ctx, c, func(msg *ckafka.Message) {
		fmt.Printf("topic:%s partition:%d offset:%v key:%s value:%s\n",
			*msg.TopicPartition.Topic, msg.TopicPartition.Partition, msg.TopicPartition.Offset, string(msg.Key), string(msg.Value))
	})
}