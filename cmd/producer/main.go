// Command producer is an example Kafka producer using go-kafka-sdk.
//
// Reads configuration from .env or environment variables:
//
//	KAFKA_BROKERS: list of brokers (e.g., localhost:9092)
//	KAFKA_TOPICS:  topic name (e.g., test-topic)
//
// Usage:
//
//	go run ./cmd/producer
//
// The program sends a test message to the specified topic.
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
	topic := "test-topic"

	p, err := kafka.NewProducer(brokers, nil)
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to create producer: %v\n", err)
		os.Exit(1)
	}
	defer p.Close()

	kafka.StartDeliveryHandler(ctx, p, func(m *ckafka.Message) {
		if m.TopicPartition.Error != nil {
			fmt.Printf("delivery failed: %v\n", m.TopicPartition.Error)
			return
		}
		fmt.Printf("delivered to %v\n", m.TopicPartition)
	})

	msg := []byte("hello from go-kafka-sdk")
	if err := kafka.Produce(ctx, p, topic, msg); err != nil {
		fmt.Fprintf(os.Stderr, "produce error: %v\n", err)
	}

	// Aguarda até 5 segundos para entregar mensagens pendentes
	p.Flush(5000)
}