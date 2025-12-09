package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	kafkasdk "github.com/tzpereira/go-kafka-sdk/kafka"

	ckafka "github.com/confluentinc/confluent-kafka-go/kafka"
	_ "github.com/joho/godotenv/autoload"
)

func main() {
	// Ctrl+C cancel
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	// Load config
	cfg := kafkasdk.NewConfigFromEnv()

	if len(cfg.Topics) == 0 {
		fmt.Println("no topics defined via KAFKA_TOPICS")
		os.Exit(1)
	}
	topic := cfg.Topics[0]

	// Producer
	producer, err := kafkasdk.NewProducer(cfg.Brokers, nil)
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to create producer: %v\n", err)
		os.Exit(1)
	}
	defer producer.Close()

	// Delivery report
	kafkasdk.StartDeliveryHandler(ctx, producer, func(m *ckafka.Message) {
		if m.TopicPartition.Error != nil {
			fmt.Printf("delivery failed: %v\n", m.TopicPartition.Error)
			return
		}
		fmt.Printf("delivered to %v\n", m.TopicPartition)
	})

	// Publish test message
	payload := []byte("hello from go-kafka-sdk")
	fmt.Printf("Producing to topic '%s'...\n", topic)

	if err := kafkasdk.Produce(ctx, producer, topic, payload); err != nil {
		fmt.Fprintf(os.Stderr, "produce error: %v\n", err)
	}

	producer.Flush(5000)
}
