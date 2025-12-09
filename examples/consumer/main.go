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

	// Load config from env
	cfg := kafkasdk.NewConfigFromEnv()

	consumer, err := kafkasdk.NewConsumer(cfg.Brokers, cfg.GroupID, cfg.Topics, nil)
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to create consumer: %v\n", err)
		os.Exit(1)
	}
	defer consumer.Close()

	fmt.Printf("Consuming from %v (group=%s)\n", cfg.Topics, cfg.GroupID)

	kafkasdk.Consume(ctx, consumer, func(msg *ckafka.Message) {
		fmt.Printf("topic=%s partition=%d offset=%v key=%s value=%s\n",
			*msg.TopicPartition.Topic,
			msg.TopicPartition.Partition,
			msg.TopicPartition.Offset,
			string(msg.Key),
			string(msg.Value),
		)
	})
}
