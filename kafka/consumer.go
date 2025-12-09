package kafka

import (
	"context"
	"log"
	"strings"
	"sync"

	ckafka "github.com/confluentinc/confluent-kafka-go/kafka"
)

// BlockForever is a constant used to indicate that the consumer should block indefinitely when reading messages.
const BlockForever = -1

// NewConsumer creates and subscribes a Kafka consumer using the provided brokers, groupID, topics, and config.
// Returns a configured *ckafka.Consumer or an error if creation or subscription fails.
func NewConsumer(brokers []string, groupID string, topics []string, config map[string]string) (*ckafka.Consumer, error) {
	conf := &ckafka.ConfigMap{
		"bootstrap.servers": strings.Join(brokers, ","),
		"group.id":          groupID,
		"auto.offset.reset": "earliest",
	}
	for k, v := range config {
		(*conf)[k] = v
	}
	consumer, err := ckafka.NewConsumer(conf)
	if err != nil {
		return nil, err
	}
	err = consumer.SubscribeTopics(topics, nil)
	if err != nil {
		return nil, err
	}
	return consumer, nil
}

// StartConsumers starts multiple Kafka consumers in separate goroutines.
// Each consumer is created using the newConsumer function and processes messages using the provided handler.
// The function blocks until all consumers have exited.
// Returns an error if any consumer fails to start.
func StartConsumers(ctx context.Context, num int, newConsumer func() (*ckafka.Consumer, error), handler func(msg *ckafka.Message)) error {
    var wg sync.WaitGroup
    for i := 0; i < num; i++ {
        wg.Add(1)
        go func() {
            defer wg.Done()
            consumer, err := newConsumer()
            if err != nil {
                log.Printf("Failed to create consumer: %v", err)
                return
            }
            defer consumer.Close()
            _ = Consume(ctx, consumer, handler)
        }()
    }
    wg.Wait()
    return nil
}

// Consume reads messages from the Kafka consumer and calls the handler for each message.
// It blocks until the context is cancelled or an error occurs.
// Returns ctx.Err() if the context is cancelled, or the error from consumer.ReadMessage if message consumption fails.
func Consume(ctx context.Context, consumer *ckafka.Consumer, handler func(msg *ckafka.Message)) error {
    for {
        select {
        case <-ctx.Done():
            return ctx.Err()
        default:
            ev := consumer.Poll(100) // 100 ms timeout
            if ev == nil {
                continue
            }
            switch e := ev.(type) {
            case *ckafka.Message:
                handler(e)
            case ckafka.Error:
                log.Printf("Consumer error: %v", e)
            }
        }
    }
}

// ConsumeBatch reads messages from the Kafka consumer in batches and calls the handler with a slice of messages.
// It blocks until the context is cancelled or an error occurs.
// Returns ctx.Err() if the context is cancelled.
func ConsumeBatch(ctx context.Context, consumer *ckafka.Consumer, handler func(msgs []*ckafka.Message)) error {
    batch := make([]*ckafka.Message, 0, 100)
    for {
        select {
        case <-ctx.Done():
            return ctx.Err()
        default:
            ev := consumer.Poll(100) // 100 ms timeout
            if ev == nil {
                if len(batch) > 0 {
                    handler(batch)
                    batch = batch[:0]
                }
                continue
            }
            switch e := ev.(type) {
            case *ckafka.Message:
                batch = append(batch, e)
                if len(batch) >= 100 {
                    handler(batch)
                    batch = batch[:0]
                }
            case ckafka.Error:
                log.Printf("Consumer error: %v", e)
            }
        }
    }
}