package kafka

import (
	"context"
	"strings"

	ckafka "github.com/confluentinc/confluent-kafka-go/kafka"
)

// NewProducer creates a Kafka producer with the given brokers and configuration.
// Returns a configured *ckafka.Producer or an error if creation fails.
func NewProducer(brokers []string, config map[string]string) (*ckafka.Producer, error) {
	conf := &ckafka.ConfigMap{
		"bootstrap.servers": strings.Join(brokers, ","),
		"acks":              "all",
	}
	for k, v := range config {
		(*conf)[k] = v
	}
	return ckafka.NewProducer(conf)
}

// StartDeliveryHandler starts a background goroutine to handle delivery reports for the given producer.
// Call this once after creating the producer, and ensure the goroutine is stopped on shutdown.
func StartDeliveryHandler(ctx context.Context, producer *ckafka.Producer, reportFunc func(*ckafka.Message)) {
       go func() {
	       for {
		       select {
		       case <-ctx.Done():
			       return
		       case e := <-producer.Events():
			       switch ev := e.(type) {
			       case *ckafka.Message:
				       if reportFunc != nil {
					       reportFunc(ev)
				       }
			       }
		       }
	       }
       }()
}

// Produce sends a message to the given Kafka topic asynchronously.
// Errors are only returned if the message could not be queued for delivery.
func Produce(_ context.Context, producer *ckafka.Producer, topic string, value []byte) error {
       return producer.Produce(&ckafka.Message{
	       TopicPartition: ckafka.TopicPartition{Topic: &topic, Partition: ckafka.PartitionAny},
	       Value:         value,
       }, nil)
}
