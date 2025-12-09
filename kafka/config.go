package kafka

import (
	"os"
	"strings"
)

type Config struct {
	Brokers []string
	GroupID string
	Topics  []string
}

// NewConfigFromEnv creates a Config struct by reading Kafka settings from environment variables.
// It expects the following environment variables to be set:
// - KAFKA_BROKERS: Comma-separated list of broker addresses.
// - KAFKA_GROUP_ID: Consumer group ID.
// - KAFKA_TOPICS: Comma-separated list of topics to subscribe to.
// Returns a pointer to the Config struct.
func NewConfigFromEnv() *Config {
	brokers := strings.Split(os.Getenv("KAFKA_BROKERS"), ",")
	for i := range brokers {
		brokers[i] = strings.TrimSpace(brokers[i])
	}
	topics := strings.Split(os.Getenv("KAFKA_TOPICS"), ",")
	for i := range topics {
		topics[i] = strings.TrimSpace(topics[i])
	}
	return &Config{
		Brokers: brokers,
		GroupID: os.Getenv("KAFKA_GROUP_ID"),
		Topics:  topics,
	}
}
