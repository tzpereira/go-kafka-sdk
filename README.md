# go-kafka-sdk

A lightweight Go SDK for Apache Kafka, providing: - Asynchronous
Producer abstraction\
- Multi-goroutine Consumer utilities\
- Batch and single-message consumption helpers

## Requirements

-   Go 1.18+
-   [confluent-kafka-go](https://github.com/confluentinc/confluent-kafka-go)
-   A running Kafka broker (see Docker example below)

## Installation

Install dependencies:

``` sh
go get github.com/confluentinc/confluent-kafka-go/kafka
go get github.com/joho/godotenv
```

## Environment Variables

Examples can be configured using a `.env` file or environment variables:

    KAFKA_BROKERS=localhost:9092
    KAFKA_TOPICS=test-topic
    KAFKA_GROUP_ID=test-group

## Running Kafka Locally (Docker Compose)

Example `docker-compose.yml`:

``` yaml
services:
  zookeeper:
    image: confluentinc/cp-zookeeper:7.5.0
    ports:
      - "2181:2181"
    environment:
      ZOOKEEPER_CLIENT_PORT: 2181
      ZOOKEEPER_TICK_TIME: 2000

  kafka:
    image: confluentinc/cp-kafka:7.5.0
    ports:
      - "9092:9092"
    environment:
      KAFKA_BROKER_ID: 1
      KAFKA_ZOOKEEPER_CONNECT: zookeeper:2181
      KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://localhost:9092
      KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 1
    depends_on:
      - zookeeper
```

Start Kafka:

``` sh
docker-compose up -d
```

## Usage

### Producer Example

See [`examples/producer/main.go`](examples/producer/main.go):

``` go
// Command producer is an example Kafka producer using go-kafka-sdk.
//
// Reads configuration from .env or environment variables:
//   KAFKA_BROKERS: list of brokers (e.g., localhost:9092)
//   KAFKA_TOPICS:  topic name (e.g., test-topic)
//
// Usage:
//   go run ./examples/producer
//
// The program sends a test message to the specified topic.
```

Run:

``` sh
go run ./examples/producer
```

### Consumer Example

See [`examples/consumer/main.go`](examples/consumer/main.go):

``` go
// Command consumer is an example Kafka consumer using go-kafka-sdk.
//
// Reads configuration from .env or environment variables:
//   KAFKA_BROKERS: list of brokers (e.g., localhost:9092)
//   KAFKA_TOPICS:  topic name (e.g., test-topic)
//   KAFKA_GROUP_ID: consumer group name (e.g., test-group)
//
// Usage:
//   go run ./examples/consumer
//
// The program consumes messages from the specified topic and prints them to the terminal.
```

Run:

``` sh
go run ./examples/consumer
```

## Running Tests

To execute all tests (Kafka must be running on `localhost:9092`):

``` sh
go test -v ./kafka
```

## Notes

-   No brokers, topics, or consumer groups are hardcoded --- pass them
    via parameters or environment variables.
-   Check the source code for more advanced use cases such as batch
    consumption, worker pools, and custom Kafka configs.