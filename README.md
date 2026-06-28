# Kafka Message Pipeline

> Educational project for learning Apache Kafka fundamentals with Go.

A small producer/consumer pipeline that demonstrates message production, streaming consumption, manual offset management, and in-memory state aggregation using the [Confluent Kafka Go client](https://github.com/confluentinc/confluent-kafka-go).

## How It Works

1. **Producer** publishes 1000 JSON messages to a topic, each tagged with a random state: `completed`, `in-progress`, or `failed`.
2. **Consumer** streams messages off the topic, parses each state, and stores it in a thread-safe persistent store bucketed by state.
3. **Tests** assert that no messages are dropped and that each message lands in the correct bucket.

## Stack

- Go 1.26
- Apache Kafka 3.3 (Bitnami image, ZooKeeper mode)
- Docker Compose

## Getting Started

```bash
# Start Kafka + ZooKeeper
docker compose up -d

# Run the producer + consumer
go run .

# Run the test suite
go test ./...
```

The broker is exposed on `localhost:9093`.

## License

For educational use.
