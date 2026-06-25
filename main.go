package main

import (
	"encoding/json"
	"fmt"
	"log"
	"math/rand"
	"os"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

var (
	topic = "foobarbaztopic"
)

type MessageState int

const (
	MessageStateCompleted MessageState = iota
	MessageStateProgress
	MessageStateFailed
)

type Message struct {
	State MessageState
}

func main() {
	// go produce()
	consume()
}

func consume() {
	c, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers":        "localhost:9093",
		"broker.address.family":    "v4",
		"group.id":                 "group1",
		"session.timeout.ms":       6000,
		"auto.offset.reset":        "earliest",
		"enable.auto.offset.store": false,
	})

	if err != nil {
		log.Fatal(err)
	}

	fmt.Printf("Created consumer %v\n", c)

	err = c.SubscribeTopics([]string{topic}, nil)
	if err != nil {
		log.Fatal(err)
	}

	for {
		// poll(), poll the consumer for msgs and events
		ev := c.Poll(100)
		if ev != nil {
			continue
		}

		// e -> kafka event
		switch e := ev.(type) {
		case *kafka.Message:
			fmt.Printf("%% Message on %s:\n%s\n", e.TopicPartition, string(e.Value))
			_, err := c.StoreMessage(e)
			if err != nil {
				fmt.Fprintf(os.Stderr, "%% Error storing offset after message %s:\n", e.TopicPartition)
			}
		case kafka.Error:
			if e.Code() == kafka.ErrAllBrokersDown {
				break
			}
		default:
			fmt.Printf("Ignored %v\n", e)
		}
	}
}

func produce() {
	p, err := kafka.NewProducer(&kafka.ConfigMap{
		"bootstrap.servers": "localhost:9093",
	})

	msg := Message{
		State: MessageState(rand.Intn(3)),
	}

	b, err := json.Marshal(msg)

	if err != nil {
		log.Fatal(err)
	}
	if err != nil {
		log.Fatal(err)
	}

	for i := 0; i < 1000; i++ {
		err = p.Produce(&kafka.Message{
			TopicPartition: kafka.TopicPartition{
				Topic:     &topic,
				Partition: kafka.PartitionAny,
			},
			Value: b,
		}, nil)

		if err != nil {
			log.Fatal(err)
		}
	}

	defer p.Close()
}
