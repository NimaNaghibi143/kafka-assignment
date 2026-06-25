package main

import (
	"encoding/json"
	"fmt"
	"log"
	"math/rand"

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
	go produce()
}

func consume() {
	c, err := kafka.NewConsumer(&kafka.ConfigMap{
		"boostrap.servers":         "localhost:9093",
		"borker.address.family":    "v4",
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
