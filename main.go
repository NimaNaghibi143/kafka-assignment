package main

import (
	"encoding/json"
	"fmt"
	"log"
	"math/rand"
	"sync"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

type Storage struct {
	mu   sync.RWMutex
	data map[string][]byte
}

func NewStorage() *Storage {
	return &Storage{
		data: map[string][]byte{},
	}
}

func (s *Storage) put(key string, val []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.data[key] = val

	return nil
}

var (
	topic = "foobarbaztopic"
)

type MessageState int

const (
	MessageStateCompleted  MessageState = iota
	MessageStateInProgress MessageState = iota
	MessageStateFailed     MessageState = iota
)

type Message struct {
	State MessageState
}

func main() {
	produce()
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
		if ev == nil {
			continue
		}

		// e -> kafka event
		switch e := ev.(type) {
		case *kafka.Message:
			_, err := c.StoreMessage(e)
			if err != nil {
				fmt.Println("store message error: ", err)
			}

			var msg Message
			if err := json.Unmarshal(e.Value, &msg); err != nil {
				log.Fatal(err)
			}
			fmt.Println(msg)
		case kafka.Error:
			if e.Code() == kafka.ErrAllBrokersDown {
				break
			}
		}
	}
}

func produce() {
	p, err := kafka.NewProducer(&kafka.ConfigMap{
		"bootstrap.servers": "localhost:9093",
	})

	if err != nil {
		log.Fatal(err)
	}

	if err != nil {
		log.Fatal(err)
	}

	for i := 0; i < 1000; i++ {
		msg := Message{
			State: MessageState(rand.Intn(3)),
		}
		b, err := json.Marshal(msg)

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
