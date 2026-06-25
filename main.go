package main

import (
	"encoding/json"
	"fmt"
	"log"
	"math/rand"
	"sync"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

type Storer interface {
	Put(string, []byte) error
	Get(string) ([]byte, error)
}

type Storage struct {
	mu   sync.RWMutex
	data map[string][]byte
}

func NewStorage() *Storage {
	return &Storage{
		data: map[string][]byte{},
	}
}

func (s *Storage) Put(key string, val []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.data[key] = val

	return nil
}

func (s *Storage) Get(key string) ([]byte, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	val, ok := s.data[key]

	if !ok {
		return nil, fmt.Errorf("value not found")
	}

	return val, nil
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
}

type Consumer struct {
	consumer *kafka.Consumer
	Storage  Storer
}

func NewConsumer(storage Storer) (*Consumer, error) {
	c, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers":        "localhost:9093",
		"broker.address.family":    "v4",
		"group.id":                 "group1",
		"session.timeout.ms":       6000,
		"auto.offset.reset":        "earliest",
		"enable.auto.offset.store": false,
	})

	if err != nil {
		return nil, err
	}

	err = c.SubscribeTopics([]string{topic}, nil)
	if err != nil {
		return nil, err
	}

	return &Consumer{
		Storage:  NewStorage(),
		consumer: c,
	}, nil
}

// it's a streaming server so it's considered a loop in my opinion
func (c *Consumer) consumeLoop() {
	for {
		// poll(), poll the consumer for msgs and events
		ev := c.consumer.Poll(100)
		if ev == nil {
			continue
		}

		// e -> kafka event
		switch e := ev.(type) {
		case *kafka.Message:
			_, err := c.consumer.StoreMessage(e)
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
