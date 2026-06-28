package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"log/slog"
	"math/rand"
	"sync"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

type Storer interface {
	Put(MessageState, []byte) error
	Get(MessageState) ([][]byte, error)
}

type Storage struct {
	mu   sync.RWMutex
	data map[MessageState][][]byte
}

func NewStorage() *Storage {
	return &Storage{
		data: map[MessageState][][]byte{
			MessageStateCompleted:  {},
			MessageStateInProgress: {},
			MessageStateFailed:     {},
		},
	}
}

func (s *Storage) Put(state MessageState, val []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.data[state] = append(s.data[state], val)

	return nil
}

func (s *Storage) Get(state MessageState) ([][]byte, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	val, ok := s.data[state]

	if !ok {
		return nil, fmt.Errorf("value not found")
	}

	return val, nil
}

const (
	lenMessages = 1000
)

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
	ctx, cancel := context.WithCancel(context.TODO())
	produce(cancel)
	storage := NewStorage()
	c, err := NewConsumer(storage)
	if err != nil {
		log.Fatal(err)
	}
	// go func() {
	// 	time.Sleep(time.Second * 5)
	// 	cancel()
	// }()

	c.consumeLoop(ctx)
	fmt.Println(counter[MessageStateCompleted])
	msgs, _ := storage.Get(MessageStateCompleted)
	fmt.Println(len(msgs))
	// fmt.Printf("%v+\n", c.Storage.(*Storage).data)
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
		Storage:  storage,
		consumer: c,
	}, nil
}

// to avoid over engineering we comment this
// func (c *Consumer) Start() {
// 	go c.consumeLoop()
// }

// it's a streaming server so it's considered a loop in my opinion
func (c *Consumer) consumeLoop(ctx context.Context) {
	count := 0
	for {
		select {
		case <-ctx.Done():
			return
		default:
			// poll(), poll the consumer for msgs and events
			ev := c.consumer.Poll(100)
			if ev == nil {
				continue
			}

			// e -> kafka event
			switch e := ev.(type) {
			case *kafka.Message:
				count++
				_, err := c.consumer.StoreMessage(e)
				if err != nil {
					fmt.Println("store message error: ", err)
				}

				var msg Message
				if err := json.Unmarshal(e.Value, &msg); err != nil {
					log.Fatal(err)
				}

				if err := c.Storage.Put(msg.State, e.Value); err != nil {
					log.Fatal(err)
				}
				if count == 1000 {
					return
				}
			case kafka.Error:
				if e.Code() == kafka.ErrAllBrokersDown {
					return
				}
			}
		}
	}
}

var counter = map[MessageState]int{
	MessageStateCompleted:  0,
	MessageStateInProgress: 0,
	MessageStateFailed:     0,
}

func produce(cancel context.CancelFunc) {

	p, err := kafka.NewProducer(&kafka.ConfigMap{
		"bootstrap.servers": "localhost:9093",
	})
	if err != nil {
		log.Fatal(err)
	}

	defer p.Close()

	slog.Info("Start Producing", "topic", topic, "messages", lenMessages)
	for i := 0; i < lenMessages; i++ {
		state := MessageState(rand.Intn(3))
		counter[state]++
		msg := Message{
			State: state,
		}
		b, _ := json.Marshal(msg)

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
	slog.Info("Done Producing", "topic", topic, "messages", lenMessages)
	p.Flush(5000)
}
