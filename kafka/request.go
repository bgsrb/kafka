package kafka

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/segmentio/kafka-go"
)

type Request struct {
	writer    *kafka.Writer
	reader    *kafka.Reader
	partition string
	messages  map[string](chan kafka.Message)
}

func (k *Kafka) NewRequest(topic string, partition int) *Request {

	//topics
	err := k.conn.CreateTopics(kafka.TopicConfig{
		Topic:             fmt.Sprintf("%s_request", topic),
		NumPartitions:     k.options.NumPartitions,
		ReplicationFactor: k.options.ReplicationFactor,
	}, kafka.TopicConfig{
		Topic:             fmt.Sprintf("%s_reponse", topic),
		NumPartitions:     k.options.NumPartitions,
		ReplicationFactor: k.options.ReplicationFactor,
	})
	if err != nil {
		panic(err.Error())
	}

	//writer
	writer := kafka.NewWriter(kafka.WriterConfig{
		Brokers: k.brokers,
		Topic:   fmt.Sprintf("%s_request", topic),
	})

	//reader
	reader := kafka.NewReader(kafka.ReaderConfig{
		Partition:       partition,
		Topic:           fmt.Sprintf("%s_reponse", topic),
		Brokers:         k.brokers,
		ReadLagInterval: time.Millisecond * 100,
		MaxWait:         1 * time.Second,
	})

	startTime := time.Now()
	reader.SetOffsetAt(context.Background(), startTime)

	req := &Request{
		writer:    writer,
		reader:    reader,
		partition: fmt.Sprint(partition),
		messages:  make(map[string](chan kafka.Message)),
	}

	go req.readMessage()

	return req
}

func (req *Request) readMessage() {
	for {
		msg, err := req.reader.ReadMessage(context.Background())
		if err != nil {
			fmt.Println("could not read message " + err.Error())
		}
		fmt.Println(string(msg.Value))
		// uuid := ""
		// for _, header := range msg.Headers {
		// 	if header.Key == "uuid" {
		// 		uuid = string(header.Value)
		// 		break
		// 	}
		// }
		// select {
		// case req.messages[uuid] <- msg:

		// default:

		// }

	}
}

func (req *Request) getMessage(uuid string) (kafka.Message, error) {
	select {
	case msg := <-req.messages[uuid]:
		delete(req.messages, uuid)
		return msg, nil
	case <-time.After(time.Second * 3):
		return kafka.Message{}, errors.New("Time out: No news in 3 second")
	}
}

func (req *Request) Request(value []byte) (kafka.Message, error) {

	key := fmt.Sprint(uuid.New())
	req.messages[key] = make(chan kafka.Message, 1)

	req.writer.WriteMessages(context.Background(), kafka.Message{
		Topic: req.writer.Topic,
		Headers: []kafka.Header{
			{
				Key:   "partition_key",
				Value: []byte(req.partition),
			},
			{
				Key:   "uuid",
				Value: []byte(fmt.Sprint(key)),
			},
		},
		Value: value,
	})
	return req.getMessage(key)
}
