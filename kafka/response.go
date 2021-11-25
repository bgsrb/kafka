package kafka

import (
	"context"
	"fmt"
	"strconv"
	"time"

	"github.com/segmentio/kafka-go"
)

//Response
type Response struct {
	writers map[int]*kafka.Writer
	reader  *kafka.Reader
	topic   string
	brokers []string
}

type Writer struct {
	writer    *kafka.Writer
	partition int
	uuid      string
	value     string
}

func (w *Writer) WriteMessages(value []byte) {
	w.writer.WriteMessages(context.Background(), kafka.Message{
		Topic:     w.writer.Topic,
		Partition: w.partition,
		Value:     value,
		Headers: []kafka.Header{
			{
				Key:   "partition_key",
				Value: []byte(fmt.Sprint(w.partition)),
			},
			{
				Key:   "uuid",
				Value: []byte(w.uuid),
			},
		},
	})
}

func (k *Kafka) NewResponse(topic string, partition int) *Response {

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

	reader := kafka.NewReader(kafka.ReaderConfig{
		Topic:           fmt.Sprintf("%s_request", topic),
		Brokers:         k.brokers,
		ReadLagInterval: time.Millisecond * 100,
		MaxWait:         1 * time.Second,

		// Partition: partition,
	})
	startTime := time.Now()
	reader.SetOffsetAt(context.Background(), startTime)

	return &Response{
		topic:   topic,
		reader:  reader,
		brokers: k.brokers,
		writers: make(map[int]*kafka.Writer, 0),
	}
}

func (resp *Response) ReadMessage() (kafka.Message, *Writer) {
	msg, err := resp.reader.ReadMessage(context.Background())
	fmt.Println(string(msg.Value))

	if err != nil {
		fmt.Println("could not read message " + err.Error())
	}

	partition := -1
	uuid := ""
	for _, header := range msg.Headers {
		if header.Key == "partition_key" {
			partition, _ = strconv.Atoi(string(header.Value))
		}

		if header.Key == "uuid" {
			uuid = string(header.Value)
		}
	}

	writer, ok := resp.writers[partition]
	if !ok {
		writer = kafka.NewWriter(kafka.WriterConfig{
			Brokers:  resp.brokers,
			Topic:    fmt.Sprintf("%s_reponse", resp.topic),
			Balancer: &Balancer{},
		})
		resp.writers[partition] = writer
	}

	return msg, &Writer{
		writer:    writer,
		partition: partition,
		uuid:      uuid,
		value:     string(msg.Value),
	}
}

type Balancer struct {
}

func (h *Balancer) Balance(msg kafka.Message, partitions ...int) int {
	partition := 0
	for _, header := range msg.Headers {
		if header.Key == "partition_key" {
			partition, _ = strconv.Atoi(string(header.Value))
		}
	}
	return int(partition)
}
