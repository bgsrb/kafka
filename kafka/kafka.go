package kafka

import (
	"github.com/segmentio/kafka-go"
)

type Kafka struct {
	options Options
	conn    *kafka.Conn
	brokers []string
}

func Connect(brokers []string, options ...func(*Kafka)) *Kafka {

	//conn
	conn, err := kafka.Dial("tcp", brokers[0])
	if err != nil {
		panic(err.Error())
	}

	//kafka
	k := &Kafka{
		brokers: brokers,
		conn:    conn,
		options: Options{
			NumPartitions:     1,
			ReplicationFactor: 1,
		},
	}
	//options
	for _, option := range options {
		option(k)
	}

	return k
}
