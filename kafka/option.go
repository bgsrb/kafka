package kafka

import "github.com/segmentio/kafka-go"

type Options struct {
	NumPartitions     int
	ReplicationFactor int
	Balancer          kafka.Balancer
}

func WithNumPartitions(partitions int) func(*Kafka) {
	return func(s *Kafka) {
		s.options.NumPartitions = partitions
	}
}

func WithReplicationFactor(replications int) func(*Kafka) {
	return func(s *Kafka) {
		s.options.ReplicationFactor = replications
	}
}

func WithBalancer(balancer kafka.Balancer) func(*Kafka) {
	return func(s *Kafka) {
		s.options.Balancer = balancer
	}
}
