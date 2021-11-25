package main

import (
	kafka "github.com/sohamkamani/golang-kafka-example/kafka"
)

func main() {

	k := kafka.Connect([]string{"localhost:9092"})

	response := k.NewResponse("01", 1)
	for {
		response.ReadMessage()
		msg, w := response.ReadMessage()
		//fmt.Println(string(msg.Value))
		w.WriteMessages([]byte(string(msg.Value)))
	}
	exit()
}

func exit() {
	exit := make(chan int)
	<-exit
}
