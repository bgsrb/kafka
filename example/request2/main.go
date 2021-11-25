package main

import (
	"fmt"
	"time"

	kafka "github.com/sohamkamani/golang-kafka-example/kafka"
)

func main() {

	k := kafka.Connect([]string{"localhost:9092"})

	request := k.NewRequest("rr22", 1)
	go func
		msg, _ := request.Request([]byte(fmt.Sprintf("req_1 (%d)", i)))
		fmt.Println(string(msg.Value))
		time.Sleep(time.Second * 10)
	){}

	go func(
		msg, _ := request.Request([]byte(fmt.Sprintf("req_1 (%d)", i)))
		fmt.Println(string(msg.Value))
		time.Sleep(time.Second * 10)
	){}
	exit()
}

func exit() {
	exit := make(chan int)
	<-exit
}
