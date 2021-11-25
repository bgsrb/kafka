package main

import (
	"fmt"
	"time"

	kafka "github.com/sohamkamani/golang-kafka-example/kafka"
)

type Message struct{}

func main() {

	k := kafka.Connect([]string{"localhost:9092"})
	request := k.NewRequest("01", 0)
	var i = 1
	for {
		// go func() {
		// 	msg := request.Request([]byte(fmt.Sprint("req_0")))
		// 	fmt.Println(string(msg.Value))
		// }()
		msg, err := request.Request([]byte(fmt.Sprint("req_0", i)))
		if err == nil {
			fmt.Println(string(msg.Value))
		}
		i++
		time.Sleep(time.Second * 1)
	}

	exit()
}

func exit() {
	exit := make(chan int)
	<-exit
}
