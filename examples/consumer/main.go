package main

import (
	"fmt"
	"github.com/apiloqbc/go-kafka-avro"
	"github.com/bsm/sarama-cluster"
	"github.com/kelseyhightower/envconfig"
	"log"
	"os"
)

var conf kafka.Config

func init() {
	conf = kafka.NewKafkaConfig()

	err := envconfig.Process("", &conf)
	if err != nil {
		log.Fatal(err.Error())
	}
}

var topic = "test"

func main() {
	consumerCallbacks := kafka.ConsumerCallbacks{
		OnDataReceived: func(msg kafka.Message) {
			fmt.Println(msg)
		},
		OnError: func(err error) {
			fmt.Println("Consumer error", err)
		},
		OnNotification: func(notification *cluster.Notification) {
			fmt.Println(notification)
		},
	}

	consumer, err := kafka.NewAvroConsumer(conf, []string{"events"}, "consumer-group", consumerCallbacks)
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
	consumer.Consume()
}
