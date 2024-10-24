package main

import (
	"fmt"
	"github.com/apiloqbc/go-kafka-avro"
	"github.com/kelseyhightower/envconfig"
	"log"
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
	topics, err := kafka.GetTopicList(conf.Brokers, conf, "(\\w+\\.)?paas-events")
	fmt.Println(topics)
	if err != nil {
		log.Fatal(err.Error())
	}
}
