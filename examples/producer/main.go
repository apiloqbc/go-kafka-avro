package main

import (
	"flag"
	"fmt"
	"github.com/apiloqbc/go-kafka-avro"
	"github.com/kelseyhightower/envconfig"
	"log"
	"os"
	"time"
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
	var n int
	schema := `{
				"type": "record",
				"name": "Example",
				"fields": [
					{"name": "Id", "type": "string"},
					{"name": "Type", "type": "string"},
					{"name": "Data", "type": "string"}
				]
			}`
	producer, err := kafka.NewAvroProducer(conf)
	if err != nil {
		fmt.Printf("Could not create avro producer: %s", err)
		os.Exit(1)
	}
	flag.IntVar(&n, "n", 1, "number")
	flag.Parse()
	for i := 0; i < n; i++ {
		fmt.Println(i)
		addMsg(producer, schema)
	}
}

func addMsg(producer *kafka.AvroProducer, schema string) {
	value := `{
		"Id": "1",
		"Type": "example_type",
		"Data": "example_data"
	}`
	key := time.Now().String()
	err := producer.Add(topic, schema, []byte(key), []byte(value))
	fmt.Println(key)
	if err != nil {
		fmt.Printf("Could not add a msg: %s", err)
		os.Exit(1)
	}
}
