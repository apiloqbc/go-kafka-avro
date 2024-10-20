package main

import (
	"flag"
	"fmt"
	kafka "github.com/apiloqbc/go-kafka-avro"
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

	// apply minimal config only for example run
	flag.StringVar(&conf.Brokers, "brokers", conf.Brokers, "CSV list of Kafka seed brokers")
	flag.StringVar(&conf.SchemaRegistries, "schema-registries", conf.SchemaRegistries, "CSV list of Kafka schema registries")
	flag.StringVar(&topic, "topics", "events", "CSV list of Kafka topics to consume")
	flag.BoolVar(&conf.Verbose, "verbose", conf.Verbose, "Enable detailed logging of Kafka client internals")
	flag.BoolVar(&conf.TLSEnabled, "tls-enabled", conf.TLSEnabled, "Enable TLS encryption")
	flag.BoolVar(&conf.SaslEnabled, "sasl-enabled", conf.SaslEnabled, "Enable SASL authentication")
	flag.StringVar(&conf.Username, "username", conf.Username, "Kafka SASL Username")
	flag.StringVar(&conf.Password, "password", conf.Password, "Kafka SASL Password")
	flag.StringVar(&conf.SaslMechanism, "sasl-mechanism", conf.SaslMechanism, "SASL Mechanism to use for authentication")

	flag.Parse()

	log.Printf("Config: %+v\n", conf)
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
