package main

import (
	"flag"
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

	// apply minimal config only for example run
	flag.StringVar(&conf.Brokers, "brokers", conf.Brokers, "CSV list of Kafka seed brokers")
	flag.StringVar(&conf.SchemaRegistries, "schema-registries", conf.SchemaRegistries, "CSV list of Kafka schema registries")
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
