package kafka

import (
	"crypto/tls"
	"crypto/x509"
	"github.com/Shopify/sarama"
	"github.com/pkg/errors"
	"io/ioutil"
	"log"
	"regexp"
)

// GetTopicList returns a list of topics that match the specified pattern.
func GetTopicList(brokers []string, envConf Config, topicPattern string) ([]string, error) {
	// init (custom) saramaConfig, enable errors and notifications
	config := sarama.NewConfig()
	config.Net.TLS.Enable = envConf.TLSEnabled
	//	config.Net.TLS.Config = &tls.Config{InsecureSkipVerify: true} // Se necess
	addSasl(envConf, config)
	err := addTls(envConf, config)
	if err != nil {
		log.Fatalf("Errore durante la creazione del client Kafka: %v", err)
	}

	// Connessione al broker Kafka
	client, err := sarama.NewClient(brokers, config)
	if err != nil {
		log.Fatalf("Errore durante la creazione del client Kafka: %v", err)
	}
	defer client.Close()

	admin, err := sarama.NewClient(brokers, config)
	if err != nil {
		log.Fatalf("Errore durante la creazione dell'admin Kafka: %v", err)
	}
	defer admin.Close()

	// Ottieni la lista di tutti i topic
	availableTopics, err := admin.Topics()
	if err != nil {
		return nil, err
	}

	// Compile the topic pattern
	reg, err := regexp.Compile(topicPattern)
	if err != nil {
		return nil, err
	}

	// Filter the topics that match the pattern
	var subTopics []string
	for _, topic := range availableTopics {
		if reg.MatchString(topic) {
			subTopics = append(subTopics, topic)
		}
	}
	return subTopics, nil
}

func addSasl(envConf Config, saramaConf *sarama.Config) {
	if envConf.SaslEnabled {
		saramaConf.Net.SASL.Enable = envConf.SaslEnabled
		saramaConf.Net.SASL.Mechanism = sarama.SASLMechanism(envConf.SaslMechanism)
		saramaConf.Net.SASL.User = envConf.Username
		saramaConf.Net.SASL.Password = envConf.Password
		saramaConf.Net.SASL.Enable = envConf.SaslEnabled
		if envConf.SaslMechanism == sarama.SASLTypeSCRAMSHA256 {
			saramaConf.Net.SASL.SCRAMClientGeneratorFunc = func() sarama.SCRAMClient {
				return &XDGSCRAMClient{HashGeneratorFcn: SHA256}
			}
		} else if envConf.SaslMechanism == sarama.SASLTypeSCRAMSHA512 {
			saramaConf.Net.SASL.SCRAMClientGeneratorFunc = func() sarama.SCRAMClient {
				return &XDGSCRAMClient{HashGeneratorFcn: SHA512}
			}
		}
	}
}

func addTls(envConf Config, saramaConf *sarama.Config) error {
	// configure TLS
	saramaConf.Net.TLS.Enable = envConf.TLSEnabled
	if envConf.CACerts != "" {
		cert, err := tls.LoadX509KeyPair(envConf.TLSCert, envConf.TLSKey)
		if err != nil {
			return errors.Wrapf(err, "failed to load TLS cert(%s) and key(%s)", envConf.TLSCert, envConf.TLSKey)
		}

		ca, err := ioutil.ReadFile(envConf.CACerts)
		if err != nil {
			return errors.Wrapf(err, "failed to load CA cert bundle at: %s", envConf.CACerts)
		}

		pool := x509.NewCertPool()
		pool.AppendCertsFromPEM(ca)

		tlsCfg := &tls.Config{
			Certificates: []tls.Certificate{cert},
			RootCAs:      pool,
		}
		saramaConf.Net.TLS.Config = tlsCfg
	}
	return nil
}
