package kafka

import (
	"crypto/sha256"
	"crypto/sha512"
	"github.com/xdg/scram"
	"time"
)

// simple Kafka config abstraction; can be populated from env vars
// via FromEnv() or fields can applied to CLI flags by the caller.
// simple Kafka config abstraction; can be populated from env vars
// via FromEnv() or fields can applied to CLI flags by the caller.
type Config struct {
	Username string `envconfig:"KAFKA_USERNAME"`
	Password string `envconfig:"KAFKA_PASSWORD"`

	Brokers  string `envconfig:"KAFKA_BROKERS"`
	Version  string `envconfig:"KAFKA_VERSION"`
	Verbose  bool   `envconfig:"KAFKA_VERBOSE"`
	ClientID string `envconfig:"KAFKA_CLIENT_ID"`

	TLSEnabled bool   `envconfig:"KAFKA_TLS_ENABLED"`
	TLSKey     string `envconfig:"KAFKA_TLS_KEY"`
	TLSCert    string `envconfig:"KAFKA_TLS_CERT"`
	CACerts    string `envconfig:"KAFKA_CA_CERTS"`

	// Consumer specific parameters
	RebalanceStrategy string        `envconfig:"KAFKA_REBALANCE_STRATEGY"`
	RebalanceTimeout  time.Duration `envconfig:"KAFKA_REBALANCE_TIMEOUT"`
	InitOffsets       string        `envconfig:"KAFKA_INIT_OFFSETS"`
	CommitInterval    time.Duration `envconfig:"KAFKA_COMMIT_INTERVAL"`

	// Producer specific parameters
	FlushInterval    time.Duration `envconfig:"KAFKA_FLUSH_INTERVAL"`
	SaslMechanism    string        `envconfig:"KAFKA_SASL_MECHANISM"`
	SaslEnabled      bool          `envconfig:"KAFKA_SASL_ENABLED"`
	SchemaRegistries string        `envconfig:"KAFKA_SCHEMA_REGISTRIES"`
}

// returns a new kafka.Config with reasonable defaults for some values
func NewKafkaConfig() Config {
	return Config{
		Brokers:           "localhost:9092",
		Version:           "1.1.0",
		ClientID:          "sarama-easy",
		RebalanceStrategy: "roundrobin",
		RebalanceTimeout:  1 * time.Minute,
		InitOffsets:       "latest",
		CommitInterval:    10 * time.Second,
		FlushInterval:     1 * time.Second,
	}
}

var (
	// SHA256 SASLMechanism
	SHA256 scram.HashGeneratorFcn = sha256.New
	// SHA512 SASLMechanism
	SHA512 scram.HashGeneratorFcn = sha512.New
)

// XDGSCRAMClient for SASL-Protocol
type XDGSCRAMClient struct {
	*scram.Client
	*scram.ClientConversation
	scram.HashGeneratorFcn
}

// Begin of XDGSCRAMClient
func (x *XDGSCRAMClient) Begin(userName, password, authzID string) (err error) {
	x.Client, err = x.HashGeneratorFcn.NewClient(userName, password, authzID)
	if err != nil {
		return err
	}
	x.ClientConversation = x.Client.NewConversation()
	return nil
}

// Step of XDGSCRAMClient
func (x *XDGSCRAMClient) Step(challenge string) (response string, err error) {
	response, err = x.ClientConversation.Step(challenge)
	return
}

// Done of XDGSCRAMClient
func (x *XDGSCRAMClient) Done() bool {
	return x.ClientConversation.Done()
}
