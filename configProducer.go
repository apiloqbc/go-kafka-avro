package kafka

import (
	"crypto/tls"
	"crypto/x509"
	"github.com/Shopify/sarama"
	"github.com/pkg/errors"
	"io/ioutil"
)

func configureProducerSasl(envConf Config, saramaConf *sarama.Config) {
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

// side effect TLS setup into Sarama config if env config specifies to do so
func configureProducerTLS(envConf Config, saramaConf *sarama.Config) error {
	// configure TLS
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
