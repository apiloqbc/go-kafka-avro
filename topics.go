package kafka

import (
	"github.com/Shopify/sarama"
	"log"
	"regexp"
)

// GetTopicList returns a list of topics that match the specified pattern.
func GetTopicList(brokers []string, conf *sarama.Config, topicPattern string) ([]string, error) {
	// Create a new Kafka client
	client, err := sarama.NewClient(brokers, conf)
	if err != nil {
		return nil, err
	}
	defer func() {
		if err := client.Close(); err != nil {
			log.Printf("Error closing the client: %v", err)
		}
	}()

	// Fetch all available topics
	availableTopics, err := client.Topics()
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
