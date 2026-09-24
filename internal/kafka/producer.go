// Package kafka provides producers for sending stock update messages
// to Kafka topics using the StockUpdate protobuf type from kafkastockv1.
package kafka

import (
	"context"
	"fmt"
	"strings"

	"github.com/IBM/sarama"
	"google.golang.org/protobuf/proto"

	kafkastockv1 "stocker-store/proto/v1/kafka"

	"github.com/anomalyco/stocker-list/internal/config"
)

const (
	// StockUpdateTopic is the Kafka topic for stock update messages.
	StockUpdateTopic = "stock.update.v1"
)

// sender is the minimal subset of a Kafka sync producer that Producer uses to
// deliver messages. It is unexported so that a fake can be injected in tests
// while a nil value keeps the zero-value Producer a no-op.
type sender interface {
	SendMessage(*sarama.ProducerMessage) (int32, int64, error)
	Close() error
}

// Producer publishes StockUpdate messages to a configured Kafka topic.
type Producer struct {
	// Topic is the Kafka topic to publish to. Empty string defaults to StockUpdateTopic.
	Topic string
	// client is the Kafka sender; nil means Kafka is disabled and every
	// publish is a graceful no-op that never dials a broker.
	client sender
}

// NewProducer builds a Producer from cfg. When cfg.KafkaBrokers is empty the
// returned Producer is a no-op (nil client, no broker dialed). Otherwise a real
// sarama sync producer is created and returned.
func NewProducer(cfg *config.Config) (*Producer, error) {
	if cfg.KafkaBrokers == "" {
		// Kafka disabled: no client, no dial.
		return &Producer{Topic: topicOrDefault(cfg)}, nil
	}

	brokers := parseBrokers(cfg.KafkaBrokers)
	if len(brokers) == 0 {
		return nil, fmt.Errorf("create kafka producer: no brokers configured")
	}

	conf := sarama.NewConfig()
	conf.Net.TLS.Enable = cfg.KafkaSSLEnabled
	conf.Producer.Return.Errors = true

	if cfg.KafkaSASLUsername != "" && cfg.KafkaSASLPassword != "" {
		conf.Net.SASL.Enable = true
		conf.Net.SASL.Mechanism = saslMechanism(cfg.KafkaSASLMechanism)
		conf.Net.SASL.User = cfg.KafkaSASLUsername
		conf.Net.SASL.Password = cfg.KafkaSASLPassword
	}

	sp, err := sarama.NewSyncProducer(brokers, conf)
	if err != nil {
		return nil, fmt.Errorf("create kafka producer: %w", err)
	}

	return &Producer{Topic: topicOrDefault(cfg), client: sp}, nil
}

// topicOrDefault returns cfg.KafkaTopic if it is non-empty, otherwise
// StockUpdateTopic.
func topicOrDefault(cfg *config.Config) string {
	if cfg.KafkaTopic != "" {
		return cfg.KafkaTopic
	}
	return StockUpdateTopic
}

// parseBrokers splits a comma-separated broker list, trimming spaces and
// dropping empty entries.
func parseBrokers(list string) []string {
	parts := strings.Split(list, ",")
	brokers := make([]string, 0, len(parts))
	for _, p := range parts {
		p = strings.TrimSpace(p)
		if p != "" {
			brokers = append(brokers, p)
		}
	}
	return brokers
}

// saslMechanism maps a configuration mechanism name (case-insensitive) to the
// sarama SASL mechanism, defaulting to PLAIN when unrecognized.
func saslMechanism(name string) sarama.SASLMechanism {
	switch strings.ToUpper(name) {
	case "SCRAM-SHA-256":
		return sarama.SASLTypeSCRAMSHA256
	case "SCRAM-SHA-512":
		return sarama.SASLTypeSCRAMSHA512
	default:
		return sarama.SASLTypePlaintext
	}
}

// PublishStockUpdate marshals and enqueues a StockUpdate message for delivery
// to the configured topic. It is a no-op (returns nil) when Kafka is disabled
// (nil client) or when msg is nil.
func (p *Producer) PublishStockUpdate(ctx context.Context, msg *kafkastockv1.StockUpdate) error {
	if p.client == nil || msg == nil {
		return nil // no-op when Kafka is disabled
	}
	if p.Topic == "" {
		p.Topic = StockUpdateTopic
	}
	data, err := proto.Marshal(msg)
	if err != nil {
		return fmt.Errorf("marshal stock update: %w", err)
	}
	rec := &sarama.ProducerMessage{
		Topic: p.Topic,
		Key:   sarama.StringEncoder(msg.Symbol),
		Value: sarama.ByteEncoder(data),
	}
	if _, _, err := p.client.SendMessage(rec); err != nil {
		return fmt.Errorf("publish to kafka topic %q: %w", p.Topic, err)
	}
	return nil
}

// Close shuts down the underlying Kafka producer, if any. It is a no-op when
// Kafka is disabled.
func (p *Producer) Close() error {
	if p.client == nil {
		return nil
	}
	return p.client.Close()
}
