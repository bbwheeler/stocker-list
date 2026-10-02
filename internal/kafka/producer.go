// Package kafka provides producers for sending stock update messages
// to Kafka topics using the Stock protobuf type from kafkastockv1.
package kafka

import (
	"context"
	"fmt"
	"strings"

	"github.com/segmentio/kafka-go"
	"google.golang.org/protobuf/proto"

	kafkastockv1 "git.wheeli.ca/brian/stocker-store/proto/v1"

	"github.com/anomalyco/stocker-list/internal/config"
)

const (
	// StockUpdateTopic is the Kafka topic for stock update messages.
	StockUpdateTopic = "stock.update.v1"
)

// sender is the minimal subset of a kafka-go Writer that Producer uses to
// deliver messages. It is unexported so that a fake can be injected in tests
// while a nil value keeps the zero-value Producer a no-op.
type sender interface {
	WriteMessages(ctx context.Context, msgs ...kafka.Message) error
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
// kafka-go writer is created and returned.
func NewProducer(cfg *config.Config) (*Producer, error) {
	if cfg.KafkaBrokers == "" {
		// Kafka disabled: no client, no dial.
		return &Producer{Topic: topicOrDefault(cfg)}, nil
	}

	brokers := parseBrokers(cfg.KafkaBrokers)
	if len(brokers) == 0 {
		return nil, fmt.Errorf("create kafka producer: no brokers configured")
	}

	w := kafka.NewWriter(kafka.WriterConfig{
		Brokers: brokers,
	})

	return &Producer{Topic: topicOrDefault(cfg), client: w}, nil
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

// PublishStockUpdate marshals and enqueues a StockUpdate message for delivery
// to the configured topic. It is a no-op (returns nil) when Kafka is disabled
// (nil client) or when msg is nil.
func (p *Producer) PublishStockUpdate(ctx context.Context, msg *kafkastockv1.Stock) error {
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
	msgOut := kafka.Message{
		Topic: p.Topic,
		Key:   []byte(msg.Symbol),
		Value: data,
	}
	if err := p.client.WriteMessages(ctx, msgOut); err != nil {
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
