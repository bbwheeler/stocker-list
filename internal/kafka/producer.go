// Package kafka provides producers for sending stock update messages
// to Kafka topics using the StockUpdate protobuf type from kafkastockv1.
package kafka

import (
	"context"

	kafkastockv1 "stocker-store/proto/v1/kafka"
)

const (
	// StockUpdateTopic is the Kafka topic for stock update messages.
	StockUpdateTopic = "stock.update.v1" // TODO: configurable
)

// Producer publishes StockUpdate messages to a configured Kafka topic.
type Producer struct {
	// Topic is the Kafka topic to publish to. Empty string defaults to StockUpdateTopic.
	Topic string
	// The actual internal kafka client would be injected in future work.
}

// PublishStockUpdate marshals and enqueues a StockUpdate message for delivery.
func (p *Producer) PublishStockUpdate(ctx context.Context, msg *kafkastockv1.StockUpdate) error {
	if p.Topic == "" {
		p.Topic = StockUpdateTopic
	}
	return nil // TODO: wire to real kafka client
}
