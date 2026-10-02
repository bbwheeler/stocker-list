package kafka

import (
	"context"
	"testing"
	"time"

	"github.com/segmentio/kafka-go"
	"google.golang.org/protobuf/proto"

	kafkastockv1 "git.wheeli.ca/brian/stocker-store/proto/v1"

	"github.com/anomalyco/stocker-list/internal/config"
)

// TestProducer_ZeroValue_NoOp verifies the zero-value Producer is a graceful
// no-op: publishing returns nil without panicking or dialing any broker.
func TestProducer_ZeroValue_NoOp(t *testing.T) {
	p := &Producer{}
	msg := &kafkastockv1.Stock{Symbol: "AC", Exchange: "TSX"}
	if err := p.PublishStockUpdate(context.Background(), msg); err != nil {
		t.Fatalf("PublishStockUpdate = %v, want nil", err)
	}
}

// TestProducer_WithTopic_NoOp mirrors how refresher_test.go uses the producer:
// a Topic set but no client must still be a no-op.
func TestProducer_WithTopic_NoOp(t *testing.T) {
	p := &Producer{Topic: StockUpdateTopic}
	msg := &kafkastockv1.Stock{Symbol: "AC", Exchange: "TSX"}
	if err := p.PublishStockUpdate(context.Background(), msg); err != nil {
		t.Fatalf("PublishStockUpdate = %v, want nil", err)
	}
}

// TestNewProducer_EmptyBrokers_NoDial verifies NewProducer with no brokers
// returns a no-op producer with the default topic applied, and never dials.
func TestNewProducer_EmptyBrokers_NoDial(t *testing.T) {
	cfg := &config.Config{RefreshCheckInterval: time.Hour} // KafkaBrokers empty
	p, err := NewProducer(cfg)
	if err != nil {
		t.Fatalf("NewProducer = %v, want nil", err)
	}
	if p == nil {
		t.Fatal("NewProducer = nil producer, want non-nil")
	}
	if got, want := p.Topic, "stock.update.v1"; got != want {
		t.Errorf("p.Topic = %q, want %q", got, want)
	}
	msg := &kafkastockv1.Stock{Symbol: "AC", Exchange: "TSX"}
	if err := p.PublishStockUpdate(context.Background(), msg); err != nil {
		t.Errorf("PublishStockUpdate = %v, want nil (no-op)", err)
	}
}

// fakeSender records every message handed to it so tests can assert on the
// produced kafka-go message without a real broker.
type fakeSender struct {
	sent []kafka.Message
	err  error
}

func (f *fakeSender) WriteMessages(ctx context.Context, msgs ...kafka.Message) error {
	f.sent = append(f.sent, msgs...)
	return f.err
}

func (f *fakeSender) Close() error { return nil }

// TestPublish_WithFakeSender verifies PublishStockUpdate marshals the message
// and forwards exactly one record with the expected topic, key, and value to
// the injected sender.
func TestPublish_WithFakeSender(t *testing.T) {
	fake := &fakeSender{}
	p := &Producer{Topic: "t", client: fake}
	msg := &kafkastockv1.Stock{Symbol: "AC", Exchange: "TSX"}

	if err := p.PublishStockUpdate(context.Background(), msg); err != nil {
		t.Fatalf("PublishStockUpdate = %v, want nil", err)
	}
	if len(fake.sent) != 1 {
		t.Fatalf("WriteMessages called with %d messages, want 1", len(fake.sent))
	}
	r := fake.sent[0]
	if r.Topic != "t" {
		t.Errorf("record topic = %q, want %q", r.Topic, "t")
	}
	if string(r.Key) != "AC" {
		t.Errorf("record key = %q, want %q", string(r.Key), "AC")
	}
	var decoded kafkastockv1.Stock
	if err := proto.Unmarshal(r.Value, &decoded); err != nil {
		t.Fatalf("proto.Unmarshal = %v, want nil", err)
	}
	if decoded.Symbol != "AC" {
		t.Errorf("decoded Symbol = %q, want %q", decoded.Symbol, "AC")
	}
	if decoded.Exchange != "TSX" {
		t.Errorf("decoded Exchange = %q, want %q", decoded.Exchange, "TSX")
	}
}

// TestProducer_Close_NoClient verifies Close is a no-op when Kafka is disabled.
func TestProducer_Close_NoClient(t *testing.T) {
	if err := (&Producer{}).Close(); err != nil {
		t.Fatalf("Close = %v, want nil", err)
	}
}
