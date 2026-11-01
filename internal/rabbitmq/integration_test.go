package rabbitmq_test

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
	"google.golang.org/protobuf/proto"

	stockstorev1 "git.wheeli.ca/brian/stocker-store/proto/v1"

	"github.com/anomalyco/stocker-list/internal/config"
	"github.com/anomalyco/stocker-list/internal/rabbitmq"
)

// TestProducerIntegration round-trips a Stock message through a real broker.
// It is skipped unless RABBITMQ_TEST_URL is set, so the default test run never
// touches the network.
func TestProducerIntegration(t *testing.T) {
	url := os.Getenv("RABBITMQ_TEST_URL")
	if url == "" {
		t.Skip("RABBITMQ_TEST_URL not set; skipping RabbitMQ integration test")
	}

	suffix := fmt.Sprintf("%d-%d", time.Now().UnixNano(), os.Getpid())
	exchange := "stocker-list-test-" + suffix
	routingKey := "stock.update.test." + suffix

	conn, err := amqp.Dial(url)
	if err != nil {
		t.Fatalf("dial rabbitmq: %v", err)
	}
	defer conn.Close()

	ch, err := conn.Channel()
	if err != nil {
		t.Fatalf("open channel: %v", err)
	}
	defer ch.Close()

	if err := ch.ExchangeDeclare(exchange, "topic", true, false, false, false, nil); err != nil {
		t.Fatalf("declare exchange: %v", err)
	}
	defer func() {
		if err := ch.ExchangeDelete(exchange, false, false); err != nil {
			t.Logf("delete exchange %q: %v", exchange, err)
		}
	}()

	queue, err := ch.QueueDeclare("", false, true, true, false, nil)
	if err != nil {
		t.Fatalf("declare queue: %v", err)
	}
	if err := ch.QueueBind(queue.Name, routingKey, exchange, false, nil); err != nil {
		t.Fatalf("bind queue: %v", err)
	}

	deliveries, err := ch.Consume(queue.Name, "", true, true, false, false, nil)
	if err != nil {
		t.Fatalf("consume: %v", err)
	}

	producer, err := rabbitmq.NewProducer(&config.Config{
		RabbitMQURL:          url,
		RabbitMQExchange:     exchange,
		RabbitMQExchangeType: "topic",
		RabbitMQRoutingKey:   routingKey,
	}, nil)
	if err != nil {
		t.Fatalf("NewProducer: %v", err)
	}
	defer func() {
		if err := producer.Close(); err != nil {
			t.Logf("close producer: %v", err)
		}
	}()

	want := &stockstorev1.Stock{Symbol: "AC", Exchange: "TSX"}
	if err := producer.PublishStockUpdate(context.Background(), want); err != nil {
		t.Fatalf("PublishStockUpdate: %v", err)
	}

	var delivery amqp.Delivery
	select {
	case delivery = <-deliveries:
	case <-time.After(10 * time.Second):
		t.Fatal("timed out waiting for a stock update message")
	}

	if delivery.Exchange != exchange {
		t.Errorf("Exchange = %q, want %q", delivery.Exchange, exchange)
	}
	if delivery.RoutingKey != routingKey {
		t.Errorf("RoutingKey = %q, want %q", delivery.RoutingKey, routingKey)
	}
	if delivery.ContentType != "application/x-protobuf" {
		t.Errorf("ContentType = %q, want %q", delivery.ContentType, "application/x-protobuf")
	}
	if delivery.ContentEncoding != "" {
		t.Errorf("ContentEncoding = %q, want empty", delivery.ContentEncoding)
	}
	if delivery.DeliveryMode != amqp.Persistent {
		t.Errorf("DeliveryMode = %d, want %d", delivery.DeliveryMode, amqp.Persistent)
	}
	if delivery.MessageId != "AC" {
		t.Errorf("MessageId = %q, want %q", delivery.MessageId, "AC")
	}
	if delivery.Type != rabbitmq.StockUpdateMessageType {
		t.Errorf("Type = %q, want %q", delivery.Type, rabbitmq.StockUpdateMessageType)
	}
	if delivery.Timestamp.IsZero() {
		t.Error("Timestamp is zero")
	}
	if age := time.Since(delivery.Timestamp); age < -time.Minute || age > time.Minute {
		t.Errorf("Timestamp = %v, want within a minute of now", delivery.Timestamp)
	}
	if got := delivery.Headers["symbol"]; got != "AC" {
		t.Errorf("Headers[symbol] = %v, want %q", got, "AC")
	}
	if got := delivery.Headers["exchange"]; got != "TSX" {
		t.Errorf("Headers[exchange] = %v, want %q", got, "TSX")
	}

	var got stockstorev1.Stock
	if err := proto.Unmarshal(delivery.Body, &got); err != nil {
		t.Fatalf("proto.Unmarshal: %v", err)
	}
	if !proto.Equal(&got, want) {
		t.Errorf("decoded = %v, want %v", &got, want)
	}
}
