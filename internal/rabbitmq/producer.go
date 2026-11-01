// Package rabbitmq provides a producer for publishing stock update messages
// to a RabbitMQ exchange using the Stock protobuf type from stockstorev1.
package rabbitmq

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/url"
	"strings"
	"sync"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
	"google.golang.org/protobuf/proto"

	stockstorev1 "git.wheeli.ca/brian/stocker-store/proto/v1"

	"github.com/anomalyco/stocker-list/internal/config"
)

const (
	// StockUpdateMessageType identifies the stock update schema and version.
	StockUpdateMessageType = "stock.update.v1"

	defaultExchange     = "stock.update"
	defaultExchangeType = "topic"
	defaultRoutingKey   = "stock.update.v1"
)

var errProducerClosed = errors.New("rabbitmq producer is closed")

// connection is the subset of *amqp.Connection the producer uses. It is
// unexported so tests can inject a fake without a broker.
type connection interface {
	Channel() (channel, error)
	Close() error
}

// channel is the subset of *amqp.Channel the producer uses.
type channel interface {
	ExchangeDeclare(name, kind string, durable, autoDelete, internal, noWait bool, args amqp.Table) error
	Confirm(noWait bool) error
	PublishWithDeferredConfirmWithContext(ctx context.Context, exchange, key string, mandatory, immediate bool, msg amqp.Publishing) (confirmation, error)
	IsClosed() bool
	Close() error
}

// confirmation is the subset of *amqp.DeferredConfirmation the producer uses.
type confirmation interface {
	WaitContext(ctx context.Context) (bool, error)
}

// dial opens a connection to the configured AMQP URI.
type dial func(string) (connection, error)

// Producer publishes stock update messages to a RabbitMQ exchange. It connects
// lazily on the first publish and treats a zero value (or an empty URL) as a
// no-op.
type Producer struct {
	exchange     string
	exchangeType string
	routingKey   string
	url          string
	log          *slog.Logger
	dial         dial

	mu     sync.Mutex
	conn   connection
	ch     channel
	closed bool
}

// NewProducer validates cfg and returns a producer. It performs no network
// I/O: the connection is established on the first publish. An empty
// RabbitMQURL returns a disabled producer whose publish and close calls are
// no-ops.
func NewProducer(cfg *config.Config, log *slog.Logger) (*Producer, error) {
	if log == nil {
		log = slog.New(slog.NewTextHandler(io.Discard, nil))
	}

	p := &Producer{
		exchange:     defaultExchange,
		exchangeType: defaultExchangeType,
		routingKey:   defaultRoutingKey,
		log:          log,
		dial:         defaultDial,
	}

	if cfg != nil {
		p.url = cfg.RabbitMQURL
		if cfg.RabbitMQExchange != "" {
			p.exchange = cfg.RabbitMQExchange
		}
		if cfg.RabbitMQExchangeType != "" {
			p.exchangeType = cfg.RabbitMQExchangeType
		}
		if cfg.RabbitMQRoutingKey != "" {
			p.routingKey = cfg.RabbitMQRoutingKey
		}
	}

	kind := strings.ToLower(strings.TrimSpace(p.exchangeType))
	switch kind {
	case "direct", "topic", "fanout", "headers":
		p.exchangeType = kind
	default:
		return nil, fmt.Errorf("RABBITMQ_EXCHANGE_TYPE must be one of direct, topic, fanout, headers, got %q", kind)
	}

	if p.url == "" {
		return p, nil
	}

	if _, err := amqp.ParseURI(p.url); err != nil {
		return nil, fmt.Errorf("parse RABBITMQ_URL: %w", redactURLError(err))
	}

	return p, nil
}

// redactURLError strips the AMQP URI from a ParseURI error. ParseURI failures
// on the URI itself are wrapped in *url.Error, whose message embeds the full
// URI including credentials; only the inner reason is safe to log.
func redactURLError(err error) error {
	var uerr *url.Error
	if errors.As(err, &uerr) {
		return uerr.Err
	}
	return err
}

// PublishStockUpdate marshals msg and publishes it to the configured exchange,
// waiting for a publisher confirm. It is a no-op when the producer is disabled
// or msg is nil. A publish or confirm failure tears down the connection and
// retries once; initial connection failures are not retried.
func (p *Producer) PublishStockUpdate(ctx context.Context, msg *stockstorev1.Stock) error {
	if p == nil || p.url == "" || msg == nil {
		return nil
	}

	body, err := proto.Marshal(msg)
	if err != nil {
		return fmt.Errorf("marshal stock update: %w", err)
	}
	if err := ctx.Err(); err != nil {
		return err
	}

	p.mu.Lock()
	defer p.mu.Unlock()

	if p.closed {
		return errProducerClosed
	}
	if err := ctx.Err(); err != nil {
		return err
	}

	publishing := amqp.Publishing{
		ContentType:  "application/x-protobuf",
		DeliveryMode: amqp.Persistent,
		MessageId:    msg.Symbol,
		Type:         StockUpdateMessageType,
		Timestamp:    time.Now().UTC(),
		Headers: amqp.Table{
			"symbol":   msg.Symbol,
			"exchange": msg.Exchange,
		},
		Body: body,
	}

	if err := p.ensureChannelLocked(ctx); err != nil {
		return err
	}

	publishErr := p.publishOnChannelLocked(ctx, publishing)
	if publishErr == nil {
		return nil
	}
	if err := ctx.Err(); err != nil {
		return err
	}

	p.log.Warn("rabbitmq publish failed; reconnecting and retrying once",
		"symbol", msg.Symbol,
		"exchange", p.exchange,
		"error", publishErr,
	)

	p.closeResourcesLocked()

	if err := p.ensureChannelLocked(ctx); err != nil {
		return fmt.Errorf("reconnect to rabbitmq after publish failure: %w", err)
	}
	if err := p.publishOnChannelLocked(ctx, publishing); err != nil {
		return fmt.Errorf("publish to rabbitmq exchange %q: %w", p.exchange, err)
	}
	return nil
}

// ensureChannelLocked returns a live channel, dialing and declaring topology
// when necessary. The caller must hold p.mu.
func (p *Producer) ensureChannelLocked(ctx context.Context) error {
	if p.ch != nil && !p.ch.IsClosed() {
		return nil
	}
	p.closeResourcesLocked()

	if err := ctx.Err(); err != nil {
		return err
	}

	d := p.dial
	if d == nil {
		d = defaultDial
	}
	conn, err := d(p.url)
	if err != nil {
		return fmt.Errorf("connect to rabbitmq: %w", err)
	}

	ch, err := conn.Channel()
	if err != nil {
		_ = conn.Close()
		return fmt.Errorf("open rabbitmq channel: %w", err)
	}

	if err := ch.ExchangeDeclare(p.exchange, p.exchangeType, true, false, false, false, nil); err != nil {
		_ = ch.Close()
		_ = conn.Close()
		return fmt.Errorf("declare rabbitmq exchange %q: %w", p.exchange, err)
	}

	if err := ch.Confirm(false); err != nil {
		_ = ch.Close()
		_ = conn.Close()
		return fmt.Errorf("enable publisher confirms: %w", err)
	}

	p.conn = conn
	p.ch = ch
	p.log.Info("connected to rabbitmq",
		"exchange", p.exchange,
		"exchange_type", p.exchangeType,
		"routing_key", p.routingKey,
	)
	return nil
}

// publishOnChannelLocked publishes one message and waits for its confirm. The
// caller must hold p.mu and have a live channel.
func (p *Producer) publishOnChannelLocked(ctx context.Context, publishing amqp.Publishing) error {
	confirm, err := p.ch.PublishWithDeferredConfirmWithContext(
		ctx, p.exchange, p.routingKey, false, false, publishing,
	)
	if err != nil {
		return err
	}
	if confirm == nil {
		return errors.New("publisher confirms not enabled")
	}

	acked, err := confirm.WaitContext(ctx)
	if err != nil {
		return err
	}
	if !acked {
		return errors.New("rabbitmq publish was not acknowledged")
	}
	return nil
}

// closeResourcesLocked closes and clears the current channel and connection.
// The caller must hold p.mu.
func (p *Producer) closeResourcesLocked() error {
	var errs []error
	if p.ch != nil {
		if err := p.ch.Close(); err != nil {
			errs = append(errs, err)
		}
		p.ch = nil
	}
	if p.conn != nil {
		if err := p.conn.Close(); err != nil {
			errs = append(errs, err)
		}
		p.conn = nil
	}
	return errors.Join(errs...)
}

// Close closes the channel and connection. It is idempotent and safe on a
// disabled or zero-value producer.
func (p *Producer) Close() error {
	if p == nil {
		return nil
	}

	p.mu.Lock()
	defer p.mu.Unlock()

	if p.closed {
		return nil
	}
	p.closed = true

	return p.closeResourcesLocked()
}

// amqpConnection adapts *amqp.Connection to the connection seam.
type amqpConnection struct {
	conn *amqp.Connection
}

func (c amqpConnection) Channel() (channel, error) {
	ch, err := c.conn.Channel()
	if err != nil {
		return nil, err
	}
	return amqpChannel{ch: ch}, nil
}

func (c amqpConnection) Close() error {
	return c.conn.Close()
}

// amqpChannel adapts *amqp.Channel to the channel seam.
type amqpChannel struct {
	ch *amqp.Channel
}

func (c amqpChannel) ExchangeDeclare(name, kind string, durable, autoDelete, internal, noWait bool, args amqp.Table) error {
	return c.ch.ExchangeDeclare(name, kind, durable, autoDelete, internal, noWait, args)
}

func (c amqpChannel) Confirm(noWait bool) error {
	return c.ch.Confirm(noWait)
}

func (c amqpChannel) PublishWithDeferredConfirmWithContext(ctx context.Context, exchange, key string, mandatory, immediate bool, msg amqp.Publishing) (confirmation, error) {
	confirm, err := c.ch.PublishWithDeferredConfirmWithContext(ctx, exchange, key, mandatory, immediate, msg)
	if err != nil {
		return nil, err
	}
	return confirm, nil
}

func (c amqpChannel) IsClosed() bool {
	return c.ch.IsClosed()
}

func (c amqpChannel) Close() error {
	return c.ch.Close()
}

// defaultDial is the production dial function. It uses the library's default
// connection and handshake timeout.
func defaultDial(url string) (connection, error) {
	conn, err := amqp.Dial(url)
	if err != nil {
		return nil, err
	}
	return amqpConnection{conn: conn}, nil
}
