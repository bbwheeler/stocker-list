package rabbitmq

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"strings"
	"testing"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
	"google.golang.org/protobuf/proto"

	stockstorev1 "git.wheeli.ca/brian/stocker-store/proto/v1"

	"github.com/anomalyco/stocker-list/internal/config"
)

// --- fakes -----------------------------------------------------------------

type fakeConfirmation struct {
	acked bool
	err   error
}

func (c *fakeConfirmation) WaitContext(ctx context.Context) (bool, error) {
	return c.acked, c.err
}

type declareCall struct {
	name       string
	kind       string
	durable    bool
	autoDelete bool
	internal   bool
	noWait     bool
	args       amqp.Table
}

type publishCall struct {
	exchange  string
	key       string
	mandatory bool
	immediate bool
	msg       amqp.Publishing
}

type fakeChannel struct {
	declares     []declareCall
	confirmCalls int
	publishes    []publishCall
	publishFn    func(ctx context.Context, call publishCall) (confirmation, error)
	closed       bool
	closeCalls   int
	closeErr     error
}

func (c *fakeChannel) ExchangeDeclare(name, kind string, durable, autoDelete, internal, noWait bool, args amqp.Table) error {
	c.declares = append(c.declares, declareCall{
		name:       name,
		kind:       kind,
		durable:    durable,
		autoDelete: autoDelete,
		internal:   internal,
		noWait:     noWait,
		args:       args,
	})
	return nil
}

func (c *fakeChannel) Confirm(noWait bool) error {
	c.confirmCalls++
	return nil
}

func (c *fakeChannel) PublishWithDeferredConfirmWithContext(ctx context.Context, exchange, key string, mandatory, immediate bool, msg amqp.Publishing) (confirmation, error) {
	call := publishCall{
		exchange:  exchange,
		key:       key,
		mandatory: mandatory,
		immediate: immediate,
		msg:       msg,
	}
	c.publishes = append(c.publishes, call)
	if c.publishFn != nil {
		return c.publishFn(ctx, call)
	}
	return &fakeConfirmation{acked: true}, nil
}

func (c *fakeChannel) IsClosed() bool { return c.closed }

func (c *fakeChannel) Close() error {
	c.closeCalls++
	c.closed = true
	return c.closeErr
}

type fakeConnection struct {
	channel    channel
	channelErr error
	closeCalls int
	closeErr   error
}

func (c *fakeConnection) Channel() (channel, error) {
	if c.channelErr != nil {
		return nil, c.channelErr
	}
	return c.channel, nil
}

func (c *fakeConnection) Close() error {
	c.closeCalls++
	return c.closeErr
}

type dialResult struct {
	conn connection
	err  error
}

type fakeDialer struct {
	calls   int
	urls    []string
	results []dialResult
}

func (d *fakeDialer) dial(url string) (connection, error) {
	d.urls = append(d.urls, url)
	i := d.calls
	d.calls++
	if i < len(d.results) {
		return d.results[i].conn, d.results[i].err
	}
	return &fakeConnection{channel: &fakeChannel{}}, nil
}

// --- helpers ---------------------------------------------------------------

func discardLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

func newEnabledProducer(t *testing.T, d *fakeDialer) *Producer {
	t.Helper()
	cfg := &config.Config{
		RabbitMQURL:          "amqp://guest:guest@localhost:5672/",
		RabbitMQExchange:     "stock.update",
		RabbitMQExchangeType: "topic",
		RabbitMQRoutingKey:   "stock.update.v1",
	}
	p, err := NewProducer(cfg, discardLogger())
	if err != nil {
		t.Fatalf("NewProducer: %v", err)
	}
	p.dial = d.dial
	return p
}

func oneConnection(ch *fakeChannel) *fakeDialer {
	return &fakeDialer{results: []dialResult{{conn: &fakeConnection{channel: ch}}}}
}

func testMessage() *stockstorev1.Stock {
	return &stockstorev1.Stock{Symbol: "AC", Exchange: "TSX"}
}

// --- construction and no-op behaviour --------------------------------------

func TestDisabledProducerNoOp(t *testing.T) {
	d := &fakeDialer{}
	p, err := NewProducer(&config.Config{}, discardLogger())
	if err != nil {
		t.Fatalf("NewProducer: %v", err)
	}
	p.dial = d.dial

	if err := p.PublishStockUpdate(context.Background(), testMessage()); err != nil {
		t.Errorf("PublishStockUpdate = %v, want nil", err)
	}
	if err := p.Close(); err != nil {
		t.Errorf("Close = %v, want nil", err)
	}
	if d.calls != 0 {
		t.Errorf("dial called %d times, want 0", d.calls)
	}
}

func TestZeroValueProducerNoOp(t *testing.T) {
	var p Producer
	if err := p.PublishStockUpdate(context.Background(), testMessage()); err != nil {
		t.Errorf("PublishStockUpdate = %v, want nil", err)
	}
	if err := p.Close(); err != nil {
		t.Errorf("Close = %v, want nil", err)
	}
}

func TestNilMessageNoOp(t *testing.T) {
	d := &fakeDialer{}
	p := newEnabledProducer(t, d)

	if err := p.PublishStockUpdate(context.Background(), nil); err != nil {
		t.Errorf("PublishStockUpdate = %v, want nil", err)
	}
	if d.calls != 0 {
		t.Errorf("dial called %d times, want 0", d.calls)
	}
}

func TestNewProducerDefaults(t *testing.T) {
	p, err := NewProducer(&config.Config{RabbitMQURL: ""}, discardLogger())
	if err != nil {
		t.Fatalf("NewProducer: %v", err)
	}
	if p.exchange != defaultExchange {
		t.Errorf("exchange = %q, want %q", p.exchange, defaultExchange)
	}
	if p.exchangeType != defaultExchangeType {
		t.Errorf("exchangeType = %q, want %q", p.exchangeType, defaultExchangeType)
	}
	if p.routingKey != defaultRoutingKey {
		t.Errorf("routingKey = %q, want %q", p.routingKey, defaultRoutingKey)
	}
}

func TestNewProducerNormalizesExchangeType(t *testing.T) {
	p, err := NewProducer(&config.Config{
		RabbitMQURL:          "amqp://guest:guest@localhost:5672/",
		RabbitMQExchangeType: "  TOPIC  ",
	}, discardLogger())
	if err != nil {
		t.Fatalf("NewProducer: %v", err)
	}
	if p.exchangeType != "topic" {
		t.Errorf("exchangeType = %q, want %q", p.exchangeType, "topic")
	}
}

func TestNewProducerInvalidURL(t *testing.T) {
	_, err := NewProducer(&config.Config{RabbitMQURL: "http://localhost:5672/"}, discardLogger())
	if err == nil {
		t.Fatal("NewProducer = nil error, want invalid URL error")
	}
	if !strings.Contains(err.Error(), "parse RABBITMQ_URL") {
		t.Errorf("error = %q, want it to contain %q", err, "parse RABBITMQ_URL")
	}
}

func TestNewProducerInvalidExchangeType(t *testing.T) {
	tests := []struct {
		name string
		url  string
	}{
		{name: "enabled", url: "amqp://guest:guest@localhost:5672/"},
		{name: "disabled", url: ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := NewProducer(&config.Config{
				RabbitMQURL:          tt.url,
				RabbitMQExchangeType: "bogus",
			}, discardLogger())
			if err == nil {
				t.Fatal("NewProducer = nil error, want invalid exchange type error")
			}
			if !strings.Contains(err.Error(), "RABBITMQ_EXCHANGE_TYPE") {
				t.Errorf("error = %q, want it to name RABBITMQ_EXCHANGE_TYPE", err)
			}
		})
	}
}

// --- topology and message mapping -----------------------------------------

func TestExchangeDeclaration(t *testing.T) {
	ch := &fakeChannel{}
	d := oneConnection(ch)
	p := newEnabledProducer(t, d)

	if err := p.PublishStockUpdate(context.Background(), testMessage()); err != nil {
		t.Fatalf("PublishStockUpdate: %v", err)
	}
	if len(ch.declares) != 1 {
		t.Fatalf("ExchangeDeclare called %d times, want 1", len(ch.declares))
	}
	got := ch.declares[0]
	if got.name != "stock.update" {
		t.Errorf("exchange name = %q, want %q", got.name, "stock.update")
	}
	if got.kind != "topic" {
		t.Errorf("exchange kind = %q, want %q", got.kind, "topic")
	}
	if !got.durable {
		t.Error("durable = false, want true")
	}
	if got.autoDelete {
		t.Error("autoDelete = true, want false")
	}
	if got.internal {
		t.Error("internal = true, want false")
	}
	if got.noWait {
		t.Error("noWait = true, want false")
	}
	if got.args != nil {
		t.Errorf("args = %v, want nil", got.args)
	}
}

func TestConfirmsEnabledOncePerConnection(t *testing.T) {
	ch := &fakeChannel{}
	d := oneConnection(ch)
	p := newEnabledProducer(t, d)

	for i := 0; i < 2; i++ {
		if err := p.PublishStockUpdate(context.Background(), testMessage()); err != nil {
			t.Fatalf("PublishStockUpdate #%d: %v", i+1, err)
		}
	}
	if ch.confirmCalls != 1 {
		t.Errorf("Confirm called %d times, want 1", ch.confirmCalls)
	}
	if d.calls != 1 {
		t.Errorf("dial called %d times, want 1", d.calls)
	}
}

func TestPublishMessageMetadata(t *testing.T) {
	ch := &fakeChannel{}
	d := oneConnection(ch)
	p := newEnabledProducer(t, d)

	msg := testMessage()
	before := time.Now().UTC()
	if err := p.PublishStockUpdate(context.Background(), msg); err != nil {
		t.Fatalf("PublishStockUpdate: %v", err)
	}
	after := time.Now().UTC()

	if len(ch.publishes) != 1 {
		t.Fatalf("publish called %d times, want 1", len(ch.publishes))
	}
	call := ch.publishes[0]
	if call.exchange != "stock.update" {
		t.Errorf("exchange = %q, want %q", call.exchange, "stock.update")
	}
	if call.key != "stock.update.v1" {
		t.Errorf("routing key = %q, want %q", call.key, "stock.update.v1")
	}
	if call.mandatory || call.immediate {
		t.Errorf("mandatory/immediate = %v/%v, want false/false", call.mandatory, call.immediate)
	}

	pub := call.msg
	if pub.ContentType != "application/x-protobuf" {
		t.Errorf("ContentType = %q, want %q", pub.ContentType, "application/x-protobuf")
	}
	if pub.ContentEncoding != "" {
		t.Errorf("ContentEncoding = %q, want empty", pub.ContentEncoding)
	}
	if pub.DeliveryMode != amqp.Persistent {
		t.Errorf("DeliveryMode = %d, want %d", pub.DeliveryMode, amqp.Persistent)
	}
	if pub.MessageId != "AC" {
		t.Errorf("MessageId = %q, want %q", pub.MessageId, "AC")
	}
	if pub.Type != StockUpdateMessageType {
		t.Errorf("Type = %q, want %q", pub.Type, StockUpdateMessageType)
	}
	if pub.Priority != 0 {
		t.Errorf("Priority = %d, want 0", pub.Priority)
	}
	if pub.CorrelationId != "" || pub.ReplyTo != "" || pub.Expiration != "" {
		t.Errorf("CorrelationId/ReplyTo/Expiration = %q/%q/%q, want empty", pub.CorrelationId, pub.ReplyTo, pub.Expiration)
	}
	if pub.Timestamp.IsZero() {
		t.Fatal("Timestamp is zero")
	}
	if pub.Timestamp.Location() != time.UTC {
		t.Errorf("Timestamp location = %v, want UTC", pub.Timestamp.Location())
	}
	if pub.Timestamp.Before(before) || pub.Timestamp.After(after) {
		t.Errorf("Timestamp = %v, want between %v and %v", pub.Timestamp, before, after)
	}
	if got := pub.Headers["symbol"]; got != "AC" {
		t.Errorf("Headers[symbol] = %v, want %q", got, "AC")
	}
	if got := pub.Headers["exchange"]; got != "TSX" {
		t.Errorf("Headers[exchange] = %v, want %q", got, "TSX")
	}

	var decoded stockstorev1.Stock
	if err := proto.Unmarshal(pub.Body, &decoded); err != nil {
		t.Fatalf("proto.Unmarshal: %v", err)
	}
	if !proto.Equal(&decoded, msg) {
		t.Errorf("decoded = %v, want %v", &decoded, msg)
	}
}

// --- publish failures and reconnect ---------------------------------------

func TestRetryOnceOnPublishError(t *testing.T) {
	failCh := &fakeChannel{publishFn: func(ctx context.Context, call publishCall) (confirmation, error) {
		return nil, errors.New("boom")
	}}
	okCh := &fakeChannel{}
	d := &fakeDialer{results: []dialResult{
		{conn: &fakeConnection{channel: failCh}},
		{conn: &fakeConnection{channel: okCh}},
	}}
	p := newEnabledProducer(t, d)

	if err := p.PublishStockUpdate(context.Background(), testMessage()); err != nil {
		t.Fatalf("PublishStockUpdate = %v, want nil after successful retry", err)
	}
	if d.calls != 2 {
		t.Errorf("dial called %d times, want 2", d.calls)
	}
	if len(failCh.publishes) != 1 {
		t.Errorf("first channel published %d times, want 1", len(failCh.publishes))
	}
	if len(okCh.publishes) != 1 {
		t.Errorf("second channel published %d times, want 1", len(okCh.publishes))
	}
	if failCh.closeCalls != 1 {
		t.Errorf("first channel closed %d times, want 1", failCh.closeCalls)
	}
}

func TestTwoConsecutiveFailures(t *testing.T) {
	firstErr := errors.New("first failure")
	secondErr := errors.New("second failure")
	failCh1 := &fakeChannel{publishFn: func(ctx context.Context, call publishCall) (confirmation, error) {
		return nil, firstErr
	}}
	failCh2 := &fakeChannel{publishFn: func(ctx context.Context, call publishCall) (confirmation, error) {
		return nil, secondErr
	}}
	d := &fakeDialer{results: []dialResult{
		{conn: &fakeConnection{channel: failCh1}},
		{conn: &fakeConnection{channel: failCh2}},
	}}
	p := newEnabledProducer(t, d)

	err := p.PublishStockUpdate(context.Background(), testMessage())
	if err == nil {
		t.Fatal("PublishStockUpdate = nil, want error")
	}
	if !strings.Contains(err.Error(), "publish to rabbitmq exchange") {
		t.Errorf("error = %q, want publish context", err)
	}
	if !strings.Contains(err.Error(), "second failure") {
		t.Errorf("error = %q, want it to wrap the second failure", err)
	}
	if strings.Contains(err.Error(), "first failure") {
		t.Errorf("error = %q, want it not to wrap the first failure", err)
	}
	if d.calls != 2 {
		t.Errorf("dial called %d times, want 2 (no third attempt)", d.calls)
	}
	if len(failCh1.publishes) != 1 || len(failCh2.publishes) != 1 {
		t.Errorf("publish counts = %d/%d, want 1/1", len(failCh1.publishes), len(failCh2.publishes))
	}
}

func TestNonAckedConfirmTriggersRetry(t *testing.T) {
	nackCh := &fakeChannel{publishFn: func(ctx context.Context, call publishCall) (confirmation, error) {
		return &fakeConfirmation{acked: false}, nil
	}}
	okCh := &fakeChannel{}
	d := &fakeDialer{results: []dialResult{
		{conn: &fakeConnection{channel: nackCh}},
		{conn: &fakeConnection{channel: okCh}},
	}}
	p := newEnabledProducer(t, d)

	if err := p.PublishStockUpdate(context.Background(), testMessage()); err != nil {
		t.Fatalf("PublishStockUpdate = %v, want nil after successful retry", err)
	}
	if d.calls != 2 {
		t.Errorf("dial called %d times, want 2", d.calls)
	}
	if len(nackCh.publishes) != 1 || len(okCh.publishes) != 1 {
		t.Errorf("publish counts = %d/%d, want 1/1", len(nackCh.publishes), len(okCh.publishes))
	}
}

func TestInitialDialErrorNotRetried(t *testing.T) {
	d := &fakeDialer{results: []dialResult{{err: errors.New("dial boom")}}}
	p := newEnabledProducer(t, d)

	err := p.PublishStockUpdate(context.Background(), testMessage())
	if err == nil {
		t.Fatal("PublishStockUpdate = nil, want dial error")
	}
	if !strings.Contains(err.Error(), "connect to rabbitmq") {
		t.Errorf("error = %q, want connect context", err)
	}
	if d.calls != 1 {
		t.Errorf("dial called %d times, want 1 (initial dial is not retried)", d.calls)
	}
}

func TestReconnectFailureAfterPublishFailure(t *testing.T) {
	failCh := &fakeChannel{publishFn: func(ctx context.Context, call publishCall) (confirmation, error) {
		return nil, errors.New("publish boom")
	}}
	d := &fakeDialer{results: []dialResult{
		{conn: &fakeConnection{channel: failCh}},
		{err: errors.New("dial boom")},
	}}
	p := newEnabledProducer(t, d)

	err := p.PublishStockUpdate(context.Background(), testMessage())
	if err == nil {
		t.Fatal("PublishStockUpdate = nil, want reconnect error")
	}
	if !strings.Contains(err.Error(), "reconnect to rabbitmq after publish failure") {
		t.Errorf("error = %q, want reconnect context", err)
	}
	if d.calls != 2 {
		t.Errorf("dial called %d times, want 2", d.calls)
	}
	if len(failCh.publishes) != 1 {
		t.Errorf("publish called %d times, want 1", len(failCh.publishes))
	}
}

func TestClosedChannelReconnects(t *testing.T) {
	ch1 := &fakeChannel{}
	ch2 := &fakeChannel{}
	conn1 := &fakeConnection{channel: ch1}
	d := &fakeDialer{results: []dialResult{
		{conn: conn1},
		{conn: &fakeConnection{channel: ch2}},
	}}
	p := newEnabledProducer(t, d)

	if err := p.PublishStockUpdate(context.Background(), testMessage()); err != nil {
		t.Fatalf("first PublishStockUpdate: %v", err)
	}
	ch1.closed = true

	if err := p.PublishStockUpdate(context.Background(), testMessage()); err != nil {
		t.Fatalf("second PublishStockUpdate: %v", err)
	}
	if d.calls != 2 {
		t.Errorf("dial called %d times, want 2", d.calls)
	}
	if len(ch1.publishes) != 1 {
		t.Errorf("closed channel published %d times, want 1", len(ch1.publishes))
	}
	if len(ch2.publishes) != 1 {
		t.Errorf("new channel published %d times, want 1", len(ch2.publishes))
	}
	if ch1.closeCalls != 1 {
		t.Errorf("stale channel closed %d times, want 1", ch1.closeCalls)
	}
	if conn1.closeCalls != 1 {
		t.Errorf("stale connection closed %d times, want 1", conn1.closeCalls)
	}
}

// --- context cancellation --------------------------------------------------

func TestCanceledContextNoRetryNoDial(t *testing.T) {
	d := &fakeDialer{}
	p := newEnabledProducer(t, d)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	err := p.PublishStockUpdate(ctx, testMessage())
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("PublishStockUpdate = %v, want context.Canceled", err)
	}
	if d.calls != 0 {
		t.Errorf("dial called %d times, want 0", d.calls)
	}
}

func TestCanceledContextAfterPublishFailureDoesNotRetry(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	failCh := &fakeChannel{publishFn: func(ctx context.Context, call publishCall) (confirmation, error) {
		cancel()
		return nil, errors.New("publish boom")
	}}
	d := oneConnection(failCh)
	p := newEnabledProducer(t, d)

	err := p.PublishStockUpdate(ctx, testMessage())
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("PublishStockUpdate = %v, want context.Canceled", err)
	}
	if d.calls != 1 {
		t.Errorf("dial called %d times, want 1 (no retry)", d.calls)
	}
	if len(failCh.publishes) != 1 {
		t.Errorf("publish called %d times, want 1 (no retry)", len(failCh.publishes))
	}
}

// --- close -----------------------------------------------------------------

func TestCloseIdempotent(t *testing.T) {
	ch := &fakeChannel{}
	conn := &fakeConnection{channel: ch}
	d := &fakeDialer{results: []dialResult{{conn: conn}}}
	p := newEnabledProducer(t, d)

	if err := p.PublishStockUpdate(context.Background(), testMessage()); err != nil {
		t.Fatalf("PublishStockUpdate: %v", err)
	}
	if err := p.Close(); err != nil {
		t.Fatalf("first Close = %v, want nil", err)
	}
	if err := p.Close(); err != nil {
		t.Fatalf("second Close = %v, want nil", err)
	}
	if ch.closeCalls != 1 {
		t.Errorf("channel closed %d times, want 1", ch.closeCalls)
	}
	if conn.closeCalls != 1 {
		t.Errorf("connection closed %d times, want 1", conn.closeCalls)
	}
	if d.calls != 1 {
		t.Errorf("dial called %d times, want 1", d.calls)
	}
}

func TestCloseJoinsErrors(t *testing.T) {
	chErr := errors.New("channel close")
	connErr := errors.New("connection close")
	ch := &fakeChannel{closeErr: chErr}
	conn := &fakeConnection{channel: ch, closeErr: connErr}
	d := &fakeDialer{results: []dialResult{{conn: conn}}}
	p := newEnabledProducer(t, d)

	if err := p.PublishStockUpdate(context.Background(), testMessage()); err != nil {
		t.Fatalf("PublishStockUpdate: %v", err)
	}
	err := p.Close()
	if !errors.Is(err, chErr) {
		t.Errorf("Close = %v, want it to include the channel error", err)
	}
	if !errors.Is(err, connErr) {
		t.Errorf("Close = %v, want it to include the connection error", err)
	}
}

func TestPublishAfterClose(t *testing.T) {
	ch := &fakeChannel{}
	d := oneConnection(ch)
	p := newEnabledProducer(t, d)

	if err := p.PublishStockUpdate(context.Background(), testMessage()); err != nil {
		t.Fatalf("PublishStockUpdate: %v", err)
	}
	if err := p.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	err := p.PublishStockUpdate(context.Background(), testMessage())
	if !errors.Is(err, errProducerClosed) {
		t.Fatalf("PublishStockUpdate = %v, want errProducerClosed", err)
	}
	if got, want := err.Error(), "rabbitmq producer is closed"; got != want {
		t.Errorf("error = %q, want %q", got, want)
	}
	if d.calls != 1 {
		t.Errorf("dial called %d times, want 1", d.calls)
	}
}

func TestDisabledProducerCloseAfterCloseStillNoOp(t *testing.T) {
	p, err := NewProducer(&config.Config{RabbitMQURL: ""}, discardLogger())
	if err != nil {
		t.Fatalf("NewProducer: %v", err)
	}
	if err := p.Close(); err != nil {
		t.Fatalf("first Close = %v, want nil", err)
	}
	if err := p.PublishStockUpdate(context.Background(), testMessage()); err != nil {
		t.Errorf("PublishStockUpdate after Close = %v, want nil", err)
	}
}
