package amqpwrapper

import (
	"context"
	"encoding/json"
	"github.com/astreter/amqpwrapper/v2/otelamqp"
	"github.com/pkg/errors"
	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/sirupsen/logrus"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
	"sync"
	"time"
)

const (
	connectionTries     = 3
	resendTries         = 3
	heartbeatTimeout    = 60 * time.Second
	defaultLocale       = "en_US"
	defaultThreadsCount = 3
)

type RabbitChannel struct {
	ctx              context.Context
	mu               sync.RWMutex
	cancel           chan bool
	waitGroup        *sync.WaitGroup
	conn             *amqp.Connection
	channel          *amqp.Channel
	url              string
	errorConnection  chan error
	notifyConfirm    chan amqp.Confirmation
	closed           bool
	consumers        map[string]consumer
	reconnecting     bool
	reconnectMutex   sync.RWMutex
	confirmSendsMode bool
	tracer           trace.Tracer
	heartbeat        time.Duration
	locale           string
}

type MessageListener func(ctx context.Context, delivery amqp.Delivery) error

type ErrRequeue struct {
	error
}

type consumer struct {
	exchangeName     string
	callback         MessageListener
	version          int
	availableThreads chan bool
}

type Config struct {
	URL          string
	Host         string
	Port         string
	User         string
	Password     string
	Vhost        string
	Debug        bool
	ConfirmSends bool
	Heartbeat    time.Duration
	Locale       string
}

type Header struct {
	Key   string
	Value string
}

type option struct {
	Name  string
	Value interface{}
}

func NewRabbitChannel(parentCtx context.Context, wg *sync.WaitGroup, cfg *Config) (*RabbitChannel, error) {
	ch := new(RabbitChannel)

	if cfg.Debug {
		logrus.SetLevel(logrus.DebugLevel)
	}

	url := cfg.URL
	logrus.Debug("RabbitMQ.URL: ", url)
	if url == "" {
		logrus.Info("RabbitMQ.URL not found, building from components")
		url = "amqp://" + cfg.User + ":" + cfg.Password + "@" + cfg.Host + ":" + cfg.Port
		if cfg.Vhost != "" {
			url = url + cfg.Vhost
		}
		logrus.Debug("RabbitMQ.URL: ", url)
	}

	ch.ctx = context.WithValue(parentCtx, "rabbitMQ channel ctx", nil)
	ch.cancel = make(chan bool)
	ch.waitGroup = wg
	ch.consumers = make(map[string]consumer)
	ch.reconnecting = false
	ch.url = url
	ch.errorConnection = make(chan error)
	ch.confirmSendsMode = cfg.ConfirmSends
	ch.tracer = otel.Tracer("amqpwrapper")

	if cfg.Heartbeat != 0 {
		ch.heartbeat = cfg.Heartbeat
	} else {
		ch.heartbeat = heartbeatTimeout
	}

	if cfg.Locale != "" {
		ch.locale = cfg.Locale
	} else {
		ch.locale = defaultLocale
	}

	err := ch.connect()
	if err != nil {
		return nil, err
	}
	go ch.reconnect()

	return ch, nil
}

func (ch *RabbitChannel) SetTracer(trace trace.Tracer) {
	ch.tracer = trace
}

func (ch *RabbitChannel) DefineExchange(exchangeName string, isAlreadyExist bool) error {
	var err error
	if isAlreadyExist {
		err = ch.channel.ExchangeDeclarePassive(
			exchangeName, // name
			"topic",      // type
			true,         // durable
			false,        // auto-deleted
			false,        // internal
			false,        // no-wait
			nil,          // arguments
		)
	} else {
		err = ch.channel.ExchangeDeclare(
			exchangeName, // name
			"topic",      // type
			true,         // durable
			false,        // auto-deleted
			false,        // internal
			false,        // no-wait
			nil,          // arguments
		)
	}
	if err != nil {
		return errors.Wrap(err, "RabbitMQ: failed to declare an exchange")
	}

	logrus.Debug("RabbitMQ: exchange `" + exchangeName + "` is declared")
	return nil
}

func (ch *RabbitChannel) Publish(
	ctx context.Context,
	message interface{},
	exchangeName,
	routingKey string,
	headers ...Header,
) error {
	headersTable := make(amqp.Table)

	ctx, span := ch.tracer.Start(ctx, "Publish", trace.WithSpanKind(trace.SpanKindProducer))
	defer span.End()

	body, err := json.Marshal(message)
	if err != nil {
		err = errors.Wrap(err, "RabbitMQ: failed to encode message")
		span.RecordError(err)
		logrus.Error(err)
		return err
	}

	span.SetAttributes(
		attribute.String("exchange", exchangeName),
		attribute.String("queue", routingKey),
		attribute.String("message body", string(body)),
	)

	if ch.closed {
		err = errors.New("rabbitMQ: failed to publish a message: connection is lost")
		span.RecordError(err)
		return err
	}

	if len(headers) > 0 {
		for _, row := range headers {
			headersTable[row.Key] = row.Value
		}
	}

	msg := amqp.Publishing{
		Headers:     headersTable,
		ContentType: "application/json",
		Body:        body,
	}

	otel.GetTextMapPropagator().Inject(ctx, otelamqp.NewPublisherMessageCarrier(&msg))

	tries := resendTries
	for {
		if tries == 0 {
			err = errors.New("RabbitMQ: too many attempts to publish a message")
			span.RecordError(err)
			logrus.Error(err)
			return err
		}
		if ch.reconnecting {
			ch.reconnectMutex.RLock()
			ch.reconnectMutex.RUnlock()
		}
		span.AddEvent("send message to RabbitMQ")
		err = ch.channel.Publish(
			exchangeName, // exchange
			routingKey,   // routing key
			false,        // mandatory
			false,        // immediate
			msg,
		)

		if err != nil {
			if err == amqp.ErrClosed && tries != 0 {
				time.Sleep(time.Millisecond * 300)
				tries -= 1
				continue
			}
			err = errors.Wrap(err, "RabbitMQ: failed to publish a message")
			span.RecordError(err)
			logrus.Error(err)
			return err
		}
		if ch.confirmSendsMode {
			span.AddEvent("waiting for confirmation", trace.WithAttributes(attribute.String("queue", routingKey)))
			logrus.WithField("queue", routingKey).Debug("waiting for confirmation")
			select {
			case confirm := <-ch.notifyConfirm:
				if !confirm.Ack {
					err = errors.New("rabbitMQ: failed to publish a message: delivery is not acknowledged")
					span.RecordError(err)
					logrus.Error(err)
					return err
				} else {
					note := "message successfully published"
					span.AddEvent(note, trace.WithAttributes(
						attribute.String("queue", routingKey),
						attribute.Int("delivery tag", int(confirm.DeliveryTag)),
					))
					logrus.WithField("queue", routingKey).WithField("delivery tag", confirm.DeliveryTag).Debug(note)
					return nil
				}
			case <-time.After(3 * time.Second):
				if tries == 0 {
					err = errors.New("rabbitMQ: failed to publish a message: delivery confirmation is not received")
					span.RecordError(err)
					logrus.Error(err)
					return err
				}
				tries -= 1
			}
		} else {
			return nil
		}
	}
}

func WithOptionDurable(flag bool) option {
	return option{
		Name:  "durable",
		Value: flag,
	}
}

func WithOptionThreads(number int) option {
	return option{
		Name:  "threads",
		Value: number,
	}
}

func WithOptionAutoDelete(flag bool) option {
	return option{
		Name:  "auto-delete",
		Value: flag,
	}
}

func WithOptionExclusive(flag bool) option {
	return option{
		Name:  "exclusive",
		Value: flag,
	}
}

func WithOptionNoWait(flag bool) option {
	return option{
		Name:  "no-wait",
		Value: flag,
	}
}

func WithOptionAutoAck(flag bool) option {
	return option{
		Name:  "auto-ack",
		Value: flag,
	}
}

func defineConsumerOptions(opts []option) map[string]interface{} {
	var options = map[string]interface{}{
		"durable":     true,
		"auto-delete": false,
		"exclusive":   false,
		"no-wait":     false,
		"auto-ack":    false,
		"threads":     defaultThreadsCount,
	}

	for _, opt := range opts {
		options[opt.Name] = opt.Value
	}

	return options
}

func (ch *RabbitChannel) SetUpConsumer(exchangeName, routingKey string, callback MessageListener, opts ...option) error {
	options := defineConsumerOptions(opts)
	q, err := ch.channel.QueueDeclare(
		routingKey,                    // name
		options["durable"].(bool),     // durable
		options["auto-delete"].(bool), // delete when unused
		options["exclusive"].(bool),   // exclusive
		options["no-wait"].(bool),     // no-wait
		nil,                           // arguments
	)
	if err != nil {
		err = errors.Wrapf(err, "RabbitMQ: failed to declare a queue %s", routingKey)
		logrus.Error(err)
		return err
	}

	err = ch.channel.QueueBind(
		q.Name,       // queue name
		routingKey,   // routing key
		exchangeName, // exchange
		options["no-wait"].(bool),
		nil,
	)
	if err != nil {
		err = errors.Wrapf(err, "RabbitMQ: failed to bind a queue %s", routingKey)
		logrus.Error(err)
		return err
	}

	msgChannel, err := ch.channel.Consume(
		q.Name,                      // queue
		"",                          // consumer
		options["auto-ack"].(bool),  // auto ack
		options["exclusive"].(bool), // exclusive
		false,                       // no local
		options["no-wait"].(bool),   // no wait
		nil,                         // args
	)
	if err != nil {
		err = errors.Wrapf(err, "RabbitMQ: failed to register a consumer %s", q.Name)
		logrus.Error(err)
		return err
	}

	ch.mu.Lock()
	defer ch.mu.Unlock()

	var version int
	if c, ok := ch.consumers[routingKey]; !ok {
		version = 1
	} else {
		version = c.version + 1
	}

	ch.consumers[routingKey] = consumer{
		exchangeName:     exchangeName,
		callback:         callback,
		version:          version,
		availableThreads: make(chan bool, options["threads"].(int)),
	}

	for len(ch.consumers[routingKey].availableThreads) < options["threads"].(int) {
		ch.consumers[routingKey].availableThreads <- true
	}

	logrus.Debugf("consumer %s is created", routingKey)

	go ch.listenQueue(routingKey, version, msgChannel, callback, ch.consumers[routingKey].availableThreads)

	return nil
}

func (ch *RabbitChannel) Close() {
	if ch.IsAlive() {
		logrus.Debug("Shutting down RabbitMQ client...")
		ch.closed = true
		_ = ch.channel.Close()
		err := ch.conn.Close()
		if err != nil {
			logrus.Error(errors.Wrap(err, "RabbitMQ: failed to close the connection"))
		}
		logrus.Info("RabbitMQ: Connection is closed")
	}
}

func (ch *RabbitChannel) IsAlive() bool {
	return !ch.conn.IsClosed()
}

func (ch *RabbitChannel) connect() error {
	var err error
	tries := 0
	for tries < connectionTries {
		tries++
		logrus.Debug("RabbitMQ: try to connect")
		if conn, err := amqp.DialConfig(ch.url, amqp.Config{Heartbeat: ch.heartbeat, Locale: ch.locale}); err != nil {
			if tries == connectionTries {
				return errors.Wrap(err, "failed to connect to RabbitMQ")
			} else {
				time.Sleep(time.Second)
			}
		} else {
			ch.conn = conn
			logrus.Debug("RabbitMQ: Connection is established")
			break
		}
	}

	ch.channel, err = ch.conn.Channel()
	if err != nil {
		return errors.Wrap(err, "RabbitMQ: failed to open a channel")
	}

	if ch.confirmSendsMode {
		err = ch.channel.Confirm(false)
		if err != nil {
			return err
		}

		ch.notifyConfirm = make(chan amqp.Confirmation)
		ch.channel.NotifyPublish(ch.notifyConfirm)
	}
	err = ch.channel.Qos(0, 0, true)
	if err != nil {
		return errors.Wrap(err, "RabbitMQ: failed to set QoS of a channel")
	}

	logrus.Debug("RabbitMQ: Channel is opened")
	go ch.startNotifyCancelOrClosed()

	return nil
}

// listens on the channel's cancelled and closed
func (ch *RabbitChannel) startNotifyCancelOrClosed() {
	notifyCloseConn := make(chan *amqp.Error)
	notifyCloseConn = ch.conn.NotifyClose(notifyCloseConn)
	notifyBlockConn := make(chan amqp.Blocking)
	notifyBlockConn = ch.conn.NotifyBlocked(notifyBlockConn)
	notifyCloseChan := make(chan *amqp.Error)
	notifyCloseChan = ch.channel.NotifyClose(notifyCloseChan)
	notifyCancelChan := make(chan string)
	notifyCancelChan = ch.channel.NotifyCancel(notifyCancelChan)
	select {
	case err := <-notifyCloseConn:
		logrus.Info("attempting to reconnect to amqp server after connection close")
		ch.errorConnection <- err
	case block := <-notifyBlockConn:
		logrus.Errorf("Server hits a memory or disk alarm: %s", block.Reason)
		ch.cancel <- true
	case err := <-notifyCloseChan:
		// If the connection close is triggered by the Server, a reconnection takes place
		if err != nil && err.Server {
			logrus.Info("attempting to reconnect to amqp server after channel close")
			ch.errorConnection <- err
		}
	case err := <-notifyCancelChan:
		logrus.Info("attempting to reconnect to amqp server after cancel")
		ch.errorConnection <- errors.New(err)
	}
}

func (ch *RabbitChannel) reconnect() {
	for {
		select {
		case errorConnection, ok := <-ch.errorConnection:
			if !ch.closed && ok && errorConnection != nil {
				if ch.reconnecting {
					continue
				}
				ch.reconnectMutex.Lock()
				ch.reconnecting = true
				logrus.Error(errors.Wrap(errorConnection, "RabbitMQ: service tries to reconnect"))
				if err := ch.connect(); err != nil {
					logrus.Error(err.Error())
					ch.cancel <- true
					return
				}

				err := ch.recoverConsumers()
				if err != nil {
					ch.cancel <- true
					return
				}
				ch.reconnecting = false
				ch.reconnectMutex.Unlock()
			} else {
				ch.cancel <- true
				return
			}
		}
	}
}

func (ch *RabbitChannel) recoverConsumers() error {
	for routingKey, consumer := range ch.consumers {
		err := ch.SetUpConsumer(consumer.exchangeName, routingKey, consumer.callback)
		if err != nil {
			return err
		}
	}
	return nil
}

func (ch *RabbitChannel) listenQueue(
	routingKey string,
	version int,
	msgChannel <-chan amqp.Delivery,
	callback MessageListener,
	availableThreads chan bool,
) {
	type key string

	logrus.Debugf("listener for queue %s.v%d is in action", routingKey, version)
	defer logrus.Debugf("listener %s.v%d is stopped", routingKey, version)
	ch.waitGroup.Add(1)
	defer ch.waitGroup.Done()

	ctx := context.WithValue(ch.ctx, key(routingKey), nil)

	for {
		select {
		case delivery, ok := <-msgChannel:
			if _, open := <-availableThreads; !open {
				return
			}
			go ch.processDelivery(delivery, ok, routingKey, version, callback, availableThreads)
		case <-ctx.Done():
			logrus.Debugf("listener %s.v%d is going down", routingKey, version)
			if err := ch.channel.Cancel(routingKey, false); err != nil {
				logrus.Errorf("cancel of %s.v%d failed: %q", routingKey, version, err)
			}

			return
		}
	}
}

// Cancel signals that connection to RabbitMQ is broken
func (ch *RabbitChannel) Cancel() <-chan bool {
	return ch.cancel
}

func (ch *RabbitChannel) processDelivery(
	delivery amqp.Delivery,
	ok bool,
	routingKey string,
	version int,
	callback MessageListener,
	availableThreads chan bool,
) {
	defer func() {
		availableThreads <- true
	}()
	if !ok {
		logrus.Debugf("channel for %s.v%d seems to be closed", routingKey, version)
		ch.reconnectMutex.RLock()
		defer ch.reconnectMutex.RUnlock()

		ch.mu.RLock()
		version = ch.consumers[routingKey].version
		ch.mu.RUnlock()

		if d, ok, err := ch.channel.Get(routingKey, false); err != nil {
			logrus.Errorf("queue %s.v%d: %q", routingKey, version, err)
			ch.cancel <- true
		} else if ok {
			if err := d.Nack(false, true); err != nil {
				logrus.Error("looks like we have lost a delivery")
				close(availableThreads)
				ch.cancel <- true
			}
		}

		logrus.Debugf("listener %s.v%d is back on track", routingKey, version)
		return
	}

	// Extract a span context from message to link.
	carrier := otelamqp.NewConsumerMessageCarrier(&delivery)
	parentSpanContext := otel.GetTextMapPropagator().Extract(context.Background(), carrier)

	ctx, span := ch.tracer.Start(parentSpanContext, routingKey, trace.WithSpanKind(trace.SpanKindConsumer))
	span.SetName("AMQP delivery")
	span.SetAttributes(
		attribute.String("exchange", delivery.Exchange),
		attribute.String("queue", routingKey),
		attribute.String("delivery payload", string(delivery.Body)),
	)
	defer span.End()

	logrus.WithField("queue", routingKey).WithField("version", version).Debug("delivery received")
	if err := callback(ctx, delivery); err != nil {
		span.RecordError(err)
		logrus.Error(err)
		_, requeue := err.(ErrRequeue)
		span.AddEvent("negatively acknowledge the delivery", trace.WithAttributes(attribute.String("queue", routingKey)))
		if err := delivery.Nack(false, requeue); err != nil {
			err = errors.Wrap(err, "RabbitMQ: message nacking failed. Consumer is turned off")
			span.RecordError(err)
			logrus.WithField("queue", routingKey).Error(err)
			close(availableThreads)
			ch.cancel <- true
		}
	} else {
		if err := delivery.Ack(false); err != nil {
			err = errors.Wrapf(err, "%s.v%d: acknowledger failed with an error", routingKey, version)
			span.RecordError(err)
			logrus.Error(err)
		}
		span.AddEvent("the delivery acknowledged")
	}
}
