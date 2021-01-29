package amqpwrapper

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/astreter/amqpwrapper/helper"
	"github.com/sirupsen/logrus"
	"github.com/streadway/amqp"
	"sync"
	"time"
)

const ConnectionTries = 3

type RabbitChannel struct {
	ctx             context.Context
	ctxCancel       context.CancelFunc
	waitGroup       *sync.WaitGroup
	conn            *amqp.Connection
	channel         *amqp.Channel
	url             string
	errorConnection chan *amqp.Error
	notifyConfirm   chan amqp.Confirmation
	closed          bool
	consumers       map[string]consumer
	reconnecting    bool
}

type messageListener func(delivery amqp.Delivery) error

type consumer struct {
	exchangeName string
	callback     messageListener
	version      int
}

type Config struct {
	URL      string
	Host     string
	Port     string
	User     string
	Password string
	Vhost    string
	Debug    bool
}

func NewRabbitChannel(ctx context.Context, ctxCancel context.CancelFunc, wg *sync.WaitGroup, cfg *Config) (*RabbitChannel, error) {
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

	ch.url = url

	err := ch.connect()
	if err != nil {
		return nil, err
	}
	go ch.reconnect()

	ch.ctx = ctx
	ch.ctxCancel = ctxCancel
	ch.waitGroup = wg
	ch.consumers = make(map[string]consumer)
	ch.reconnecting = false

	return ch, nil
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
		return fmt.Errorf("RabbitMQ: failed to declare an exchange: %s", err.Error())
	}

	logrus.Debug("RabbitMQ: exchange `" + exchangeName + "` is declared")
	return nil
}

func (ch *RabbitChannel) Publish(message interface{}, exchangeName, routingKey string, safeMode bool) error {
	ch.waitGroup.Add(1)
	defer ch.waitGroup.Done()
	body, err := json.Marshal(message)
	if err != nil {
		logrus.Error(fmt.Errorf("RabbitMQ: failed to encode message: %w", err).Error())
		return err
	}

	if ch.closed {
		return errors.New("rabbitMQ: failed to publish a message: connection is lost")
	}

	for {
		err = ch.channel.Publish(
			exchangeName, // exchange
			routingKey,   // routing key
			false,        // mandatory
			false,        // immediate
			amqp.Publishing{
				ContentType: "application/json",
				Body:        body,
			})

		if err != nil {
			if err == amqp.ErrClosed {
				continue
			}
			logrus.Error(fmt.Errorf("RabbitMQ: failed to publish a message: %w", err).Error())
			return err
		}
		if safeMode {
			select {
			case confirm := <-ch.notifyConfirm:
				if confirm.Ack {
					return nil
				}
			case <-time.After(3 * time.Second):
				return errors.New("rabbitMQ: failed to publish a message: delivery confirmation is not received")
			}
		}
		return nil
	}
}

func (ch *RabbitChannel) SetUpConsumer(exchangeName, routingKey string, callback messageListener) error {
	q, err := ch.channel.QueueDeclare(
		routingKey, // name
		true,       // durable
		false,      // delete when unused
		false,      // exclusive
		false,      // no-wait
		nil,        // arguments
	)
	if err != nil {
		err = fmt.Errorf("RabbitMQ: failed to declare a queue %s: %w", routingKey, err)
		logrus.Error(err.Error())
		return err
	}

	err = ch.channel.QueueBind(
		q.Name,       // queue name
		routingKey,   // routing key
		exchangeName, // exchange
		false,
		nil,
	)
	if err != nil {
		err = fmt.Errorf("RabbitMQ: failed to bind a queue %s: %w", routingKey, err)
		logrus.Error(err.Error())
		return err
	}

	msgChannel, err := ch.channel.Consume(
		q.Name, // queue
		"",     // consumer
		false,  // auto ack
		false,  // exclusive
		false,  // no local
		false,  // no wait
		nil,    // args
	)
	if err != nil {
		err = fmt.Errorf("RabbitMQ: failed to register a consumer %s: %w", q.Name, err)
		logrus.Error(err.Error())
		return err
	}
	var version int
	if c, ok := ch.consumers[routingKey]; !ok {
		version = 1
	} else {
		version = c.version + 1
	}
	ch.consumers[routingKey] = consumer{
		exchangeName: exchangeName,
		callback:     callback,
		version:      version,
	}
	logrus.Debugf("consumer %s is created", routingKey)

	go ch.listenQueue(routingKey, version, msgChannel, callback)

	return nil
}

func (ch *RabbitChannel) Close() error {
	ch.closed = true
	_ = ch.channel.Close()
	err := ch.conn.Close()
	if err != nil {
		return fmt.Errorf("RabbitMQ: failed to close the connection: %w", err)
	}
	logrus.Debug("RabbitMQ: Connection is closed")
	return nil
}

func (ch *RabbitChannel) IsAlive() bool {
	return !ch.conn.IsClosed()
}

func (ch *RabbitChannel) connect() error {
	var err error
	tries := 0
	for tries < ConnectionTries {
		tries++
		logrus.Debug("RabbitMQ: try to connect")
		if conn, err := amqp.Dial(ch.url); err != nil {
			if tries == ConnectionTries {
				return fmt.Errorf("failed to connect to RabbitMQ: %s", err.Error())
			} else {
				time.Sleep(time.Second)
			}
		} else {
			ch.conn = conn
			ch.errorConnection = make(chan *amqp.Error)
			ch.conn.NotifyClose(ch.errorConnection)
			logrus.Debug("RabbitMQ: Connection is established")
			break
		}
	}

	ch.channel, err = ch.conn.Channel()
	if err != nil {
		return fmt.Errorf("RabbitMQ: failed to open a channel: %s", err.Error())
	}

	err = ch.channel.Confirm(false)
	if err != nil {
		return fmt.Errorf("RabbitMQ: %s", err.Error())
	}

	ch.notifyConfirm = make(chan amqp.Confirmation)
	ch.channel.NotifyPublish(ch.notifyConfirm)

	err = ch.channel.Qos(3, 0, true)
	if err != nil {
		return fmt.Errorf("RabbitMQ: failed to set QoS of a channel: %s", err.Error())
	}

	logrus.Debug("RabbitMQ: Channel is opened")
	return nil
}

func (ch *RabbitChannel) reconnect() {
	for {
		errorConnection, ok := <-ch.errorConnection
		if !ch.closed && ok {
			ch.reconnecting = true
			logrus.Error(fmt.Errorf("RabbitMQ: service tries to reconnect: %w", errorConnection).Error())
			if err := ch.connect(); err != nil {
				logrus.Error(err.Error())
				panic(err)
			}

			err := ch.recoverConsumers()
			if err != nil {
				panic(err)
			}
			ch.reconnecting = false
		} else {
			return
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

func (ch *RabbitChannel) listenQueue(routingKey string, version int, msgChannel <-chan amqp.Delivery, callback messageListener) {
	type key string

	logrus.Debugf("listener for queue %s.v%d is in action", routingKey, version)
	defer logrus.Debugf("listener %s.v%d is stopped", routingKey, version)
	ch.waitGroup.Add(1)
	defer ch.waitGroup.Done()

	ctx := context.WithValue(ch.ctx, key(routingKey), nil)

	for {
		select {
		case delivery, ok := <-msgChannel:
			if !ok {
				logrus.Debugf("channel for %s.v%d seems to be closed", routingKey, version)
				time.Sleep(time.Second)
				if err := helper.Retry(3, time.Second, func() error {
					if ch.reconnecting {
						return errors.New("reconnecting is in progress")
					}
					version = ch.consumers[routingKey].version
					return nil
				}); err != nil {
					ch.ctxCancel()
					return
				}

				if d, ok, err := ch.channel.Get(routingKey, false); err != nil {
					logrus.Errorf("queue %s.v%d: %q", routingKey, version, err)
					ch.ctxCancel()
				} else if ok {
					if err := d.Nack(false, true); err != nil {
						logrus.Error("looks like we have lost a delivery")
						ch.ctxCancel()
					}
				} else {
					logrus.Debugf("listener %s.v%d is back on track", routingKey, version)
				}

				return
			}

			logrus.Debug(fmt.Sprintf("comsumer %s.v%d: delivery recieved", routingKey, version))
			if err := callback(delivery); err != nil {
				if err := delivery.Nack(false, false); err != nil {
					logrus.Error(fmt.Errorf("RabbitMQ: %s: message nacking failed: %w. Consumer is turned off", routingKey, err))
					ch.ctxCancel()
				}
			} else {
				if err := delivery.Ack(false); err != nil {
					logrus.Error(fmt.Errorf("%s.v%d: acknowledger failed with an error: %w", routingKey, version, err))
				}
			}
		case <-ctx.Done():
			logrus.Debugf("listener %s.v%d is going down", routingKey, version)
			if err := ch.channel.Cancel(routingKey, false); err != nil {
				logrus.Errorf("cancel of %s.v%d failed: %q", routingKey, version, err)
			}

			return
		}
	}
}
