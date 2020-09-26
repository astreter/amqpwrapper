package amqpwrapper

import (
	"encoding/json"
	"fmt"
	"github.com/rs/zerolog/log"
	"github.com/streadway/amqp"
)

type RabbitChannel struct {
	conn            *amqp.Connection
	channel         *amqp.Channel
	exchangeName    string
	url             string
	errorConnection chan *amqp.Error
	errorChannel    chan *amqp.Error
	closed          bool
	consumers       map[string]messageListener
}

type messageListener func(delivery amqp.Delivery)

func NewRabbitChannel(cfgURL string) (*RabbitChannel, error) {
	ch := new(RabbitChannel)

	ch.url = cfgURL
	ch.consumers = make(map[string]messageListener)

	if err := ch.connect(); err != nil {
		return nil, err
	}
	go ch.reconnect()

	if err := ch.openChannel(); err != nil {
		return nil, err
	}
	go ch.reOpenChannel()

	return ch, nil
}

func (ch *RabbitChannel) DeclareExchange(exchangeName string) error {
	ch.exchangeName = exchangeName
	err := ch.channel.ExchangeDeclare(
		ch.exchangeName, // name
		"topic",         // type
		true,            // durable
		false,           // auto-deleted
		false,           // internal
		false,           // no-wait
		nil,             // arguments
	)
	if err != nil {
		return fmt.Errorf("RabbitMQ: failed to declare an exchange: %s", err.Error())
	}

	log.Info().Msg("RabbitMQ: exchange `" + ch.exchangeName + "` is declared")
	return nil
}

func (ch *RabbitChannel) Publish(message interface{}, routingKey string) error {

	body, err := json.Marshal(message)
	if err != nil {
		log.Error().Msg(fmt.Errorf("RabbitMQ: failed to encode message: %w", err).Error())
		return err
	}

	err = ch.channel.Publish(
		ch.exchangeName, // exchange
		routingKey,      // routing key
		false,           // mandatory
		false,           // immediate
		amqp.Publishing{
			ContentType: "application/json",
			Body:        body,
		})

	if err != nil {
		log.Error().Msg(fmt.Errorf("RabbitMQ: failed to publish a message: %w", err).Error())
		return err
	}
	return nil
}

func (ch *RabbitChannel) SetUpConsumer(routingKey string, callback messageListener) error {
	q, err := ch.channel.QueueDeclare(
		routingKey, // name
		true,       // durable
		true,       // delete when unused
		false,      // exclusive
		false,      // no-wait
		nil,        // arguments
	)
	if err != nil {
		err = fmt.Errorf("RabbitMQ: failed to declare a queue: %w", err)
		log.Error().Msg(err.Error())
		return err
	}

	err = ch.channel.QueueBind(
		q.Name,          // queue name
		routingKey,      // routing key
		ch.exchangeName, // exchange
		false,
		nil,
	)
	if err != nil {
		err = fmt.Errorf("RabbitMQ: failed to bind a queue: %w", err)
		log.Error().Msg(err.Error())
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
		err = fmt.Errorf("RabbitMQ: failed to register a consumer: %w", err)
		log.Error().Msg(err.Error())
		return err
	}

	ch.consumers[routingKey] = callback

	go func() {
		for {
			select {
			case delivery := <-msgChannel:
				callback(delivery)
			}
		}
	}()

	return nil
}

func (ch *RabbitChannel) Close() error {
	ch.closed = true
	ch.channel.Close()
	err := ch.conn.Close()
	if err != nil {
		return fmt.Errorf("RabbitMQ: failed to close the connection: %w", err)
	}
	log.Info().Msg("RabbitMQ: Connection is closed")
	return nil
}

func (ch *RabbitChannel) connect() error {
	conn, err := amqp.Dial(ch.url)
	if err != nil {
		return fmt.Errorf("failed to connect to RabbitMQ: %s", err.Error())
	}
	ch.conn = conn
	ch.errorConnection = make(chan *amqp.Error)
	ch.conn.NotifyClose(ch.errorConnection)
	log.Info().Msg("RabbitMQ: Connection is established")

	return nil
}

func (ch *RabbitChannel) openChannel() error {
	var err error
	ch.channel, err = ch.conn.Channel()
	if err != nil {
		return fmt.Errorf("RabbitMQ: failed to open a channel: %s", err.Error())
	}
	ch.channel.NotifyClose(ch.errorChannel)
	log.Info().Msg("RabbitMQ: Channel is opened")

	err = ch.channel.Qos(3, 0, true)
	if err != nil {
		return fmt.Errorf("RabbitMQ: failed to set QoS of a channel: %s", err.Error())
	}
	return nil
}

func (ch *RabbitChannel) reconnect() {
	for {
		errorConnection := <-ch.errorConnection
		if !ch.closed {
			log.Error().Msg(fmt.Errorf("RabbitMQ: service tries to reconnect: %w", errorConnection).Error())

			if err := ch.connect(); err != nil {
				log.Error().Msg(err.Error())
				panic(err)
			}
			if err := ch.openChannel(); err != nil {
				log.Error().Msg(err.Error())
				panic(err)
			}
			ch.recoverConsumers()
		} else {
			return
		}
	}
}

func (ch *RabbitChannel) reOpenChannel() {
	for {
		errorChannel := <-ch.errorChannel
		if !ch.closed && !ch.conn.IsClosed() {
			log.Error().Msg(fmt.Errorf("RabbitMQ: service tries to reopen channel: %w", errorChannel).Error())

			if err := ch.openChannel(); err != nil {
				log.Error().Msg(err.Error())
				panic(err)
			}
			ch.recoverConsumers()
		} else {
			return
		}
	}
}

func (ch *RabbitChannel) recoverConsumers() {
	for key, callback := range ch.consumers {
		err := ch.SetUpConsumer(key, callback)
		if err != nil {
			log.Error().Msg(err.Error())
		}
	}
}
