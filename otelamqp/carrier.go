package otelamqp

import (
	amqp "github.com/rabbitmq/amqp091-go"
)

// PublisherMessageCarrier injects and extracts traces from a sarama.ProducerMessage.
type PublisherMessageCarrier struct {
	msg *amqp.Publishing
}

// NewPublisherMessageCarrier creates a new PublisherMessageCarrier.
func NewPublisherMessageCarrier(msg *amqp.Publishing) PublisherMessageCarrier {
	return PublisherMessageCarrier{msg: msg}
}

// Get retrieves a single value for a given key.
func (c PublisherMessageCarrier) Get(key string) string {
	if val, exists := c.msg.Headers[key]; exists {
		return val.(string)
	}
	return ""
}

// Set sets a header.
func (c PublisherMessageCarrier) Set(key, val string) {
	c.msg.Headers[key] = val
}

// Keys returns a slice of all key identifiers in the carrier.
func (c PublisherMessageCarrier) Keys() []string {
	out := make([]string, len(c.msg.Headers))
	for key, _ := range c.msg.Headers {
		out = append(out, key)
	}
	return out
}

// ConsumerMessageCarrier injects and extracts traces from a sarama.ConsumerMessage.
type ConsumerMessageCarrier struct {
	delivery *amqp.Delivery
}

// NewConsumerMessageCarrier creates a new ConsumerMessageCarrier.
func NewConsumerMessageCarrier(delivery *amqp.Delivery) ConsumerMessageCarrier {
	return ConsumerMessageCarrier{delivery: delivery}
}

// Get retrieves a single value for a given key.
func (c ConsumerMessageCarrier) Get(key string) string {
	if val, exists := c.delivery.Headers[key]; exists {
		return val.(string)
	}
	return ""
}

// Set sets a header.
func (c ConsumerMessageCarrier) Set(key, val string) {
	c.delivery.Headers[key] = val
}

// Keys returns a slice of all key identifiers in the carrier.
func (c ConsumerMessageCarrier) Keys() []string {
	out := make([]string, len(c.delivery.Headers))
	for key, _ := range c.delivery.Headers {
		out = append(out, key)
	}
	return out
}
