package rabbitmq

import (
	"errors"

	amqp "github.com/rabbitmq/amqp091-go"
)

type Consumer struct {
	Pool  *ConnectionPool
	Queue string
}

func NewConsumer(pool *ConnectionPool, queue string) (<-chan amqp.Delivery, error) {
	consumer := &Consumer{Pool: pool}
	err := consumer.declareQueue(queue)
	if err != nil {
		return nil, err
	}

	return consumer.newDeliveryChannel()
}

func (c *Consumer) declareQueue(q string) error {
	if q == "" {
		return errors.New("no queue name has been specified for consumer")
	}
	c.Queue = q
	return nil
}

func (c *Consumer) newDeliveryChannel() (<-chan amqp.Delivery, error) {
	ch := c.Pool.Get()
	deliveryChannel, err := ch.Consume(c.Queue, "", false, false, false, false, nil)

	if err != nil {
		return nil, err
	}

	return deliveryChannel, nil

}
