package rabbitmq

import (
	amqp "github.com/rabbitmq/amqp091-go"
)

type Consumer struct {
	pool         *ConnectionPool
	queueName    string
	durable      bool
	exchangeName string
	routKey      string
}

func NewConsumer(pool *ConnectionPool, queueName, exchangeName, routKey string, durable bool, args amqp.Table) (<-chan amqp.Delivery, error) {
	consumer := &Consumer{pool: pool, queueName: queueName, durable: durable, exchangeName: exchangeName, routKey: routKey}
	return consumer.declareQueueAndGetNewDeliveryChannel(args)
}

func (c *Consumer) declareQueueAndGetNewDeliveryChannel(args amqp.Table) (<-chan amqp.Delivery, error) {
	ch := c.pool.Get()
	queue, err := ch.QueueDeclare(c.queueName, c.durable, false, false, false, nil)
	if err != nil {
		return nil, err
	}
	err = ch.QueueBind(queue.Name, c.routKey, c.exchangeName, false, nil)
	if err != nil {
		return nil, err
	}
	deliveryChannel, err := ch.Consume(queue.Name, "", false, false, false, false, args)

	if err != nil {
		return nil, err
	}

	return deliveryChannel, nil
}
