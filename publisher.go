package rabbitmq

import (
	"context"
	"errors"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

type Publisher struct {
	Pool         *ConnectionPool
	ExchangeName string
	ExchangeKey  string
}

func NewPublisher(pool *ConnectionPool, exchange, exType string, exchangeDurable bool) (*Publisher, error) {

	publisher := &Publisher{Pool: pool}

	err := publisher.DeclareExchange(exchange, exType, exchangeDurable)
	if err != nil {
		return nil, err
	}

	return publisher, nil
}

func (p *Publisher) DeclareExchange(name, exType string, durable bool) (err error) {

	if name == "" {
		return errors.New("no exchange name has been specified")
	}
	p.ExchangeName = name

	ch := p.Pool.Get()
	err = ch.ExchangeDeclare(
		p.ExchangeName,
		exType,  // durable
		durable, // delete when unused
		false,   // exclusive
		false,   // no-wait
		false,
		nil, // arguments
	)
	if !ch.IsClosed() {
		go p.Pool.Put(ch)
	}

	if err != nil {
		return err
	}

	return nil

}

func (p *Publisher) NewPublish(body []byte, contentType string, durable bool, timeout time.Duration) error {

	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	var deliveryMode uint8
	if durable {
		deliveryMode = amqp.Persistent
	} else {
		deliveryMode = amqp.Transient
	}

	ch := p.Pool.Get()

	err := ch.PublishWithContext(ctx, p.ExchangeName, p.ExchangeKey, false, false, amqp.Publishing{
		ContentType:  contentType,
		Body:         body,
		DeliveryMode: deliveryMode,
	})

	if !ch.IsClosed() {
		go p.Pool.Put(ch)
	}

	if err != nil {
		return err
	}
	return nil

}
