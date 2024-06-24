package rabbitmq

import (
	"context"
	"errors"
	amqp "github.com/rabbitmq/amqp091-go"
)

type Publisher struct {
	Pool         *ConnectionPool
	exchangeName string
	kind         string
	exchangeKey  string
}

func NewPublisher(pool *ConnectionPool, exchangeName, kind, exchangeKey string, durable bool) (*Publisher, error) {

	publisher := &Publisher{Pool: pool, exchangeName: exchangeName, kind: kind, exchangeKey: exchangeKey}

	err := publisher.DeclareExchange(durable)
	if err != nil {
		return nil, err
	}

	return publisher, nil
}

func (p *Publisher) DeclareExchange(durable bool) (err error) {

	if p.exchangeName == "" {
		return errors.New("no exchange name has been specified")
	}

	ch := p.Pool.Get()
	err = ch.ExchangeDeclare(
		p.exchangeName,
		p.kind,  // kind
		durable, // durable
		false,
		false,
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

func (p *Publisher) NewPublish(ctx context.Context, body []byte, contentType string, durable bool) error {

	var deliveryMode uint8
	if durable {
		deliveryMode = amqp.Persistent
	} else {
		deliveryMode = amqp.Transient
	}

	ch := p.Pool.Get()

	err := ch.PublishWithContext(ctx, p.exchangeName, p.exchangeKey, false, false, amqp.Publishing{
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
