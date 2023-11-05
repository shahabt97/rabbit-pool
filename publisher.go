package rabbitmq

import (
	"context"
	"errors"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

type Publisher struct {
	Pool  *ConnectionPool
	Queue string
}

func NewPublisher(pool *ConnectionPool, queue string) (*Publisher, error) {

	publisher := &Publisher{Pool: pool}

	err := publisher.DeclareQueue(queue)
	if err != nil {
		return nil, err
	}

	return publisher, nil
}

func (p *Publisher) DeclareQueue(q string) error {
	
	if q == "" {
		return errors.New("no queue name has been specified")
	}
	p.Queue = q

	ch := p.Pool.Get()
	_, err := ch.QueueDeclare(
		p.Queue,
		true,  // durable
		false, // delete when unused
		false, // exclusive
		false, // no-wait
		nil,   // arguments
	)
	if !ch.IsClosed() {
		go p.Pool.Put(ch)
	}

	if err != nil {
		return err
	}

	return nil

}

func (p *Publisher) NewPublish(body []byte, contentType string) error {

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	ch := p.Pool.Get()
	err := ch.PublishWithContext(ctx, "", p.Queue, false, false, amqp.Publishing{
		ContentType:  contentType,
		Body:         body,
		DeliveryMode: amqp.Persistent,
	})

	if !ch.IsClosed() {
		go p.Pool.Put(ch)
	}

	if err != nil {

		return err
	}
	return nil

}
