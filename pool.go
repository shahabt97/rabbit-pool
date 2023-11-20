package rabbitmq

import (
	"context"
	"fmt"
	"sync"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

type ConnectionPool struct {
	conn *amqp.Connection

	MaxSize  int
	IdleNum  int
	channels map[*amqp.Channel]int
	Mu       sync.Mutex
	AddNew   chan *amqp.Channel
}

func NewPool(conn *amqp.Connection, maxSize int, initChanNumber int) *ConnectionPool {
	pool := &ConnectionPool{
		conn:     conn,
		MaxSize:  maxSize,
		IdleNum:  initChanNumber,
		AddNew:   make(chan *amqp.Channel),
		channels: make(map[*amqp.Channel]int),
	}

	for i := 0; i < pool.IdleNum; i++ {
		pool.Mu.Lock()
		pool.channels[pool.New()] = 1
		pool.Mu.Unlock()
	}

	return pool
}

func (cp *ConnectionPool) Get() *amqp.Channel {
	var popChan *amqp.Channel

	cp.Mu.Lock()
	defer cp.Mu.Unlock()
	
	if cp.IdleNum > 0 {
		for ch := range cp.channels {
			popChan = ch
			delete(cp.channels, ch)
			cp.IdleNum--
			break
		}

		return popChan
	}

	ctx, cancel := context.WithDeadline(context.Background(), time.Now().Add(1000*time.Millisecond))
	defer cancel()

	select {
	case ch := <-cp.AddNew:
		popChan = ch
		return popChan

	case <-ctx.Done():
		popChan = cp.New()
		return popChan
	}

}

func (cp *ConnectionPool) Put(ch *amqp.Channel) {

	switchHandler := make(chan int)

	go func(s chan<- int, newChannel *amqp.Channel) {
		cp.AddNew <- newChannel
		s <- 1
	}(switchHandler, ch)

	go func(s chan<- int) {
		deadline := time.NewTimer(10000 * time.Millisecond)
		<-deadline.C
		s <- 0
	}(switchHandler)

	i := <-switchHandler

	go func(s chan int) {
		<-s
		close(s)

	}(switchHandler)

	if i == 0 {

		cp.Mu.Lock()
		ch2 := <-cp.AddNew
		cp.save([2]*amqp.Channel{ch2, ch})
		cp.Mu.Unlock()

	}

}

func (cp *ConnectionPool) save(channels [2]*amqp.Channel) {

	for _, ch := range channels {
		if cp.IdleNum < cp.MaxSize {
			if _, exists := cp.channels[ch]; !exists {
				if !ch.IsClosed() {
					cp.channels[ch] = 1
					cp.IdleNum++
				}
			}
		} else {
			if !ch.IsClosed() {
				if _, exists := cp.channels[ch]; !exists {
					ch.Close()
				}
			}
		}
	}
}

func (cp *ConnectionPool) New() *amqp.Channel {

	ch, err := cp.conn.Channel()
	if err != nil {
		panic(fmt.Errorf("error in creating new channel in rabbitMQ: %v", err))
	}

	return ch

}