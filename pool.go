package rabbitmq

import (
	"context"
	"fmt"
	"math/rand"
	"sync"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/samber/lo"
)

type ConnectionPool struct {
	url             string
	conn            []*amqp.Connection
	connectionMutex sync.Mutex
	connCount       int

	MaxSize  int
	IdleNum  int
	channels map[*amqp.Channel]int
	Mu       sync.Mutex
	AddNew   chan *amqp.Channel
}

func NewPool(url string, maxConn int, maxSize int, initChanNumber int) *ConnectionPool {
	pool := &ConnectionPool{
		url:       url,
		MaxSize:   maxSize,
		connCount: maxConn,
		IdleNum:   initChanNumber,
		AddNew:    make(chan *amqp.Channel),
		channels:  make(map[*amqp.Channel]int),
	}

	for i := 0; i < pool.connCount; i++ {

		c, err := amqp.Dial(url)
		if err != nil {
			panic(err)
		}

		pool.conn = append(pool.conn, c)

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
	if cp.IdleNum > 0 {
		for ch := range cp.channels {
			popChan = ch
			delete(cp.channels, ch)
			cp.IdleNum--

			break
		}

		cp.Mu.Unlock()
		return popChan
	}

	ctx, cancel := context.WithDeadline(context.Background(), time.Now().Add(1000*time.Millisecond))
	defer cancel()

	select {
	case ch := <-cp.AddNew:
		popChan = ch
		cp.Mu.Unlock()
		return popChan

	case <-ctx.Done():
		popChan = cp.New()
		cp.Mu.Unlock()
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

	var interval = 5
	for {

		ch, err := cp.getConn().Channel()
		if err != nil {
			time.Sleep(time.Duration(interval) * time.Second)
			interval = interval * 2
			if interval > 600 {
				panic(fmt.Errorf("error in creating new channel in rabbitMQ: %v", err))
			}
			continue
		}

		return ch

	}

}

func (cp *ConnectionPool) getConn() *amqp.Connection {

	var interval = 5
	for {

		cp.connectionMutex.Lock()

		if len(cp.conn) == 0 {
			cp.connectionMutex.Unlock()
			time.Sleep(time.Duration(interval) * time.Second)
			interval *= 2
			continue
		}

		index := rand.Intn(len(cp.conn))
		conn := cp.conn[index]
		if conn.IsClosed() {
			go cp.createNewConn(conn)
			cp.conn = lo.Filter[*amqp.Connection](cp.conn, func(item *amqp.Connection, index int) bool {
				return item != conn
			})
			cp.connectionMutex.Unlock()
			continue
		}

		cp.connectionMutex.Unlock()
		return conn

	}

}

func (cp *ConnectionPool) createNewConn(conn *amqp.Connection) {

	var interval = 5
	for {
		newConn, err := amqp.Dial(cp.url)
		if err != nil {
			time.Sleep(time.Duration(interval) * time.Second)
			interval *= 2
			if interval > 40 {
				panic(err)
			}
			continue
		}
		cp.connectionMutex.Lock()
		cp.conn = append(cp.conn, newConn)
		cp.connectionMutex.Unlock()
		return
	}

}
