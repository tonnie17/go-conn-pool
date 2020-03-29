package pool

import (
	"errors"
	"log"
	"math/rand"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

var (
	network  = "tcp"
	address  = "127.0.0.1:7777"
	maxConns = 50
)

func simpleTCPServer() {
	l, err := net.Listen(network, address)
	if err != nil {
		log.Fatal(err)
	}
	defer l.Close()

	for {
		conn, err := l.Accept()
		if err != nil {
			log.Fatal(err)
		}

		go func() {
			buffer := make([]byte, 256)
			conn.Read(buffer)
		}()
	}
}

func init() {
	go simpleTCPServer()
	time.Sleep(time.Millisecond * 300) // wait until tcp server has been settled
	rand.Seed(time.Now().UTC().UnixNano())
}

func TestPool_Base(t *testing.T) {
	p := NewPool(&Options{
		Addr:        address,
		Protocol:    "tcp",
		PoolSize:    maxConns,
		DialTimeout: 5 * time.Second,
	})
	defer p.Close()

	conn, err := p.Get()
	assert.Nil(t, err)
	assert.NotNil(t, conn)

	assert.Equal(t, 0, p.Len())
	p.Put(conn)
	assert.Equal(t, 1, p.Len())

	badConn := &Conn{}
	badConn.MarkUnusable()
	p.Put(badConn)
	assert.Equal(t, 1, p.Len())

	conn1, err := p.Get()
	conn2, err := p.Get()
	p.Put(conn1)
	p.Put(conn2)
	assert.Equal(t, 2, p.Len())

	wg := new(sync.WaitGroup)
	for i := 0; i < maxConns; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			conn, err := p.Get()
			if err != nil {
				assert.FailNow(t, "Failed")
			}
			time.Sleep(20 * time.Millisecond)
			p.Put(conn)
		}()
	}
	wg.Wait()

	assert.Equal(t, maxConns, p.Len())
	err = p.Put(conn)
	assert.Equal(t, err, errPoolFull)

	p.Close()
	assert.Equal(t, 0, p.Len())

	conn, err = p.Get()
	assert.Empty(t, conn)
	assert.NotEmpty(t, err)
	assert.Equal(t, err, errPoolClosed)
}

func TestPool_Reaper(t *testing.T) {
	p := NewPool(&Options{
		Addr:              address,
		Protocol:          "tcp",
		PoolSize:          maxConns,
		DialTimeout:       5 * time.Second,
		IdleTimeout:       10 * time.Millisecond,
		IdleCheckInterval: 1 * time.Millisecond,
	})
	defer p.Close()

	conn, err := p.Get()
	assert.Nil(t, err)
	err = p.Put(conn)
	assert.Nil(t, err)
	assert.Equal(t, 1, p.Len())
	time.Sleep(20 * time.Millisecond)
	// Should be reaped from pool
	assert.Equal(t, 0, p.Len())
}

func TestPool_MinIdleConns(t *testing.T) {
	p := NewPool(&Options{
		Addr:              address,
		Protocol:          "tcp",
		PoolSize:          maxConns,
		DialTimeout:       5 * time.Second,
		MinIdleConns:      5,
		IdleTimeout:       10 * time.Millisecond,
		IdleCheckInterval: 1 * time.Millisecond,
	})
	defer p.Close()

	assert.Equal(t, 5, p.Len())
	time.Sleep(20 * time.Millisecond)
	// Should be reaped from pool
	assert.Equal(t, 0, p.Len())
}

func TestPool_TestOnBorrow(t *testing.T) {
	p := NewPool(&Options{
		Addr:         address,
		Protocol:     "tcp",
		PoolSize:     maxConns,
		DialTimeout:  5 * time.Second,
		MinIdleConns: 5,
		TestOnBorrow: func(c *Conn, t time.Time) error {
			return errors.New("ping failed")
		},
	})
	defer p.Close()
	// TestOnBorrow failed
	_, err := p.Get()
	assert.NotNil(t, err)
}

func TestPool_Concurrent(t *testing.T) {
	p := NewPool(&Options{
		Addr:        address,
		Protocol:    "tcp",
		PoolSize:    maxConns,
		DialTimeout: 5 * time.Second,
	})
	defer p.Close()

	var (
		wg sync.WaitGroup
		mu sync.Mutex
	)
	for i := 0; i < 1000; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			conn, err := p.Get()
			if err != nil {
				mu.Lock()
				defer mu.Unlock()
				t.FailNow()
			}
			conn.SetWriteDeadline(time.Now().Add(1 * time.Second))
			conn.Write([]byte("hello"))
			time.Sleep(time.Duration(rand.Intn(100)) * time.Millisecond)
			p.Put(conn)
		}()
	}
	wg.Wait()
}

func TestPool_RaceRepeatedConn(t *testing.T) {
	p := NewPool(&Options{
		Addr:        address,
		Protocol:    "tcp",
		PoolSize:    maxConns,
		DialTimeout: 5 * time.Second,
	})
	defer p.Close()

	conn, err := p.Get()
	if err != nil {
		t.FailNow()
	}
	var wg sync.WaitGroup
	for i := 0; i < 100; i++ {
		go func() {
			p.Put(conn)
		}()
	}
	wg.Wait()
}
