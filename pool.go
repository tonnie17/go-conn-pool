package pool

import (
	"errors"
	"fmt"
	"net"
	"os"
	"sync"
	"time"
)

var (
	defaultMaxConns = 100

	errBadConn    = errors.New("connection is broken")
	errPoolClosed = errors.New("connection pool is closed")
	errPoolFull   = errors.New("connection pool is full")
)

// Options contains options of connection pool.
type Options struct {
	Protocol    string
	Addr        string
	DialTimeout time.Duration
	PoolSize    int // Maximum number of idle connections in the pool.

	// Minimum number of idle connection in the pool.
	// When the `NewPool` function is being to call, MinIdleConns size of connections
	// will be initilized in the pool. Each time the `Get` function was called,
	// the pool will check this value to ensure there are enough idle connections
	// in the pool. Note that if `IdleTimeout` and `IdleCheckInterval` were configured,
	// it might be less than MinIdleConns number of idle connections in the pool.
	MinIdleConns      int
	IdleTimeout       time.Duration
	IdleCheckInterval time.Duration
	TestOnBorrow      func(c *Conn, lastUsedAt time.Time) error

	// Marks the connection to unusable if any errors occurs,
	// the broken connection is unable return to the pool.
	DropIfError bool

	// Application supplied helper to displays some useful information of the pool.
	Logger interface {
		Info(string)
		Error(string)
	}
}

type nilLogger struct{}

func (l *nilLogger) Info(string)  {}
func (l *nilLogger) Error(string) {}

type stdlLogger struct{}

func (l *stdlLogger) Info(msg string)  { fmt.Fprintln(os.Stdout, msg) }
func (l *stdlLogger) Error(msg string) { fmt.Fprintln(os.Stderr, msg) }

var (
	// StdlLogger displays pool's logs to standard output
	StdlLogger = new(stdlLogger)
)

// Pool maintains a pool of connections.
// The following example shows how to use a pool:
//
// func example() {
// 		serverPool = NewPool(&PoolConfig{
// 			Protocol: "tcp",
// 			Addr:     SERVER_ADDR_ID_TEST,
// 			PoolSize: 100,
// 			Timeout:  5,
// 			IdleCheckInterval: 30 * time.Second,
// 			IdleTimeout: 1 * time.Minute
// 		})
// 		conn, err := serverPool.Get()
// 		defer serverPool.Put(conn)
// 		// Do Some operations on conn here...
// }
type Pool struct {
	conns   chan *Conn
	connsMu sync.Mutex
	opts    *Options
	closed  bool
}

// A Conn wraps a *Conn and its owning pool
type Conn struct {
	net.Conn
	t           time.Time // last time put back to the pool
	mu          sync.RWMutex
	unusable    bool
	dropIfError bool
}

func (c *Conn) GetT() time.Time {
	return c.t
}

func (c *Conn) Read(b []byte) (n int, err error) {
	n, err = c.Conn.Read(b)
	if c.dropIfError && err != nil {
		c.MarkUnusable()
	}
	return n, err
}

func (c *Conn) Write(b []byte) (n int, err error) {
	n, err = c.Conn.Write(b)
	if c.dropIfError && err != nil {
		c.MarkUnusable()
	}
	return n, err
}

// Close closes the underlying connection when it was unavailable.
func (c *Conn) Close() error {
	c.mu.RLock()
	defer c.mu.RUnlock()

	if c.unusable {
		err := c.Conn.Close()
		if err != nil {
			return err
		}
	}
	return nil
}

// MarkUnusable marks the connection is no longer to reuse.
func (c *Conn) MarkUnusable() {
	c.mu.RLock()
	c.unusable = true
	c.mu.RUnlock()
}

// NewPool returns a new client pool.
func NewPool(opts *Options) *Pool {
	maxConns := opts.PoolSize
	if maxConns <= 0 {
		maxConns = defaultMaxConns
	}
	p := &Pool{
		conns: make(chan *Conn, maxConns),
		opts:  opts,
	}

	if opts.Logger == nil {
		opts.Logger = new(nilLogger)
	}

	for i := 0; i < opts.MinIdleConns; i++ {
		conn, err := p.dial()
		if err != nil {
			opts.Logger.Error("Connection dial error: " + err.Error())
			continue
		}
		p.Put(conn)
	}
	if p.opts.IdleCheckInterval > 0 && p.opts.IdleTimeout > 0 {
		go p.reapStalesConn()
	}
	return p
}

func (p *Pool) isStaleConn(conn *Conn) bool {
	return p.opts.IdleTimeout > 0 && conn.t.Add(p.opts.IdleTimeout).Before(time.Now())
}

// reapStalesConn clears all stale connections.
func (p *Pool) reapStalesConn() {
	freshConns := make([]*Conn, p.opts.PoolSize)
	t := time.NewTicker(p.opts.IdleCheckInterval)
	for range t.C {
		p.connsMu.Lock()
		if p.closed {
			p.connsMu.Unlock()
			break
		}
		index := 0
	Loop:
		for {
			select {
			case conn := <-p.conns:
				if p.isStaleConn(conn) {
					conn.Conn.Close()
				} else {
					freshConns[index] = conn
					index++
				}
			default:
				break Loop
			}
		}
		for i := 0; i < index; i++ {
			p.conns <- freshConns[i]
		}
		p.connsMu.Unlock()
	}
}

func (p *Pool) dial() (*Conn, error) {
	p.connsMu.Lock()
	if p.closed {
		p.connsMu.Unlock()
		return nil, errPoolClosed
	}
	p.connsMu.Unlock()
	conn, err := net.DialTimeout(p.opts.Protocol, p.opts.Addr, p.opts.DialTimeout)
	if err != nil {
		p.opts.Logger.Error("Connection dial timeout: " + err.Error())
		return nil, err
	}
	pc := &Conn{Conn: conn, dropIfError: p.opts.DropIfError}
	pc.t = time.Now()
	return pc, err
}

// Put puts the connection back to the pool
func (p *Pool) Put(conn *Conn) error {
	if conn == nil {
		return nil
	}

	conn.mu.Lock()
	if conn.unusable {
		conn.mu.Unlock()
		return errBadConn
	}
	conn.t = time.Now()
	conn.mu.Unlock()

	p.connsMu.Lock()
	defer p.connsMu.Unlock()

	if p.closed {
		return errPoolClosed
	}

	select {
	case p.conns <- conn:
		return nil
	default:
		return errPoolFull
	}
}

// Get takes a connection from pool.
func (p *Pool) Get() (*Conn, error) {
	for {
		select {
		case conn := <-p.conns:
			// channel was closed
			if conn == nil {
				return nil, errPoolClosed
			}
			if p.isStaleConn(conn) {
				conn.Conn.Close()
				continue
			}
			// Keep MinIdleConns size of connections on the pool
			if p.opts.MinIdleConns > 0 && p.opts.MinIdleConns > p.Len() {
				idleConn, err := p.dial()
				if err != nil {
					p.opts.Logger.Error("Connection dial error: " + err.Error())
				}
				p.Put(idleConn)
			}
			if p.opts.TestOnBorrow != nil {
				if err := p.opts.TestOnBorrow(conn, conn.t); err != nil {
					p.opts.Logger.Error("TestOnBorrow error: " + err.Error())
					return nil, err
				}
			}
			return conn, nil
		// When the pool is unable to provide a reuseable connection,
		// a new connections will be create instead of waiting one,
		// the pool will keeps max size of idle connections and drops
		// those redundant handles.
		default:
			return p.dial()
		}
	}
}

// Len returns the number of idle connections of the pool
func (p *Pool) Len() int {
	p.connsMu.Lock()
	size := len(p.conns)
	p.connsMu.Unlock()
	return size
}

// Close closes all connections of pool
func (p *Pool) Close() error {
	if p.closed {
		return errPoolClosed
	}
	var retErr error
	p.connsMu.Lock()
	conns := p.conns
	p.conns = nil
	p.closed = true
	p.connsMu.Unlock()

	close(conns)
	for conn := range conns {
		err := conn.Conn.Close()
		if err != nil && retErr == nil {
			p.opts.Logger.Error("Close connection error: " + err.Error())
			retErr = err
		}
	}
	return retErr
}
