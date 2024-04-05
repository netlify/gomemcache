package memcache

import (
	"bufio"
	"context"
	"errors"
	"net"
	"sync"
	"sync/atomic"
	"time"
)

const (
	DefaultMaxConns        = 10
	DefaultIdleLifetime    = 5 * time.Minute
	DefaultIdleClosePeriod = 1 * time.Minute
)

type PoolConfig struct {
	MaxConns        int           // maximum number of connections to keep open per server (includes active and idle connections)
	MaxIdleLifetime time.Duration // maximum amount of time an idle connection is kept open
	IdleClosePeriod time.Duration // how often to close (reap) idle connections
}

// withDefaults sets default values for any zero fields in the config.
func (c *PoolConfig) withDefaults() {
	if c.MaxConns == 0 {
		c.MaxConns = DefaultMaxConns
	}
	if c.MaxIdleLifetime == 0 {
		c.MaxIdleLifetime = DefaultIdleLifetime
	}
	if c.IdleClosePeriod == 0 {
		c.IdleClosePeriod = DefaultIdleClosePeriod
	}
}

type dialer func(ctx context.Context, network, addr string) (net.Conn, error)

// ClusterPool is a thin wrapper around multiple Pools, one for each server in the cluster.
type ClusterPool struct {
	dial   dialer
	config PoolConfig
	mu     sync.RWMutex
	pools  map[string]*Pool
}

func NewClusterPool(dial dialer, config PoolConfig) *ClusterPool {
	return &ClusterPool{
		dial:   dial,
		config: config,
		pools:  make(map[string]*Pool),
	}
}

func (cp *ClusterPool) Get(ctx context.Context, addr net.Addr) (*Conn, error) {
	pool := cp.getPool(addr)
	return pool.Get(ctx)
}

func (cp *ClusterPool) Put(c *Conn) {
	pool := cp.getPool(c.addr)
	pool.Put(c)
}

func (cp *ClusterPool) Release(addr net.Addr) {
	pool := cp.getPool(addr)
	pool.Release()
}

func (cp *ClusterPool) Each(fn func(string, *Pool)) {
	cp.mu.RLock()
	defer cp.mu.RUnlock()

	for addr, pool := range cp.pools {
		fn(addr, pool)
	}
}

// getPool returns the pool for the given address, creating it if necessary.
// This is required because the cluster is dynamic and new servers can be added at any time.
func (cp *ClusterPool) getPool(addr net.Addr) *Pool {
	cp.mu.RLock()
	pool, ok := cp.pools[addr.String()]
	cp.mu.RUnlock()

	if ok {
		return pool
	}

	cp.mu.Lock()
	defer cp.mu.Unlock()

	// double check now that we hold the exclusive lock
	pool, ok = cp.pools[addr.String()]
	if ok {
		return pool
	}

	pool = NewPool(addr, cp.dial, cp.config)
	cp.pools[addr.String()] = pool
	return pool
}

// Pool is a connection pool for a single server.
// It manages a pool of idle connections and limits the number of active connections.
// It also periodically closes idle connections to minimize unused resources.
// Note: Any connection obtained from the pool must be returned to the pool when done (either using the Put or Release method).
type Pool struct {
	addr   net.Addr
	dialer func(ctx context.Context, network, addr string) (net.Conn, error)
	config PoolConfig

	mu     sync.Mutex
	idle   []idleConn
	active chan struct{}
	hits   atomic.Int64
	misses atomic.Int64
}

type idleConn struct {
	c *Conn
	t time.Time // last used time
}

func NewPool(addr net.Addr, dial dialer, config PoolConfig) *Pool {
	// ensure config has defaults
	config.withDefaults()

	p := &Pool{
		addr:   addr,
		dialer: dial,
		config: config,
	}

	// start a goroutine to close idle connections periodically
	go func() {
		ticker := time.NewTicker(p.config.IdleClosePeriod)
		defer ticker.Stop()
		for range ticker.C {
			p.closeIdle()
		}
	}()

	p.active = make(chan struct{}, p.config.MaxConns)
	return p
}

// Get returns a connection from the pool or creates a new one if necessary.
// The connection must be returned to the pool when done.
// If max connections are already in use, Get will block until a connection is available or the context is canceled.
func (p *Pool) Get(ctx context.Context) (*Conn, error) {
	// first acquire a slot
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case p.active <- struct{}{}:
	}

	// then try to get an idle connection
	if c := p.getIdle(); c != nil {
		p.hits.Add(1)
		return c, nil
	}
	p.misses.Add(1)

	// if no idle connection, create a new one
	nc, err := p.dialer(ctx, p.addr.Network(), p.addr.String())
	if err != nil {
		// release the slot
		<-p.active

		var ne net.Error
		if errors.As(err, &ne) && ne.Timeout() {
			return nil, &ConnectTimeoutError{p.addr}
		}
		return nil, err
	}

	return &Conn{
		nc:   nc,
		addr: p.addr,
		rw:   bufio.NewReadWriter(bufio.NewReader(nc), bufio.NewWriter(nc)),
		pool: p,
	}, nil
}

func (p *Pool) getIdle() *Conn {
	p.mu.Lock()
	defer p.mu.Unlock()

	if len(p.idle) == 0 {
		return nil
	}

	// idle is a stack (LIFO)
	ic := p.idle[len(p.idle)-1]
	p.idle = p.idle[:len(p.idle)-1]
	return ic.c
}

// Put returns a connection to the pool for reuse.
func (p *Pool) Put(c *Conn) {
	p.mu.Lock()
	defer p.mu.Unlock()

	// idle is a stack (LIFO)
	p.idle = append(p.idle, idleConn{c: c, t: time.Now()})
	<-p.active
}

// Release releases a slot in the pool. This should be called if a connection was closed outside the pool
// (most likely due to an error).
func (p *Pool) Release() {
	<-p.active
}

// closeIdle closes idle connections that have been idle for longer than the max idle lifetime.
func (p *Pool) closeIdle() {
	p.mu.Lock()
	defer p.mu.Unlock()

	idle := make([]idleConn, 0, len(p.idle))
	for _, ic := range p.idle {
		if time.Since(ic.t) > p.config.MaxIdleLifetime {
			ic.c.nc.Close()
		} else {
			idle = append(idle, ic)
		}
	}
	p.idle = idle
}

func (p *Pool) Stats() PoolStats {
	p.mu.Lock()
	defer p.mu.Unlock()

	return PoolStats{
		Active: int64(len(p.active)),
		Idle:   int64(len(p.idle)),
		Hit:    p.hits.Swap(0),
		Miss:   p.misses.Swap(0),
	}
}

type PoolStats struct {
	Active int64 // number of active connections
	Idle   int64 // number of idle connections
	Hit    int64 // number of cache hits (since last call to Stats)
	Miss   int64 // number of cache misses (since last call to Stats)
}

type Conn struct {
	pool *Pool
	addr net.Addr
	nc   net.Conn
	rw   *bufio.ReadWriter
}

func (c *Conn) ExtendDeadline(d time.Duration) error {
	return c.nc.SetDeadline(time.Now().Add(d))
}

func (c *Conn) Release(err error) {
	if err == nil || resumableError(err) {
		c.pool.Put(c)
	} else {
		c.pool.Release()
		c.nc.Close()
	}
}

// resumableError returns true if err is only a protocol-level cache error.
// This is used to determine whether or not a server connection should
// be re-used or not. If an error occurs, by default we don't reuse the
// connection, unless it was just a cache error.
func resumableError(err error) bool {
	switch err {
	case ErrCacheMiss, ErrCASConflict, ErrNotStored, ErrMalformedKey:
		return true
	}
	return false
}

// ConnectTimeoutError is the error type used when it takes
// too long to connect to the desired host. This level of
// detail can generally be ignored.
type ConnectTimeoutError struct {
	Addr net.Addr
}

func (cte *ConnectTimeoutError) Error() string {
	return "memcache: connect timeout to " + cte.Addr.String()
}
