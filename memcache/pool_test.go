package memcache

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"net"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ory/dockertest/v3"
	"github.com/stretchr/testify/require"
)

func TestPool(t *testing.T) {
	addr := setupMemcached(t)

	t.Run("max conns", func(t *testing.T) {
		p, created := tpool(addr, PoolConfig{MaxConns: 2})

		// first two connections will be created
		c1, err := p.Get(tctx(t, 100*time.Millisecond))
		require.NoError(t, err)
		c2, err := p.Get(tctx(t, 100*time.Millisecond))
		require.NoError(t, err)
		require.Equal(t, int64(2), created.Load())

		// subsequent connections will block until one of the first two is returned
		_, err = p.Get(tctx(t, 100*time.Millisecond))
		require.ErrorIs(t, err, context.DeadlineExceeded)

		// release one of the first two connections
		c1.Release(nil)

		// now the third connection will be available (but it was pooled so it should match the first one)
		c3, err := p.Get(tctx(t, 100*time.Millisecond))
		require.NoError(t, err)
		require.Equal(t, c1, c3)
		require.Equal(t, int64(2), created.Load())

		// cleanup
		c2.Release(errors.New("dont reuse"))
		c3.Release(errors.New("dont reuse"))
	})

	t.Run("idle close", func(t *testing.T) {
		p, created := tpool(addr, PoolConfig{
			MaxConns:        1,
			MaxIdleLifetime: 50 * time.Millisecond,
			IdleClosePeriod: 50 * time.Millisecond,
		})

		// create two connections and return to the pool
		c1, err := p.Get(tctx(t, 100*time.Millisecond))
		require.NoError(t, err)

		require.Equal(t, int64(1), created.Load())
		c1.Release(nil)

		// wait for idle close
		require.Eventually(t, func() bool {
			p.mu.Lock()
			defer p.mu.Unlock()
			return len(p.idle) == 0 && len(p.active) == 0
		}, 200*time.Millisecond, 50*time.Millisecond)
	})

	t.Run("error", func(t *testing.T) {

	})

	t.Run("load", func(t *testing.T) {
		//client := New([]string{addr.String()})
		//p, _ := tpool(addr, PoolConfig{MaxConns: 5})
		//client.Pool = p

	})
}

func tpool(addr net.Addr, config PoolConfig) (*Pool, *atomic.Int64) {
	created := atomic.Int64{}
	d := net.Dialer{}
	dctx := func(ctx context.Context, network, addr string) (net.Conn, error) {
		created.Add(1)
		return d.DialContext(ctx, "tcp", addr)
	}

	pool := NewPool(addr, dctx, config)
	return pool, &created
}

func tctx(t *testing.T, d time.Duration) context.Context {
	ctx, cancel := context.WithTimeout(context.Background(), d)
	t.Cleanup(cancel)
	return ctx
}

func setupMemcached(t *testing.T) net.Addr {
	pool, err := dockertest.NewPool("")
	require.NoError(t, err)
	require.NoError(t, pool.Client.Ping())

	fmt.Println("Starting memcached container...")
	resource, err := pool.RunWithOptions(&dockertest.RunOptions{
		Repository:   "memcached",
		Tag:          "1.6.12",
		ExposedPorts: []string{"11211"},
	})
	require.NoError(t, err)

	t.Cleanup(func() {
		require.NoError(t, pool.Purge(resource))
		fmt.Println("Memcached container purged")
	})

	addr := fmt.Sprintf("localhost:%s", resource.GetPort("11211/tcp"))
	fmt.Println("Memcached container started at: ", addr)

	fmt.Println("Waiting for memcached to be ready...")
	require.NoError(t, pool.Retry(func() error {
		return ping(addr)
	}))

	fmt.Println("Memcached is ready")

	return dockerAddr{address: addr}
}

type dockerAddr struct {
	address string
}

func (a dockerAddr) Network() string {
	return "tcp"
}

func (a dockerAddr) String() string {
	return a.address
}

func ping(addr string) error {
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		return err
	}
	defer conn.Close()

	rw := bufio.NewReadWriter(bufio.NewReader(conn), bufio.NewWriter(conn))

	if _, err := fmt.Fprintf(rw, "version\r\n"); err != nil {
		return err
	}
	if err := rw.Flush(); err != nil {
		return err
	}
	line, err := rw.ReadSlice('\n')
	if err != nil {
		return err
	}
	if string(line) != "VERSION 1.6.12\r\n" {
		return fmt.Errorf("unexpected response: %q", line)
	}
	return nil
}
