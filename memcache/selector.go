/*
Copyright 2011 The gomemcache AUTHORS

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

     http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package memcache

import (
	"hash/crc32"
	"math/rand"
	"net"
	"strings"
	"sync"
)

// Note: If you are implementing ServerSelector, you will have to implement the
// new method PickAnyServer

// ServerSelector is the interface that selects a memcache server
// as a function of the item's key.
//
// All ServerSelector implementations must be safe for concurrent use
// by multiple goroutines.
type ServerSelector interface {
	// PickServer returns the server address that a given item
	// should be shared onto.
	PickServer(key string) (net.Addr, error)

	// PickAnyServer returns any active server, preferably not the
	// same one every time in order to distribute the load.
	// This can be used to get information which is server agnostic.
	PickAnyServer() (net.Addr, error)
	Each(func(net.Addr) error) error

	// SetServers updates the list of servers available for selection.
	SetServers(servers ...string) error
}

var (
	_ ServerSelector = (*ServerList)(nil)
	_ ServerSelector = RendezvousSelector{}
)

// ServerList is a simple ServerSelector. Its zero value is usable.
type ServerList struct {
	mu    sync.RWMutex
	addrs []net.Addr
}

// staticAddr caches the Network() and String() values from any net.Addr.
type staticAddr struct {
	ntw, str string
}

func newStaticAddr(a net.Addr) net.Addr {
	return &staticAddr{
		ntw: a.Network(),
		str: a.String(),
	}
}

func (s *staticAddr) Network() string { return s.ntw }
func (s *staticAddr) String() string  { return s.str }

// SetServers changes a ServerList's set of servers at runtime and is
// safe for concurrent use by multiple goroutines.
//
// Each server is given equal weight. A server is given more weight
// if it's listed multiple times.
//
// SetServers returns an error if any of the server names fail to
// resolve. No attempt is made to connect to the server. If any error
// is returned, no changes are made to the ServerList.
func (ss *ServerList) SetServers(servers ...string) error {
	naddr := make([]net.Addr, len(servers))
	for i, server := range servers {
		if strings.Contains(server, "/") {
			addr, err := net.ResolveUnixAddr("unix", server)
			if err != nil {
				return err
			}
			naddr[i] = newStaticAddr(addr)
		} else {
			tcpaddr, err := net.ResolveTCPAddr("tcp", server)
			if err != nil {
				return err
			}
			naddr[i] = newStaticAddr(tcpaddr)
		}
	}

	ss.mu.Lock()
	defer ss.mu.Unlock()
	ss.addrs = naddr
	return nil
}

// Each iterates over each server calling the given function
func (ss *ServerList) Each(f func(net.Addr) error) error {
	ss.mu.RLock()
	defer ss.mu.RUnlock()
	for _, a := range ss.addrs {
		if err := f(a); nil != err {
			return err
		}
	}
	return nil
}

// keyBufPool returns []byte buffers for use by PickServer's call to
// crc32.ChecksumIEEE to avoid allocations. (but doesn't avoid the
// copies, which at least are bounded in size and small)
var keyBufPool = sync.Pool{
	New: func() interface{} {
		b := make([]byte, 256)
		return &b
	},
}

// PickAnyServer picks any active server
// This can be used to get information which is not linked to a key or which could be on any server.
func (ss *ServerList) PickAnyServer() (net.Addr, error) {
	ss.mu.RLock()
	defer ss.mu.RUnlock()
	if len(ss.addrs) == 0 {
		return nil, ErrNoServers
	}

	if len(ss.addrs) == 1 {
		return ss.addrs[0], nil
	}
	return ss.addrs[rand.Intn(len(ss.addrs))], nil

}

func (ss *ServerList) PickServer(key string) (net.Addr, error) {
	ss.mu.RLock()
	defer ss.mu.RUnlock()
	if len(ss.addrs) == 0 {
		return nil, ErrNoServers
	}
	if len(ss.addrs) == 1 {
		return ss.addrs[0], nil
	}
	bufp := keyBufPool.Get().(*[]byte)
	n := copy(*bufp, key)
	cs := crc32.ChecksumIEEE((*bufp)[:n])
	keyBufPool.Put(bufp)

	return ss.addrs[cs%uint32(len(ss.addrs))], nil
}

func NewRendezvousSelector() RendezvousSelector {
	return RendezvousSelector{new(ServerList)}
}

// RendezvousSelector is a ServerSelector which uses the rendezvous hashing algorithm to select a server for a given key.
// See https://en.wikipedia.org/wiki/Rendezvous_hashing
type RendezvousSelector struct {
	*ServerList
}

// PickServer uses the rendezvous hashing algorithm.
// It loops over each server, hashes key+server together, and selects the server with the largest hash value.
// Key -> server mappings should be deterministic and uniformly distributed.
// This method is threadsafe.
func (rs RendezvousSelector) PickServer(key string) (net.Addr, error) {
	var (
		count    int
		maxScore uint32
		maxNode  net.Addr
		buf      = make([]byte, 0, 256) // max cache key size is 250, go allocates in base 2
	)

	buf = append(buf, key...)

	err := rs.ServerList.Each(func(addr net.Addr) error {
		count++
		buf = buf[:len(key)] // truncate the buffer, after this the buffer will only contain the key, server bytes will be removed.
		buf = append(buf, addr.String()...)
		score := crc32.ChecksumIEEE(buf)

		if score > maxScore || maxNode == nil || (score == maxScore && addr.String() > maxNode.String()) {
			maxScore = score
			maxNode = addr
		}

		return nil
	})

	if count == 0 {
		return nil, ErrNoServers
	}

	return maxNode, err
}
