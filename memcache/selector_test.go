/*
Copyright 2014 Google Inc.

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
	"math/rand"
	"net"
	"testing"

	"github.com/stretchr/testify/require"
)

func BenchmarkPickServer(b *testing.B) {
	// at least two to avoid 0 and 1 special cases:
	benchPickServer(b, "127.0.0.1:1234", "127.0.0.1:1235")
}

func BenchmarkPickServer_Single(b *testing.B) {
	benchPickServer(b, "127.0.0.1:1234")
}

func benchPickServer(b *testing.B, servers ...string) {
	b.ReportAllocs()
	var ss ServerList
	ss.SetServers(servers...)
	for i := 0; i < b.N; i++ {
		if _, err := ss.PickServer("some key"); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkPickAnyServer(b *testing.B) {
	// at least two to avoid 0 and 1 special cases:
	benchPickAnyServer(b, "127.0.0.1:1234", "127.0.0.1:1235")
}

func TestPickAnyServer(t *testing.T) {
	pickServerTests := []struct {
		serverList            []string
		expectedServersPicked int
	}{
		{[]string{"127.0.0.1:1234"}, 1},
		{[]string{"127.0.0.1:1234", "127.0.0.1:1235", "127.0.0.1:1236"}, 2},
	}
	for _, tt := range pickServerTests {
		var ss ServerList
		ss.SetServers(tt.serverList...)
		serverCounter := make(map[string]int)
		for i := 0; i < 1000; i++ {
			var addr net.Addr
			var err error
			if addr, err = ss.PickAnyServer(); err != nil {
				t.Errorf("pickAnyServer(%v) failed due to %v", tt.serverList, err)
			}
			serverCounter[addr.String()]++
		}
		// Verify that server counter contains at least 2 values.
		if len(serverCounter) < tt.expectedServersPicked {
			t.Errorf("failed to randomize server list (%v), serverCounter (%v). got:%v, want at least:%v", tt.serverList, serverCounter, len(serverCounter), tt.expectedServersPicked)
		}
	}
}

func TestPickAnyServerThrows(t *testing.T) {
	var ss ServerList
	if _, err := ss.PickAnyServer(); err != ErrNoServers {
		t.Errorf("expected error with no servers, got:%v", err)
	}
}

func BenchmarkPickAnyServer_Single(b *testing.B) {
	benchPickAnyServer(b, "127.0.0.1:1234")
}

func benchPickAnyServer(b *testing.B, servers ...string) {
	b.ReportAllocs()
	var ss ServerList
	ss.SetServers(servers...)
	for i := 0; i < b.N; i++ {
		if _, err := ss.PickAnyServer(); err != nil {
			b.Fatal(err)
		}
	}
}

var servers = []string{"127.0.0.1:1000", "127.0.0.1:1001", "127.0.0.1:1002", "127.0.0.1:1003", "127.0.0.1:1004",
	"127.0.0.1:1005", "127.0.0.1:1006", "127.0.0.1:1007", "127.0.0.1:1008", "127.0.0.1:1009"}

func TestRendezvousSelector_PickServer(t *testing.T) {
	t.Run("empty returns error", func(t *testing.T) {
		rs := RendezvousSelector{&ServerList{}}
		_, err := rs.PickServer("key")
		require.ErrorIs(t, err, ErrNoServers)
	})

	t.Run("single node", func(t *testing.T) {
		sl := ServerList{}
		server := "127.0.0.1:1234"
		require.NoError(t, sl.SetServers(server))

		rs := RendezvousSelector{&sl}

		keys := randKeys(1000)
		for _, key := range keys {
			target, err := rs.PickServer(key)
			require.NoError(t, err)
			require.Equal(t, server, target.String())
		}
	})

	t.Run("key is consistently routed", func(t *testing.T) {
		sl := ServerList{}
		require.NoError(t, sl.SetServers(servers...))
		rs := RendezvousSelector{&sl}

		keys := randKeys(1000)
		previous := make(map[string]net.Addr)
		for i := 0; i < 100; i++ {
			rand.Shuffle(len(keys), func(i, j int) {
				keys[i], keys[j] = keys[j], keys[i]
			})

			for _, key := range keys {
				target, err := rs.PickServer(key)
				require.NoError(t, err)

				_, ok := previous[key]
				if !ok {
					previous[key] = target
				}

				require.Equal(t, previous[key].String(), target.String())
			}
		}
	})

	t.Run("all servers are routed to", func(t *testing.T) {
		sl := ServerList{}
		require.NoError(t, sl.SetServers(servers...))
		rs := RendezvousSelector{&sl}

		keys := randKeys(1000)
		set := make(map[string]struct{})
		for _, key := range keys {
			target, err := rs.PickServer(key)
			require.NoError(t, err)
			set[target.String()] = struct{}{}
		}

		require.Equal(t, len(servers), len(set))
	})

	t.Run("server added", func(t *testing.T) {
		sl := ServerList{}
		require.NoError(t, sl.SetServers(servers...))
		rs := RendezvousSelector{&sl}

		keys := randKeys(1000)
		previous := make(map[string]net.Addr)
		for _, k := range keys {
			target, err := rs.PickServer(k)
			require.NoError(t, err)
			previous[k] = target
		}

		require.NoError(t, rs.SetServers(append(servers, "127.0.0.1:1010")...))

		var migrated int
		for _, k := range keys {
			target, err := rs.PickServer(k)
			require.NoError(t, err)

			if previous[k].String() != target.String() {
				migrated++
			}
		}

		require.InDelta(t, 90, migrated, 60) // expect about 1000/11 migrations
	})

	t.Run("server removed", func(t *testing.T) {
		sl := ServerList{}
		require.NoError(t, sl.SetServers(servers...))
		rs := RendezvousSelector{&sl}

		keys := randKeys(1000)
		previous := make(map[string]net.Addr)
		for _, k := range keys {
			target, err := rs.PickServer(k)
			require.NoError(t, err)
			previous[k] = target
		}

		require.NoError(t, rs.SetServers(append(servers[:len(servers)-1])...))

		var migrated int
		for _, k := range keys {
			target, err := rs.PickServer(k)
			require.NoError(t, err)

			if previous[k].String() != target.String() {
				migrated++
			}
		}

		require.InDelta(t, 100, migrated, 50) // expect about 1000/10 migrations
	})
}

func BenchmarkRendezvousSelector_PickServer(b *testing.B) {
	sl := ServerList{}
	require.NoError(b, sl.SetServers(servers...))
	rs := RendezvousSelector{&sl}
	key := randString(64)

	b.ReportAllocs()
	var result net.Addr
	for i := 0; i < b.N; i++ {
		result, _ = rs.PickServer(key)
	}

	require.NotNil(b, result)
}

var letters = []rune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ")

func randString(n int) string {
	b := make([]rune, n)
	for i := range b {
		b[i] = letters[rand.Intn(len(letters))]
	}
	return string(b)
}

func randKeys(n int) (keys []string) {
	for i := 0; i < n; i++ {
		keys = append(keys, randString(64))
	}
	return
}
