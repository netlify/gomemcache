/*
Copyright 2020 Google Inc.

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

// Package memcache provides a client for the memcached cache server.
package memcache

import (
	"context"
	"errors"
	"net"
	"os/exec"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"
)

const (
	testPollingTime          = 2 * time.Millisecond
	portLow                  = 49152
	portHigh                 = 65535
	memcachedCreationTimeout = 5 * time.Second

	teardownTimeout                = 100 * time.Millisecond
	discoveryStabilizationDuration = 10 * time.Millisecond
	memcachedStabilizationDuration = 20 * time.Millisecond
)

var fakeDiscoveryServer = fakeDiscoveryMemcacheServer{}
var ready = make(chan bool)
var exit = make(chan bool)

// Run the memcached binary as a child process and connect to its unix socket.
func TestDiscoveryUnixSocket(t *testing.T) {
	ctx := context.Background()
	defer ctx.Done()
	fakeDiscoveryServer.start(ctx)
	t.Cleanup(teardown)
	openPorts := findOpenLocalHostPort(portLow, portHigh, 2)
	if openPorts == nil || len(openPorts) < 2 {
		t.Fatalf("could not find two open ports, openPorts:%v", openPorts)
		return
	}

	go startMemcachedServer(t, openPorts[0], ready, exit)
	go startMemcachedServer(t, openPorts[1], ready, exit)
	s1Ready, waitErr := waitOnChannelWithTimeout(ready, memcachedCreationTimeout)
	if waitErr != nil {
		t.Fatalf("memcache server could not be created due to %v", waitErr)
	}
	s2Ready, waitErr := waitOnChannelWithTimeout(ready, memcachedCreationTimeout)
	if waitErr != nil {
		t.Fatalf("memcache server could not be created due to %v", waitErr)
	}
	if !s1Ready || !s2Ready {
		t.Skipf("one of the memcached server was not ready.")
	}

	fakeDiscoveryServer.updateDiscoveryInformation(1, openPorts)
	discoveryClient, err := newDiscoveryClient(fakeDiscoveryServer.currentAddress, new(ServerList), testPollingTime)
	if err != nil {
		t.Fatalf("could not create discovery client due to %v", err)
	}
	testWithDiscoveryClient(t, &fakeDiscoveryServer, discoveryClient)
	discoveryClient.stopPolling()
}

func waitOnChannelWithTimeout(c chan bool, timeout time.Duration) (bool, error) {
	select {
	case res := <-c:
		return res, nil
	case <-time.After(timeout):
		return false, errors.New("channel timed out")
	}
}

func teardown() {
	fakeDiscoveryServer.stop()
	close(exit)
	time.Sleep(teardownTimeout)
}

func getCurrentServerPorts(c *Client) []int {
	serverAddress := make([]int, 0, 3)
	recordAllServers := func(a net.Addr) error {
		_, portStr, err := net.SplitHostPort(a.String())
		if err != nil {
			return err
		}
		port, err := strconv.ParseInt(portStr, 10, 0)
		if err != nil {
			return err
		}
		serverAddress = append(serverAddress, int(port))
		return nil
	}
	c.selector.Each(recordAllServers)
	sort.Ints(serverAddress)
	return serverAddress
}

// Equal tells whether a and b contain the same elements.
func equalIntArray(slice1, slice2 []int) bool {
	if len(slice1) != len(slice2) {
		return false
	}
	for i, value := range slice1 {
		if value != slice2[i] {
			return false
		}
	}
	return true
}

func testSingleConfigCase(t *testing.T, fakeMemcacheServer *fakeDiscoveryMemcacheServer, c *Client, discoveryID int, portsToSet, expectedPorts []int) {
	fakeMemcacheServer.updateDiscoveryInformation(discoveryID, portsToSet)
	time.Sleep(discoveryStabilizationDuration)
	discoveryPorts := getCurrentServerPorts(c)
	if !equalIntArray(expectedPorts, discoveryPorts) {
		t.Fatalf("configId:%v want: %v != got %v", discoveryID, expectedPorts, discoveryPorts)
	}
}

func testSingleInvalidConfigCase(t *testing.T, fakeMemcacheServer *fakeDiscoveryMemcacheServer, c *Client, discoveryResponse string, expectedPorts []int) {
	fakeMemcacheServer.updateDiscoveryResponse(discoveryResponse)
	time.Sleep(discoveryStabilizationDuration)
	discoveryPorts := getCurrentServerPorts(c)
	if !equalIntArray(expectedPorts, discoveryPorts) {
		t.Fatalf("discoveryResponse:%v want: %v != got %v", discoveryResponse, expectedPorts, discoveryPorts)
	}
}

func testValidConfigChange(t *testing.T, fakeMemcacheServer *fakeDiscoveryMemcacheServer, c *Client) {
	originalPortList := getCurrentServerPorts(c)

	// Greater config id should update discovery information
	newPorts := []int{1, 2, 3}
	testSingleConfigCase(t, fakeMemcacheServer, c, 3, newPorts, newPorts)

	// Update to original configuration
	testSingleConfigCase(t, fakeMemcacheServer, c, 20, originalPortList, originalPortList)

	// Same config id should not change the config
	testSingleConfigCase(t, fakeMemcacheServer, c, 20, newPorts, originalPortList)

	// Older config id should
	testSingleConfigCase(t, fakeMemcacheServer, c, 19, newPorts, originalPortList)

	// Not found case with config id 0
	testSingleConfigCase(t, fakeMemcacheServer, c, 0, newPorts, originalPortList)
}

func testInvalidConfigChange(t *testing.T, fakeMemcacheServer *fakeDiscoveryMemcacheServer, c *Client) {
	originalPortList := getCurrentServerPorts(c)

	// Completely broken response
	testSingleInvalidConfigCase(t, fakeMemcacheServer, c, "broken", originalPortList)

	// Partially broken response with intparse error
	var result strings.Builder
	result.WriteString("CONFIG cluster 0 80\r\n")
	result.WriteString("100\r\n")
	result.WriteString("localhost|localhost|brokenInt")
	result.WriteString("\n\r\n")
	testSingleInvalidConfigCase(t, fakeMemcacheServer, c, result.String(), originalPortList)
}

func testWithDiscoveryClient(t *testing.T, fakeMemcacheServer *fakeDiscoveryMemcacheServer, c *Client) {
	// Run discovery config tests
	if fakeMemcacheServer != nil {
		testValidConfigChange(t, fakeMemcacheServer, c)
		testInvalidConfigChange(t, fakeMemcacheServer, c)
	}

	// reuse the other test library
	testWithClient(t, c)
}

func startMemcachedServer(t *testing.T, port int, ready chan<- bool, exit <-chan bool) error {
	t.Logf("starting memcached server on port: %d", port)
	cmd := exec.Command("memcached", "-p", strconv.Itoa(port))
	t.Logf("starting memcached server with command : %v", cmd)
	if err := cmd.Start(); err != nil {
		ready <- false
		return errors.New("could not find memcached server")
	}
	t.Logf("started memcached server on port:%d", port)
	// Allow the server to come up
	time.Sleep(memcachedStabilizationDuration)
	ready <- true

	<-exit
	cmd.Process.Kill()
	time.Sleep(memcachedStabilizationDuration)
	t.Logf("memcached server on port:%d exited", port)
	return nil
}

func findOpenLocalHostPort(portLow, portHigh, openPortsToFind int) []int {
	timeout := 10 * time.Millisecond
	openPorts := make([]int, 0, 2)
	for i := portLow; i < portHigh; i++ {
		addressToTry := net.JoinHostPort("localhost", strconv.Itoa(i))
		conn, err := net.DialTimeout("tcp", addressToTry, timeout)
		// if connection is refused, it could be a free port
		if err != nil && strings.Contains(err.Error(), "connection refused") {
			// try opening a tcp connection and if it succeeds this is a good port
			l, err1 := net.Listen("tcp", addressToTry)
			if err1 == nil {
				openPorts = append(openPorts, i)
				l.Close()
				if len(openPorts) == 2 {
					break
				}
			}
		} else if conn != nil {
			conn.Close()
		}
	}
	return openPorts
}
