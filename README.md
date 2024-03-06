## About

This is a memcache client library for the Go programming language
(http://golang.org/).

It is a fork of https://github.com/bradfitz/gomemcache/memcache, 
with support for Autodiscovery client based on https://cloud.google.com/memorystore/docs/memcached/auto-discovery-overview

## Example

Install with:

```shell
$ go get github.com/bradfitz/gomemcache/memcache
```

Then use it like:

```go
import (
    "github.com/bradfitz/gomemcache/memcache"
)

func main() {
     mc := memcache.New("10.0.0.1:11211", "10.0.0.2:11211", "10.0.0.3:11212")
     mc.Set(&memcache.Item{Key: "foo", Value: []byte("my value")})

     it, err := mc.Get("foo")
     ...
}
```

## Example with Autodiscovery

```go
import (
        "github.com/ankitkinra/gomemcache/memcache"
)

func main() {
     mcDiscovery := memcache.NewDiscoveryClient("10.0.0.1:11211", 30 * time.Second)
     mcDiscovery.Set(&memcache.Item{Key: "foo", Value: []byte("my value")})

     it, err := mc.Get("foo")
     ...
     mcDiscovery.Stop()
}
```

### Note
Remember to call Stop() on the discovery enabled client to stop the polling, else this can leak "go" methods.

