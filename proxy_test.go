package main

import (
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var proxyTestVersion = &version{
	name: "foo",
	sequins: &sequins{
		config: sequinsConfig{
			Sharding: shardingConfig{
				ProxyTimeout:      duration{30 * time.Millisecond},
				ProxyStageTimeout: duration{10 * time.Millisecond},
			},
		},
	},
}

func httptestHost(s *httptest.Server) string {
	parsed, _ := url.Parse(s.URL)
	return parsed.Host
}

func TestProxySinglePeer(t *testing.T) {
	w := httptest.NewRecorder()
	peer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprintln(w, "all good")
	}))

	peers := []string{httptestHost(peer)}

	r, _ := http.NewRequest("GET", "http://localhost", nil)
	res, err := proxyTestVersion.proxy(w, r, peers)
	// Port changes on every test run.
	host, _, err := net.SplitHostPort(w.Header().Get("X-Sequins-Proxied-to"))
	assert.Equal(t, host, "127.0.0.1")
	assert.NoError(t, err, "simple proxying should work")
	assert.NotNil(t, res, "simple proxying should work")
}

func TestProxySlowPeer(t *testing.T) {
	w := httptest.NewRecorder()
	slowPeer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(100 * time.Millisecond)
		fmt.Fprintln(w, "sorry, did you need something?")
	}))

	goodPeer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprintln(w, "all good")
	}))

	notReachedPeer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		require.FailNow(t, "proxying should succeed before getting to a third peer")
	}))

	peers := []string{httptestHost(slowPeer), httptestHost(goodPeer), httptestHost(notReachedPeer)}
	r, _ := http.NewRequest("GET", "http://localhost", nil)
	res, err := proxyTestVersion.proxy(w, r, peers)
	require.NoError(t, err, "proxying should work on the second peer")
	require.NotNil(t, res, "proxying should work on the second peer")

	assert.Equal(t, "all good\n", string(res))
}

func TestProxyErrorPeer(t *testing.T) {
	w := httptest.NewRecorder()
	errorPeer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(500)
	}))

	goodPeer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprintln(w, "all good")
	}))

	notReachedPeer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Fail(t, "proxying should succeed before getting to a third peer")
	}))

	peers := []string{httptestHost(errorPeer), httptestHost(goodPeer), httptestHost(notReachedPeer)}
	r, _ := http.NewRequest("GET", "http://localhost", nil)
	res, err := proxyTestVersion.proxy(w, r, peers)
	require.NoError(t, err, "proxying should work on the second peer")
	require.NotNil(t, res, "proxying should work on the second peer")

	assert.Equal(t, "all good\n", string(res))
}

func TestProxySlowPeerErrorPeer(t *testing.T) {
	w := httptest.NewRecorder()
	slowPeer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(15 * time.Millisecond)
		fmt.Fprintln(w, "all good, sorry to keep you waiting")
	}))

	errorPeer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(500)
	}))

	peers := []string{httptestHost(slowPeer), httptestHost(errorPeer)}
	r, _ := http.NewRequest("GET", "http://localhost", nil)
	res, err := proxyTestVersion.proxy(w, r, peers)
	require.NoError(t, err, "proxying should work on the first peer")
	require.NotNil(t, res, "proxying should work on the first peer")

	assert.Equal(t, "all good, sorry to keep you waiting\n", string(res))
}

func TestProxyTimeout(t *testing.T) {
	w := httptest.NewRecorder()
	slowPeer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(100 * time.Millisecond)
		fmt.Fprintln(w, "sorry, did you need something?")
	}))

	notReachedPeer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Fail(t, "proxying should never try a fourth peer")
	}))

	peers := []string{
		httptestHost(slowPeer),
		httptestHost(slowPeer),
		httptestHost(slowPeer),
		httptestHost(notReachedPeer),
	}

	r, _ := http.NewRequest("GET", "http://localhost", nil)
	res, err := proxyTestVersion.proxy(w, r, peers)
	assert.Equal(t, errProxyTimeout, err, "proxying should time out with all slow peers")
	assert.Nil(t, res, "proxying should time out with all slow peers")
}

func TestProxyErrors(t *testing.T) {
	w := httptest.NewRecorder()
	errorPeer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(500)
	}))

	peers := []string{
		httptestHost(errorPeer),
		httptestHost(errorPeer),
		httptestHost(errorPeer),
	}

	r, _ := http.NewRequest("GET", "http://localhost", nil)
	res, err := proxyTestVersion.proxy(w, r, peers)
	assert.Equal(t, errNoAvailablePeers, err, "proxying should return errNoAvailablePeers if all error")
	assert.Nil(t, res, "proxying should return errNoAvailablePeers if all error")
}
