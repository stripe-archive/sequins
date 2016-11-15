package main

import (
	"fmt"
	"io"
	"io/ioutil"
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
				ProxyTimeout:      duration{300 * time.Millisecond},
				ProxyStageTimeout: duration{100 * time.Millisecond},
			},
		},
	},
}

func readAll(t *testing.T, r io.Reader) string {
	b, err := ioutil.ReadAll(r)
	require.NoError(t, err)
	return string(b)
}

func httptestHost(s *httptest.Server) string {
	parsed, _ := url.Parse(s.URL)
	return parsed.Host
}

func TestProxySinglePeer(t *testing.T) {
	goodPeer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprintln(w, "all good")
	}))

	peers := []string{httptestHost(goodPeer)}
	r, _ := http.NewRequest("GET", "http://localhost", nil)
	res, peer, err := proxyTestVersion.proxy(r, peers)

	require.NoError(t, err, "simple proxying should work")
	assert.NotNil(t, res, "simple proxying should work")

	assert.Equal(t, httptestHost(goodPeer), peer, "the returned peer should be correct")
	assert.Equal(t, "all good\n", readAll(t, res.Body))
}

func TestProxySlowPeer(t *testing.T) {
	slowPeer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(1 * time.Second)
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
	res, peer, err := proxyTestVersion.proxy(r, peers)
	require.NoError(t, err, "proxying should work on the second peer")
	require.NotNil(t, res, "proxying should work on the second peer")

	assert.Equal(t, httptestHost(goodPeer), peer, "the returned peer should be correct")
	assert.Equal(t, "all good\n", readAll(t, res.Body))
}

func TestProxyErrorPeer(t *testing.T) {
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
	res, peer, err := proxyTestVersion.proxy(r, peers)

	require.NoError(t, err, "proxying should work on the second peer")
	require.NotNil(t, res, "proxying should work on the second peer")

	assert.Equal(t, httptestHost(goodPeer), peer, "the returned peer should be correct")
	assert.Equal(t, "all good\n", readAll(t, res.Body))
}

func TestProxySlowPeerErrorPeer(t *testing.T) {
	slowPeer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(150 * time.Millisecond)
		fmt.Fprintln(w, "all good, sorry to keep you waiting")
	}))

	errorPeer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(500)
	}))

	peers := []string{httptestHost(slowPeer), httptestHost(errorPeer)}
	r, _ := http.NewRequest("GET", "http://localhost", nil)
	res, peer, err := proxyTestVersion.proxy(r, peers)

	require.NoError(t, err, "proxying should work on the first peer")
	require.NotNil(t, res, "proxying should work on the first peer")

	assert.Equal(t, httptestHost(slowPeer), peer, "the returned peer should be correct")
	assert.Equal(t, "all good, sorry to keep you waiting\n", readAll(t, res.Body))
}

func TestProxyTimeout(t *testing.T) {
	slowPeer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(1 * time.Second)
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
	res, peer, err := proxyTestVersion.proxy(r, peers)

	assert.Equal(t, errProxyTimeout, err, "proxying should time out with all slow peers")
	assert.Nil(t, res, "proxying should time out with all slow peers")
	assert.Equal(t, "", peer, "peer should be empty if proxying timed out")
}

func TestProxyErrors(t *testing.T) {
	errorPeer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(500)
	}))

	peers := []string{
		httptestHost(errorPeer),
		httptestHost(errorPeer),
		httptestHost(errorPeer),
	}

	r, _ := http.NewRequest("GET", "http://localhost", nil)
	res, peer, err := proxyTestVersion.proxy(r, peers)

	assert.Equal(t, errNoAvailablePeers, err, "proxying should return errNoAvailablePeers if all error")
	assert.Nil(t, res, "proxying should return errNoAvailablePeers if all error")
	assert.Equal(t, "", peer, "peer should be empty if proxying timed out")
}
