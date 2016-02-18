package main

import (
	"bytes"
	"errors"
	"fmt"
	"io/ioutil"
	"log"
	"math/rand"
	"net/http"
	"time"

	"github.com/stripe/sequins/blocks"
)

var errNoAvailablePeers = errors.New("no available peers")
var errProxiedIncorrectly = errors.New("this server doesn't have the requested partition")

// TODO: need a way to clean up zookeeper cruft

// TODO: verify that the set of files stays consistent; once at startup, once
// when we finish indexing, and then maybe periodically after that.

// A version represents a single version of a particular sequins db: in
// other words, a collection of files. In the distributed case, it understands
// partitions and can route requests.
type version struct {
	sequins *sequins

	db            string
	name          string
	created       time.Time
	blockStore    *blocks.BlockStore
	partitions    *partitions
	numPartitions int
}

func (vs *version) waitForPeers() {
	if vs.sequins.peers == nil {
		return
	}

	vs.partitions.advertiseAndWait()
}

// serveKey is the entrypoint for incoming HTTP requests.
func (vs *version) serveKey(w http.ResponseWriter, r *http.Request, key string) {
	res, err := vs.get(key, r)

	if err == errNoAvailablePeers {
		// All of our peers failed us. 503.
		log.Printf("All peers failed to respond for /%s/%s (version %s)", vs.db, key, vs.name)
		w.WriteHeader(http.StatusBadGateway)
	} else if err != nil {
		// Some other error. 500.
		log.Printf("Error fetching value for /%s/%s: %s\n", vs.db, key, err)
		w.WriteHeader(http.StatusInternalServerError)
	} else if res == nil {
		// Either the key doesn't exist locally, or we got back the
		// proxied response, and it didn't exist on the peer. 404.
		w.WriteHeader(http.StatusNotFound)
	} else {
		// Explicitly unset Content-Type, so ServeContent doesn't try to do any
		// sniffing.
		w.Header()["Content-Type"] = nil
		w.Header().Add("X-Sequins-Version", vs.name)

		http.ServeContent(w, r, key, vs.created, bytes.NewReader(res))
	}
}

// get looks up a value locally, or, failing that, asks a peer that has it.
// If the request was proxied, it is not proxied further.
func (vs *version) get(key string, r *http.Request) ([]byte, error) {
	partition := blocks.KeyPartition(key, vs.numPartitions)

	if vs.partitions == nil || vs.partitions.local[partition] {
		res, err := vs.blockStore.Get(key)
		if err == blocks.ErrNotFound {
			err = nil // TODO this is silly
		}

		return res, err
	} else if r.URL.Query().Get("proxy") == "" {
		return vs.proxyRequest(key, partition, r)
	} else {
		return nil, errProxiedIncorrectly
	}
}

// proxyRequest proxies the request, trying each peer that should have the key
// in turn.
func (vs *version) proxyRequest(key string, partition int, r *http.Request) ([]byte, error) {
	peers := vs.partitions.getPeers(partition)

	// TODO: don't blacklist nodes, but we can weight them lower
	shuffled := make([]string, len(peers))
	perm := rand.Perm(len(peers))
	for i, v := range perm {
		shuffled[v] = peers[i]
	}

	client := &http.Client{Timeout: vs.sequins.config.ZK.ProxyTimeout.Duration}
	r.URL.RawQuery = fmt.Sprintf("proxy=%s", vs.name)
	r.URL.Scheme = "http"
	r.RequestURI = ""

	for _, peer := range shuffled {
		r.URL.Host = peer
		resp, err := client.Do(r)
		if err != nil {
			log.Printf("Error proxying request to peer %s/%s: %s\n", peer, r.URL.String(), err)
			continue
		} else if resp.StatusCode == 404 {
			return nil, nil
		} else if resp.StatusCode != 200 {
			log.Printf("Error proxying request to peer %s/%s: got %d\n", peer, r.URL.String(), resp.StatusCode)
			continue
		}

		defer resp.Body.Close()
		return ioutil.ReadAll(resp.Body)
	}

	return nil, errNoAvailablePeers
}

func (vs *version) close() error {
	return vs.blockStore.Close()
}
