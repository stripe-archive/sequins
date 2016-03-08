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

func (vs *version) waitForPeers() bool {
	if vs.sequins.peers == nil || vs.numPartitions == 0 {
		return true
	}

	return vs.partitions.advertiseAndWait()
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
		w.Header().Add("X-Sequins-Version", vs.name)
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
	if vs.numPartitions == 0 {
		return nil, nil
	}

	partition, alternatePartition := blocks.KeyPartition(key, vs.numPartitions)
	if vs.hasPartition(partition) || vs.hasPartition(alternatePartition) {
		return vs.blockStore.Get(key)
	} else if r.URL.Query().Get("proxy") == "" {
		res, err := vs.proxyRequest(partition, r)
		if res == nil && err == nil && alternatePartition != partition {
			log.Println("Trying alternate partition for pathological key", key)
			res, err = vs.proxyRequest(alternatePartition, r)
		}

		return res, err
	} else {
		return nil, errProxiedIncorrectly
	}

}

// hasPartition returns true if we have the partition available locally.
func (vs *version) hasPartition(partition int) bool {
	return vs.partitions == nil || vs.partitions.local[partition]
}

// proxyRequest proxies the request, trying each peer that should have the key
// in turn.
func (vs *version) proxyRequest(partition int, r *http.Request) ([]byte, error) {
	peers := vs.partitions.getPeers(partition)

	// Shuffle the peers, so we try them in a random order.
	// TODO: don't blacklist nodes, but we can weight them lower
	shuffled := make([]string, len(peers))
	perm := rand.Perm(len(peers))
	for i, v := range perm {
		shuffled[v] = peers[i]
	}

	for _, peer := range shuffled {
		res, err := vs.proxyAttempt(peer, r)
		if err != nil {
			log.Printf("Error proxying request to peer %s: %s", r.URL.String(), err)
			continue
		}

		return res, nil
	}

	return nil, errNoAvailablePeers
}

func (vs *version) proxyAttempt(peer string, r *http.Request) ([]byte, error) {
	r.URL.RawQuery = fmt.Sprintf("proxy=%s", vs.name)
	r.URL.Scheme = "http"
	r.URL.Host = peer
	proxyRequest, err := http.NewRequest("GET", r.URL.String(), nil)
	if err != nil {
		return nil, err
	}

	resp, err := vs.sequins.proxyClient.Do(proxyRequest)
	if err != nil {
		return nil, err
	}

	defer resp.Body.Close()
	if resp.StatusCode == 404 {
		return nil, nil
	} else if resp.StatusCode != 200 {
		return nil, fmt.Errorf("got %d", resp.StatusCode)
	}

	return ioutil.ReadAll(resp.Body)
}

func (vs *version) close() {
	if vs.partitions != nil {
		vs.partitions.close()
	}

	if vs.blockStore != nil {
		vs.blockStore.Close()
	}
}

func (vs *version) delete() error {
	if vs.blockStore != nil {
		return vs.blockStore.Delete()
	}

	return nil
}
