package main

import (
	"bytes"
	"errors"
	"fmt"
	"io/ioutil"
	"log"
	"math/rand"
	"net/http"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/stripe/sequins/blocks"
)

const versionHeader = "X-Sequins-Version"

var errNoAvailablePeers = errors.New("no available peers")
var errProxiedIncorrectly = errors.New("this server doesn't have the requested partition")

// A version represents a single version of a particular sequins db: in
// other words, a collection of files. In the sharding-enabled case, it
// understands distribution of partitions and can route requests.
type version struct {
	sequins *sequins

	path                    string
	db                      string
	name                    string
	created                 time.Time
	blockStore              *blocks.BlockStore
	blockStoreLock          sync.RWMutex
	partitions              *partitions
	numPartitions           int
	selectedLocalPartitions map[int]bool
}

func newVersion(sequins *sequins, path, db, name string, numPartitions int) *version {
	vs := &version{
		sequins:       sequins,
		path:          path,
		db:            db,
		name:          name,
		created:       time.Now(),
		numPartitions: numPartitions,
	}

	var local map[int]bool
	if sequins.peers != nil {
		vs.partitions = watchPartitions(sequins.zkWatcher, sequins.peers,
			db, name, numPartitions, sequins.config.Sharding.Replication)

		local = vs.partitions.pickLocalPartitions()
		vs.selectedLocalPartitions = local
	}

	// Try loading anything we have locally. If it doesn't work out, that's ok.
	_, err := os.Stat(filepath.Join(path, ".manifest"))
	if err == nil {
		log.Println("Loading version from manifest at", path)

		blockStore, err := blocks.NewFromManifest(path, local)
		if err != nil {
			log.Println("Error loading", vs.db, "version", vs.name, "from manifest:", err)
		}

		vs.blockStore = blockStore
		if vs.partitions != nil {
			vs.partitions.updateLocalPartitions(local)
		}
	}

	return vs
}

func (vs *version) getBlockStore() *blocks.BlockStore {
	vs.blockStoreLock.RLock()
	defer vs.blockStoreLock.RUnlock()

	return vs.blockStore
}

func (vs *version) ready() bool {

	if vs.numPartitions == 0 {
		return true
	} else if vs.sequins.peers == nil {
		return vs.getBlockStore() != nil
	}

	return vs.partitions.ready()
}

func (vs *version) advertiseAndWait() bool {
	if vs.sequins.peers == nil || vs.numPartitions == 0 {
		return true
	}

	return vs.partitions.advertiseAndWait()
}

// serveKey is the entrypoint for incoming HTTP requests.
func (vs *version) serveKey(w http.ResponseWriter, r *http.Request, key string) {
	res, err := vs.get(key, r)

	if err == errNoAvailablePeers {
		// All of our peers failed us. 502.
		log.Printf("All peers failed to respond for /%s/%s (version %s)", vs.db, key, vs.name)
		w.WriteHeader(http.StatusBadGateway)
	} else if err != nil {
		// Some other error. 500.
		log.Printf("Error fetching value for /%s/%s: %s\n", vs.db, key, err)
		w.WriteHeader(http.StatusInternalServerError)
	} else if res == nil {
		// Either the key doesn't exist locally, or we got back the
		// proxied response, and it didn't exist on the peer. 404.
		w.Header().Add(versionHeader, vs.name)
		w.WriteHeader(http.StatusNotFound)
	} else {
		// Explicitly unset Content-Type, so ServeContent doesn't try to do any
		// sniffing.
		w.Header()["Content-Type"] = nil
		w.Header().Add(versionHeader, vs.name)
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
	bs := vs.getBlockStore()
	if bs != nil && vs.hasPartition(partition) || vs.hasPartition(alternatePartition) {
		return bs.Get(key)
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
	return vs.getBlockStore() != nil && (vs.partitions == nil || vs.partitions.local[partition])
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

	bs := vs.getBlockStore()
	if bs != nil {
		bs.Close()
	}
}

func (vs *version) delete() error {
	bs := vs.getBlockStore()
	if bs != nil {
		return bs.Delete()
	}

	return nil
}
