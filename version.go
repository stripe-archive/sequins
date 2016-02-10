package main

import (
	"bytes"
	"errors"
	"fmt"
	"io/ioutil"
	"log"
	"math/rand"
	"net/http"
	"path"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/stripe/sequins/blocks"
)

// /sequins/
//   nodes/
//     node1
//     node2
//   partitions/
//     foo/
//       version1/
//         00000:node1
//         00001:node2

var errNoValidPartitions = errors.New("all partitions are fully replicated")
var errNoAvailablePeers = errors.New("no available peers")
var errProxiedIncorrectly = errors.New("this server doesn't have the requested partition")

// TODO: need a way to clean up zookeeper cruft

// TODO: verify that the set of files stays consistent; once at startup, once
// when we finish indexing, and then maybe periodically after that.

// A version represents a single version of a particular sequins db: in
// other words, a collection of files. In the distributed case, it understands
// partitions and can route requests.
type version struct {
	name          string
	created       time.Time
	numPartitions int
	blockStore    *blocks.BlockStore

	peers     *peers
	zkWatcher *zkWatcher

	localPartitions  map[int]bool
	remotePartitions map[int][]string
	partitionLock    sync.RWMutex

	missingPartitions int
	partitionsReady   chan bool
}

// sync syncs the remote partitions from zoolander whenever they change.
func (vs *version) sync(updates chan []string) {
	for {
		nodes := <-updates
		vs.updatePartitions(nodes)
	}
}

func (vs *version) updatePartitions(nodes []string) {
	vs.partitionLock.Lock()
	defer vs.partitionLock.Unlock()

	remotePartitions := make(map[int][]string)
	for _, node := range nodes {
		parts := strings.SplitN(node, "@", 2)
		p, _ := strconv.Atoi(parts[0])
		host := parts[1]
		if host != vs.peers.address {
			remotePartitions[p] = append(remotePartitions[p], host)
		}
	}

	// Check for each partition. If every one is available on at least one node,
	// then we're ready to rumble.
	vs.remotePartitions = remotePartitions
	missing := 0
	for i := 0; i < vs.numPartitions; i++ {
		if _, ok := vs.localPartitions[i]; ok {
			continue
		}

		if _, ok := vs.remotePartitions[i]; ok {
			continue
		}

		missing += 1
	}

	vs.missingPartitions = missing
	if missing == 0 {
		select {
		case vs.partitionsReady <- true:
		default:
		}
	}
}

func (vs *version) waitReady() error {
	if vs.peers == nil {
		return nil
	}

	vs.missingPartitions = vs.numPartitions - len(vs.localPartitions)
	vs.partitionsReady = make(chan bool)

	partitionPath := path.Join("partitions", vs.name)
	err := vs.zkWatcher.createPath(partitionPath)
	if err != nil {
		return err
	}

	updates := vs.zkWatcher.watchChildren(partitionPath)
	go vs.sync(updates)

	// Advertise that our partitions are ready. At any point after this,
	// we could receive proxied requests for this version.
	vs.advertisePartitions()

	// Wait for all remote partitions to be available.
	for {
		vs.partitionLock.RLock()
		ready := (vs.missingPartitions == 0)
		vs.partitionLock.RUnlock()
		if ready {
			break
		}

		// TODO: seems to always wait 10s
		log.Printf("Waiting for all partitions to be available (missing %d)", vs.missingPartitions)

		t := time.NewTimer(10 * time.Second)
		select {
		case <-t.C:
		case <-vs.partitionsReady:
		}
	}

	return nil
}

// advertisePartitions creates an ephemeral node for each partition this local
// peer is responsible for.
// TODO: this should maybe be a zk multi op?
func (vs *version) advertisePartitions() {
	for partition := range vs.localPartitions {
		node := fmt.Sprintf("%05d@%s", partition, vs.peers.address)
		vs.zkWatcher.createEphemeral(path.Join("partitions", vs.name, node))
	}
}

// ServeHTTP is the entrypoint for requests.
func (vs *version) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	key := strings.TrimPrefix(r.URL.Path, "/")
	res, err := vs.get(key, r)

	if err != nil {
		log.Printf("Error fetching value for %s: %s\n", key, err)
		w.WriteHeader(http.StatusInternalServerError)
	} else if res == nil {
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
func (vs *version) get(key string, r *http.Request) ([]byte, error) {
	partition := blocks.KeyPartition(key, vs.numPartitions)

	proxied := (r.URL.Query().Get("proxy") != "")
	isLocal := false
	if vs.localPartitions == nil {
		isLocal = true
	} else {
		vs.partitionLock.RLock()
		isLocal = vs.localPartitions[partition]
		vs.partitionLock.RUnlock()
	}

	if isLocal {
		res, err := vs.blockStore.Get(key)
		if err == blocks.ErrNotFound {
			err = nil // TODO this is silly
		}

		return res, err
	} else if !proxied {
		return vs.proxyRequest(key, partition, r)
	} else {
		return nil, errProxiedIncorrectly
	}
}

// proxyRequest proxies the request, trying each peer that should have the key
// in turn.
func (vs *version) proxyRequest(key string, partition int, r *http.Request) ([]byte, error) {
	vs.partitionLock.RLock()
	peers := vs.remotePartitions[partition]
	vs.partitionLock.RUnlock()

	// TODO: don't blacklist nodes, but we can weight them lower
	shuffled := make([]string, len(peers))
	perm := rand.Perm(len(peers))
	for i, v := range perm {
		shuffled[v] = peers[i]
	}

	client := &http.Client{Timeout: 100 * time.Millisecond} // TODO configurable
	r.URL.RawQuery = fmt.Sprintf("proxy=%s", vs.name)
	r.URL.Scheme = "http"
	r.RequestURI = ""

	for _, peer := range shuffled {
		log.Println("Trying peer", peer, "for key", key, "with partition", partition)

		r.URL.Host = peer
		resp, err := client.Do(r)
		if err != nil {
			log.Printf("Error proxying request to peer %s: %s\n", peer, err)
			continue
		} else if resp.StatusCode != 200 {
			log.Printf("Error proxying request to peer %s: got %s\n", peer, resp.StatusCode)
			continue
		}

		defer resp.Body.Close()
		return ioutil.ReadAll(resp.Body)
	}

	return nil, errNoAvailablePeers
}

// TODO: cleanup zk state. not everything is ephemeral, and we may be long-running w/ multiple versions

func (vs *version) close() error {
	return vs.blockStore.Close()
}
