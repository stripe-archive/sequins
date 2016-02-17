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
	numPartitions int
	blockStore    *blocks.BlockStore

	localPartitions  map[int]bool
	remotePartitions map[int][]string
	partitionLock    sync.RWMutex

	missingPartitions int
	partitionsReady   chan bool
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

	if vs.localPartitions == nil || vs.localPartitions[partition] {
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
	vs.partitionLock.RLock()
	peers := vs.remotePartitions[partition]
	vs.partitionLock.RUnlock()

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
			log.Printf("Error proxying request to peer %s/%s: got %s\n", peer, r.URL.String(), resp.StatusCode)
			continue
		}

		defer resp.Body.Close()
		return ioutil.ReadAll(resp.Body)
	}

	return nil, errNoAvailablePeers
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
		if host != vs.sequins.peers.address {
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
	if vs.sequins.peers == nil {
		return nil
	}

	vs.missingPartitions = vs.numPartitions - len(vs.localPartitions)
	vs.partitionsReady = make(chan bool)

	// Create the partitions path we're going to watch, in case no one has done
	// that yet.
	partitionPath := path.Join("partitions", vs.db, vs.name)
	err := vs.sequins.zkWatcher.createPath(partitionPath)
	if err != nil {
		return err
	}

	// Watch for updates to the partitions path.
	updates := vs.sequins.zkWatcher.watchChildren(partitionPath)
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

		log.Printf("Waiting for all partitions of %s version %s to be available (missing %d)", vs.db, vs.name, vs.missingPartitions)

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
		node := fmt.Sprintf("%05d@%s", partition, vs.sequins.peers.address)
		vs.sequins.zkWatcher.createEphemeral(path.Join("partitions", vs.db, vs.name, node))
	}
}

func (vs *version) close() error {
	return vs.blockStore.Close()
}
