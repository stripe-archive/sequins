package main

import (
	"fmt"
	"log"
	"path"
	"strconv"
	"strings"
	"sync"
	"time"
)

// TODO testable

// partitions represents a list of partitions for a single version and their
// mapping to nodes, synced from zookeeper. It's also responsible for
// advertising the partitions we have locally.
type partitions struct {
	peers     *peers
	zkWatcher *zkWatcher

	db      string
	version string
	zkPath  string

	numPartitions int
	replication   int

	missing int
	local   map[int]bool
	remote  map[int][]string

	lock        sync.RWMutex
	noneMissing chan bool
}

func watchPartitions(zkWatcher *zkWatcher, peers *peers, db, version string, numPartitions, replication int) *partitions {
	p := &partitions{
		peers:         peers,
		zkWatcher:     zkWatcher,
		db:            db,
		version:       version,
		zkPath:        path.Join("partitions", db, version),
		numPartitions: numPartitions,
		replication:   replication,
		local:         make(map[int]bool),
		remote:        make(map[int][]string),
		noneMissing:   make(chan bool),
	}

	// Create the partitions path we're going to watch, in case no one has done
	// that yet.
	p.zkPath = path.Join("partitions", db, version)
	zkWatcher.createPath(p.zkPath)

	updates, _ := zkWatcher.watchChildren(p.zkPath)
	p.updateRemotePartitions(<-updates)
	go p.sync(updates)
	return p
}

// pickLocalPartitions selects which partitions are local by iterating through
// them all, and checking the hashring to see if this peer is one of the
// replicas.
func (p *partitions) pickLocalPartitions() map[int]bool {
	partitions := make(map[int]bool)
	disp := make([]int, 0)

	for i := 0; i < p.numPartitions; i++ {
		partitionId := p.partitionId(i)

		replicas := p.peers.pick(partitionId, p.replication)
		for _, replica := range replicas {
			if replica == peerSelf {
				partitions[i] = true
				disp = append(disp, i)
			}
		}
	}

	return partitions
}

// sync syncs the remote partitions from zoolander whenever they change.
func (p *partitions) sync(updates chan []string) {
	for {
		nodes, ok := <-updates
		if !ok {
			close(p.noneMissing)
			break
		}

		p.updateRemotePartitions(nodes)
	}
}

func (p *partitions) updateLocalPartitions(local map[int]bool) {
	p.lock.Lock()
	defer p.lock.Unlock()

	p.local = local
	p.updateMissing()
}

func (p *partitions) updateRemotePartitions(nodes []string) {
	p.lock.Lock()
	defer p.lock.Unlock()

	remote := make(map[int][]string)
	for _, node := range nodes {
		parts := strings.SplitN(node, "@", 2)
		partition, _ := strconv.Atoi(parts[0])
		host := parts[1]
		if host != p.peers.address {
			remote[partition] = append(remote[partition], host)
		}
	}

	p.remote = remote
	p.updateMissing()
}

func (p *partitions) updateMissing() {
	// Check for each partition. If every one is available on at least one node,
	// then we're ready to rumble.
	missing := 0
	for i := 0; i < p.numPartitions; i++ {
		if _, ok := p.local[i]; ok {
			continue
		}

		if _, ok := p.remote[i]; ok {
			continue
		}

		missing += 1
	}

	p.missing = missing
	if missing == 0 {
		select {
		case p.noneMissing <- true:
		default:
		}
	}
}

func (p *partitions) ready() bool {
	p.lock.RLock()
	defer p.lock.RUnlock()

	return p.missing == 0
}

// advertiseAndWait advertises the partitions we have locally, and waits until
// it sees at least one peer for every remote partition. It returns false only
// if it was closed before that happens.
func (p *partitions) advertiseAndWait() bool {
	// Advertise that our local partitions are ready.
	p.advertisePartitions()

	for {
		p.lock.RLock()
		missing := p.missing
		p.lock.RUnlock()
		if missing == 0 {
			break
		}

		log.Printf("Waiting for all partitions of %s version %s to be available (missing %d)",
			p.db, p.version, missing)

		t := time.NewTimer(10 * time.Second)
		select {
		case <-t.C:
		case success := <-p.noneMissing:
			// If success is false, it's because the close() was called before we
			// finished waiting on peers.
			return success
		}
	}

	return true
}

// advertisePartitions creates an ephemeral node for each partition this local
// peer is responsible for.
// TODO: this should maybe be a zk multi op?
func (p *partitions) advertisePartitions() {
	for partition := range p.local {
		p.zkWatcher.createEphemeral(p.partitionZKNode(partition))
	}
}

func (p *partitions) unadvertisePartitions() {
	for partition := range p.local {
		p.zkWatcher.removeEphemeral(p.partitionZKNode(partition))
	}
}

func (p *partitions) partitionZKNode(partition int) string {
	return path.Join(p.zkPath, fmt.Sprintf("%05d@%s", partition, p.peers.address))
}

// getPeers returns the list of peers who have the given partition available.
func (p *partitions) getPeers(partition int) []string {
	p.lock.RLock()
	defer p.lock.RUnlock()

	peers := make([]string, len(p.remote[partition]))
	copy(peers, p.remote[partition])
	return peers
}

// partitionId returns a string id for the given partition, to be used for the
// consistent hashing ring. It's not really meant to be unique, but it should be
// different for different versions with the same number of partitions, so that
// they don't shard identically.
func (p *partitions) partitionId(partition int) string {
	return fmt.Sprintf("%s:%05d", p.zkPath, partition)
}

func (p *partitions) close() {
	p.lock.Lock()
	p.lock.Unlock()

	p.zkWatcher.removeWatch(p.zkPath)
	p.unadvertisePartitions()
}
