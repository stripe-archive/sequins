package main

import (
	"fmt"
	"path"
	"strconv"
	"strings"
	"sync"
)

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

	selected        map[int]bool
	local           map[int]bool
	remote          map[int][]string
	numMissing      int
	ready           chan bool
	readyClosed     bool
	shouldAdvertise bool

	lock sync.RWMutex
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
		ready:         make(chan bool),
	}

	p.pickLocalPartitions()

	if peers != nil {
		updates, _ := zkWatcher.watchChildren(p.zkPath)
		p.updateRemotePartitions(<-updates)
		go p.sync(updates)
	}

	p.updateMissing()
	return p
}

// pickLocalPartitions selects which partitions are local by iterating through
// them all, and checking the hashring to see if this peer is one of the
// replicas.
func (p *partitions) pickLocalPartitions() {
	selected := make(map[int]bool)

	for i := 0; i < p.numPartitions; i++ {
		if p.peers != nil {
			partitionId := p.partitionId(i)

			replicas := p.peers.pick(partitionId, p.replication)
			for _, replica := range replicas {
				if replica == peerSelf {
					selected[i] = true
				}
			}
		} else {
			selected[i] = true
		}
	}

	p.selected = selected
}

// sync syncs the remote partitions from zoolander whenever they change.
func (p *partitions) sync(updates chan []string) {
	for {
		nodes, ok := <-updates
		if !ok {
			break
		}

		p.updateRemotePartitions(nodes)
	}
}

func (p *partitions) updateLocalPartitions(local map[int]bool) {
	p.lock.Lock()
	defer p.lock.Unlock()

	for partition := range local {
		p.local[partition] = true
	}
	p.updateMissing()

	if p.shouldAdvertise {
		for partition := range p.local {
			p.zkWatcher.createEphemeral(p.partitionZKNode(partition))
		}
	}
}

func (p *partitions) updateRemotePartitions(nodes []string) {
	if p.peers == nil {
		return
	}

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

	p.numMissing = missing
	if missing == 0 && !p.readyClosed {
		close(p.ready)
		p.readyClosed = true
	}
}

func (p *partitions) missing() int {
	p.lock.Lock()
	defer p.lock.Unlock()

	return p.numMissing
}

func (p *partitions) needed() map[int]bool {
	p.lock.Lock()
	defer p.lock.Unlock()

	needed := make(map[int]bool)
	for partition := range p.selected {
		if !p.local[partition] {
			needed[partition] = true
		}
	}

	return needed
}

func (p *partitions) have(partition int) bool {
	p.lock.Lock()
	defer p.lock.Unlock()

	return p.local[partition]
}

// advertisePartitions creates an ephemeral node for each partition this local
// peer is responsible for. It will continue to do so whenever
// updateLocalPartitions is called, until unadvertisePartitions is called to
// disable this behavior.
func (p *partitions) advertisePartitions() {
	if p.peers == nil {
		return
	}

	p.lock.Lock()
	defer p.lock.Unlock()

	p.shouldAdvertise = true
	for partition := range p.local {
		p.zkWatcher.createEphemeral(p.partitionZKNode(partition))
	}
}

func (p *partitions) unadvertisePartitions() {
	if p.peers == nil {
		return
	}

	p.lock.Lock()
	defer p.lock.Unlock()

	p.shouldAdvertise = false
	for partition := range p.local {
		p.zkWatcher.removeEphemeral(p.partitionZKNode(partition))
	}
}

func (p *partitions) partitionZKNode(partition int) string {
	return path.Join(p.zkPath, fmt.Sprintf("%05d@%s", partition, p.peers.address))
}

// getPeers returns the list of peers who have the given partition available.
func (p *partitions) getPeers(partition int) []string {
	if p.peers == nil {
		return nil
	}

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
	defer p.lock.Unlock()

	if p.peers != nil {
		p.zkWatcher.removeWatch(p.zkPath)
		p.unadvertisePartitions()
	}
}
