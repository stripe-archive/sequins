package sharding

import (
	"fmt"
	"log"
	"path"
	"strings"
	"sync"
	"time"

	"stathat.com/c/consistent"

	"github.com/stripe/sequins/zk"
)

const peerSelf = "(self)"

// Peers represents a remote list of peers, synced with zookeeper. It's also
// responsible for advertising this particular node's existence.
type Peers struct {
	shardID string
	address string

	peers map[peer]bool
	ring  *consistent.Consistent
	lock  sync.RWMutex

	resetConvergenceTimer chan bool
}

type peer struct {
	shardID string
	address string
}

func WatchPeers(zkWatcher *zk.Watcher, shardID, address string) *Peers {
	p := &Peers{
		shardID: shardID,
		address: address,
		peers:   make(map[peer]bool),
		ring:    consistent.New(),
		resetConvergenceTimer: make(chan bool),
	}

	node := path.Join("nodes", fmt.Sprintf("%s@%s", p.shardID, p.address))
	zkWatcher.CreateEphemeral(node)

	updates, disconnected := zkWatcher.WatchChildren("nodes")
	go p.sync(updates, disconnected)

	return p
}

// sync runs in the background, and syncs the list of peers from zookeeper
// whenever they change.
func (p *Peers) sync(updates chan []string, disconnected chan bool) {
	for {
		var nodes []string
		select {
		case nodes = <-updates:
		case <-disconnected:
		}

		select {
		case p.resetConvergenceTimer <- true:
		default:
		}

		if nodes != nil {
			p.updatePeers(nodes)
		}
	}
}

func (p *Peers) updatePeers(addrs []string) {
	p.lock.Lock()
	defer p.lock.Unlock()

	// Log any new peers.
	newPeers := make(map[peer]bool)
	shards := make(map[string]bool)
	disp := make([]string, 0, len(addrs))
	for _, node := range addrs {
		parts := strings.SplitN(node, "@", 2)
		id := parts[0]
		addr := parts[1]

		if addr == p.address {
			continue
		}

		peer := peer{shardID: id, address: addr}
		disp = append(disp, peer.display())
		if !p.peers[peer] {
			log.Println("New peer:", peer.display())
		}

		shards[id] = true
		newPeers[peer] = true
	}

	// Log for any lost peers.
	for peer := range p.peers {
		if !newPeers[peer] {
			log.Println("Lost peer:", peer.display())
		}
	}

	log.Println("Peers: ", disp)

	shards[p.shardID] = true
	allShards := make([]string, 0, len(shards))
	for shard := range shards {
		allShards = append(allShards, shard)
	}

	p.ring.Set(allShards)
	p.peers = newPeers
}

// WaitToConverge blocks until the list of peers has stabilized for dur.
func (p *Peers) WaitToConverge(dur time.Duration) {
	log.Printf("Waiting for list of peers to stabilize for %v...", dur)
	timer := time.NewTimer(dur)

	for {
		timer.Reset(dur)
		select {
		case <-p.resetConvergenceTimer:
		case <-timer.C:
			return
		}
	}
}

// Get returns the current list of peers.
func (p *Peers) Get() []string {
	p.lock.RLock()
	defer p.lock.RUnlock()

	addrs := make([]string, 0, len(p.peers))
	for peer := range p.peers {
		addrs = append(addrs, peer.address)
	}

	return addrs
}

// pick returns the list of peers who have a given partition. It returns at most
// n entries.
func (p *Peers) pick(partitionId string, n int) []string {
	p.lock.RLock()
	defer p.lock.RUnlock()

	picked, _ := p.ring.GetN(partitionId, n)
	shards := make(map[string]bool)
	for _, shard := range picked {
		shards[shard] = true
	}

	addrs := make([]string, 0, len(shards))
	for peer := range p.peers {
		if shards[peer.shardID] {
			addrs = append(addrs, peer.address)
		}
	}

	if shards[p.shardID] {
		addrs = append(addrs, peerSelf)
	}

	return addrs
}

func (p *peer) display() string {
	if p.shardID == p.address {
		return p.address
	} else {
		return fmt.Sprintf("%s (%s)", p.address, p.shardID)
	}
}
