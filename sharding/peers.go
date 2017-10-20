package sharding

import (
	"log"
	"path"
	"sync"
	"time"

	"github.com/stripe/sequins/zk"
)

const peerSelf = "(self)"

// Peers represents a remote list of peers, synced with zookeeper. It's also
// responsible for advertising this particular node's existence.
type Peers struct {
	address string

	peers map[string]bool
	lock  sync.RWMutex

	resetConvergenceTimer chan bool
}

func WatchPeers(zkWatcher *zk.Watcher, address string) *Peers {
	p := &Peers{
		address: address,
		peers:   make(map[string]bool),
		resetConvergenceTimer: make(chan bool),
	}

	zkWatcher.CreateEphemeral(path.Join("nodes", address))

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
	newPeers := make(map[string]bool)
	disp := make([]string, 0, len(addrs))
	for _, addr := range addrs {
		if addr == p.address {
			continue
		}

		disp = append(disp, addr)
		if !p.peers[addr] {
			log.Println("New peer:", addr)
		}

		newPeers[addr] = true
	}

	// Log for any lost peers.
	for peer := range p.peers {
		if !newPeers[peer] {
			log.Println("Lost peer:", peer)
		}
	}

	log.Println("Peers: ", disp)

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

	peers := make([]string, 0, len(p.peers))
	for peer := range p.peers {
		peers = append(peers, peer)
	}

	return peers
}
