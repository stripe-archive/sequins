package main

import (
	"log"
	"path"
	"sync"
	"time"

	"stathat.com/c/consistent"
)

// TODO testable

const peerSelf = "(self)"

// peers represents a remote list of peers, synced with zookeeper. It's also
// responsible for advertising this particular node's existence.
type peers struct {
	address string

	peers map[string]bool
	ring  *consistent.Consistent
	lock  sync.RWMutex

	resetConvergenceTimer chan bool
}

func watchPeers(zkWatcher *zkWatcher, address string) *peers {
	p := &peers{
		address: address,
		peers:   make(map[string]bool),
		ring:    consistent.New(),
		resetConvergenceTimer: make(chan bool),
	}

	zkWatcher.createPath("nodes")
	zkWatcher.createEphemeral(path.Join("nodes", p.address))
	updates, disconnected := zkWatcher.watchChildren("nodes")
	go p.sync(updates, disconnected)

	return p
}

func (p *peers) sync(updates chan []string, disconnected chan bool) {
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

func (p *peers) updatePeers(addrs []string) {
	p.lock.Lock()
	defer p.lock.Unlock()

	// Log any new peers.
	newPeers := make(map[string]bool)
	for _, addr := range addrs {
		if addr == p.address {
			continue
		}

		if !p.peers[addr] {
			log.Println("New peer:", addr)
		}

		newPeers[addr] = true
	}

	// Log for any lost peers.
	for addr := range p.peers {
		if !newPeers[addr] {
			log.Println("Lost peer:", addr)
		}
	}

	p.ring.Set(addrs)
	p.peers = newPeers
}

func (p *peers) pick(partitionId string, n int) []string {
	p.lock.RLock()
	defer p.lock.RUnlock()

	picked, _ := p.ring.GetN(partitionId, n)
	addrs := make([]string, len(picked))
	for i, addr := range picked {
		if addr == p.address {
			addrs[i] = peerSelf
		} else {
			addrs[i] = addr
		}
	}

	return addrs
}

func (p *peers) waitToConverge(dur time.Duration) {
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
