package main

import (
	"log"
	"path"
	"sync"
	"time"

	"stathat.com/c/consistent"
)

const peerSelf = "(self)"

// peers represents a remote list of peers, synced with zookeeper. It's also
// responsible for advertising this particular node's existence.
type peers struct {
	address    string
	advertised string

	peers map[string]bool
	ring  *consistent.Consistent
	lock  sync.RWMutex

	convergenceTimer *time.Timer
	convergenceTime  time.Duration
}

func watchPeers(zkWatcher *zkWatcher, address string) *peers {
	p := &peers{
		address: address,
		peers:   make(map[string]bool),
		ring:    consistent.New(),
	}

	zkWatcher.createEphemeral(path.Join("nodes", p.advertised))
	updates := zkWatcher.watchChildren("nodes")
	go p.sync(updates)

	return p
}

func (p *peers) sync(updates chan []string) {
	for {
		nodes := <-updates
		timer := p.convergenceTimer

		newPeers := make(map[string]bool)
		for _, node := range nodes {
			newPeers[node] = true
		}

		if timer != nil {
			timer.Reset(p.convergenceTime)
		}

		p.updatePeers(newPeers)
	}
}

func (p *peers) updatePeers(newPeers map[string]bool) {
	p.lock.Lock()
	defer p.lock.Unlock()

	// Log for any lost peers.
	for addr := range p.peers {
		if !p.peers[addr] {
			log.Println("Lost peer:", addr)
		}
	}

	// Log for any new peers, and build a list for the ring.
	addrs := make([]string, len(newPeers)+1)
	for addr := range newPeers {
		if !p.peers[addr] {
			log.Println("New peer:", addr)
		}

		addrs = append(addrs, addr)
	}

	addrs = append(addrs, p.address)
	p.ring.Set(addrs)
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
	if p.convergenceTimer != nil {
		return
	}

	log.Println("Waiting", dur, "for list of peers to converge...")
	p.convergenceTime = dur
	p.convergenceTimer = time.NewTimer(dur)
	<-p.convergenceTimer.C
}
