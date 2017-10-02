package sharding

import (
	"fmt"
	"log"
	"path"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"stathat.com/c/consistent"

	"github.com/stripe/sequins/zk"
)

const peerSelf = "(self)"

// Peers represents a remote list of Peers, synced with zookeeper. It's also
// responsible for advertising this particular node's existence.
type Peers struct {
	ShardID string
	Address string

	Peers map[peer]bool
	ring  *consistent.Consistent
	Lock  sync.RWMutex

	resetConvergenceTimer chan bool
}

type peer struct {
	ShardID string
	Address string
}

func (p *Peers) SmallestAvailableShardID() (string, error) {
	peerList := make([]int, len(p.Peers), len(p.Peers))
	i := 0
	for peer := range p.Peers {
		num, err := strconv.Atoi(peer.ShardID)
		if err != nil {
			return "", fmt.Errorf("Can't convert shardID %q to int", peer.ShardID)
		}

		peerList[i] = num
		i++
	}

	sort.Ints(peerList)

	prevNum := 0
	for _, num := range peerList {
		if num != prevNum+1 {
			return fmt.Sprint(prevNum + 1), nil
		}

		prevNum = num
	}

	return fmt.Sprint(prevNum + 1), nil
}

func WatchPeersNoJoin(zkWatcher *zk.Watcher) *Peers {
	p := &Peers{
		Peers:                 make(map[peer]bool),
		ring:                  consistent.New(),
		resetConvergenceTimer: make(chan bool),
	}

	updates, disconnected := zkWatcher.WatchChildren("nodes")
	go p.sync(updates, disconnected)

	return p
}

func WatchPeers(zkWatcher *zk.Watcher, shardID, address string) *Peers {
	p := &Peers{
		ShardID:               shardID,
		Address:               address,
		Peers:                 make(map[peer]bool),
		ring:                  consistent.New(),
		resetConvergenceTimer: make(chan bool),
	}

	node := path.Join("nodes", fmt.Sprintf("%s@%s", p.ShardID, p.Address))
	zkWatcher.CreateEphemeral(node)

	updates, disconnected := zkWatcher.WatchChildren("nodes")
	go p.sync(updates, disconnected)

	return p
}

// sync runs in the background, and syncs the list of Peers from zookeeper
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
	p.Lock.Lock()
	defer p.Lock.Unlock()

	// Log any new Peers.
	newPeers := make(map[peer]bool)
	shards := make(map[string]bool)
	disp := make([]string, 0, len(addrs))
	for _, node := range addrs {
		parts := strings.SplitN(node, "@", 2)
		id := parts[0]
		addr := parts[1]

		if addr == p.Address {
			continue
		}

		peer := peer{ShardID: id, Address: addr}
		disp = append(disp, peer.display())
		if !p.Peers[peer] {
			log.Println("New peer:", peer.display())
		}

		shards[id] = true
		newPeers[peer] = true
	}

	// Log for any lost Peers.
	for peer := range p.Peers {
		if !newPeers[peer] {
			log.Println("Lost peer:", peer.display())
		}
	}

	log.Println("Peers: ", disp)

	shards[p.ShardID] = true
	allShards := make([]string, 0, len(shards))
	for shard := range shards {
		allShards = append(allShards, shard)
	}

	p.ring.Set(allShards)
	p.Peers = newPeers
}

// WaitToConverge blocks until the list of Peers has stabilized for dur.
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

// Get returns the current list of Peers.
func (p *Peers) Get() []string {
	p.Lock.RLock()
	defer p.Lock.RUnlock()

	addrs := make([]string, 0, len(p.Peers))
	for peer := range p.Peers {
		addrs = append(addrs, peer.Address)
	}

	return addrs
}

// pick returns the list of Peers who have a given partition. It returns at most
// n entries.
func (p *Peers) pick(partitionId string, n int) []string {
	p.Lock.RLock()
	defer p.Lock.RUnlock()

	picked, _ := p.ring.GetN(partitionId, n)
	shards := make(map[string]bool)
	for _, shard := range picked {
		shards[shard] = true
	}

	addrs := make([]string, 0, len(shards))
	for peer := range p.Peers {
		if shards[peer.ShardID] {
			addrs = append(addrs, peer.Address)
		}
	}

	if shards[p.ShardID] {
		addrs = append(addrs, peerSelf)
	}

	return addrs
}

func (p *peer) display() string {
	if p.ShardID == p.Address {
		return p.Address
	} else {
		return fmt.Sprintf("%s (%s)", p.Address, p.ShardID)
	}
}
