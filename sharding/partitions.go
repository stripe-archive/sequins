package sharding

import (
	"fmt"
	"path"
	"sort"
	"strconv"
	"strings"
	"sync"

	"log"

	"github.com/stripe/sequins/zk"
)

// Partitions represents a list of partitions for a single version and their
// mapping to nodes, synced from zookeeper. It's also the source of truth for
// which partitions this node has locally, as well as which partitions it should
// have. Finally, it's responsible for advertising those partitions we do have
// locally to other peers.
type Partitions struct {
	// Ready is closed once all partitions are available, either locally or on
	// peers.
	Ready chan bool

	peers     *Peers
	zkWatcher *zk.Watcher

	db      string
	version string
	zkPath  string

	numPartitions int
	replication   int

	// The minimum replication to stop counting a partition as "missing".
	minReplication int

	selected        map[int]bool
	local           map[int]bool
	remote          map[int][]string
	readyClosed     bool
	shouldAdvertise bool

	lock sync.RWMutex
}

// WatchPartitions creates a watch on the partitions prefix in zookeeper, and
// returns a Partitions object for managing local and remote partitions.
func WatchPartitions(zkWatcher *zk.Watcher, peers *Peers, db, version string, numPartitions, replication, minReplication int) *Partitions {
	p := &Partitions{
		Ready: make(chan bool),

		peers:          peers,
		zkWatcher:      zkWatcher,
		db:             db,
		version:        version,
		zkPath:         path.Join("partitions", db, version),
		numPartitions:  numPartitions,
		replication:    replication,
		minReplication: minReplication,
		local:          make(map[int]bool),
		remote:         make(map[int][]string),
	}

	p.pickLocal()

	if peers != nil {
		updates, _ := zkWatcher.WatchChildren(p.zkPath)
		p.updateRemote(<-updates)
		go p.sync(updates)
	} else {
		log.Printf("peers is nil?! for %s of %s", version, db)
	}

	p.updateReplicationStatus()
	return p
}

// pickLocal selects which partitions are local by creating an array of partition IDs
// in order, each repeated by the target replication, and sequentially assigning each
// partition to a node.
func (p *Partitions) pickLocal() {
	selected := make(map[int]bool, p.numPartitions)

	if p.peers == nil {
		for i := 0; i < p.numPartitions; i++ {
			selected[i] = true
		}
	} else {
		toAssign := make([]int, 0, p.numPartitions*p.replication)
		for i := 0; i < p.numPartitions; i++ {
			for j := 0; j < p.replication; j++ {
				toAssign = append(toAssign, i)
			}
		}

		// only keep unique shardIDs so that nodes with the same
		// shardID will get the same partition assignments
		ids := make(map[string]bool)
		for _, id := range append(p.peers.GetShardIds(), p.peers.ShardID) {
			ids[id] = true
		}

		uniqueIds := make([]string, 0, len(ids))
		for id := range ids {
			uniqueIds = append(uniqueIds, id)
		}
		sort.Strings(uniqueIds)

		for i, id := range toAssign {
			assignee := i % len(uniqueIds)
			if uniqueIds[assignee] == p.peers.ShardID {
				selected[id] = true
			}
		}

		mine := -1
		for i, u := range uniqueIds {
			if u == p.peers.ShardID {
				mine = i
			}
		}
		log.Printf("pickLocal params: %s/%s shard %s => %d parts, shard %d/%d\n",
			p.db, p.version, p.peers.ShardID, p.numPartitions, mine, len(uniqueIds))

		picked := []int{}
		for s, _ := range selected {
			picked = append(picked, s)
		}
		sort.Ints(picked)
		log.Printf("pickLocal result: %s/%s shard %s => %v\n", p.db, p.version, p.peers.ShardID, picked)
	}
	p.selected = selected
}

// sync runs in the background, and syncs the remote partitions from zookeeper
// whenever they change.
func (p *Partitions) sync(updates chan []string) {
	for {
		nodes, ok := <-updates
		if !ok {
			break
		}

		p.updateRemote(nodes)
	}
}

// FindPeers returns the list of peers who have the given partition.
func (p *Partitions) FindPeers(partition int) []string {
	if p.peers == nil {
		return nil
	}

	p.lock.RLock()
	defer p.lock.RUnlock()

	peers := make([]string, len(p.remote[partition]))
	copy(peers, p.remote[partition])
	return peers
}

// Update updates the list of local partitions to the given list.
func (p *Partitions) UpdateLocal(local map[int]bool) {
	p.lock.Lock()
	defer p.lock.Unlock()

	for partition := range local {
		p.local[partition] = true
	}
	p.updateReplicationStatus()

	if p.shouldAdvertise {
		for partition := range p.local {
			p.zkWatcher.CreateEphemeral(p.partitionZKNode(partition))
		}
	}
}

// SelectedLocal returns the set of partitions that were selected to have
// locally.
func (p *Partitions) SelectedLocal() map[int]bool {
	return p.selected
}

// NeededLocal returns the set of partitions that were selected to have locally,
// but aren't local yet.
func (p *Partitions) NeededLocal() map[int]bool {
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

// HaveLocal returns true if the partition is already available locally.
func (p *Partitions) HaveLocal(partition int) bool {
	p.lock.Lock()
	defer p.lock.Unlock()

	return p.local[partition]
}

// Advertise creates an ephemeral node for each partition this local peer is
// responsible for. The Partitions object will then continue to do so whenever
// UpdateLocal is called, until Unadvertise is called to disable this behavior.
func (p *Partitions) Advertise() {
	if p.peers == nil {
		return
	}

	p.lock.Lock()
	defer p.lock.Unlock()

	p.shouldAdvertise = true
	for partition := range p.local {
		p.zkWatcher.CreateEphemeral(p.partitionZKNode(partition))
	}
}

// Unadvertise removes the ephemeral nodes created by Advertise, and informs the
// Partitions object to no longer automatically create them.
func (p *Partitions) Unadvertise() {
	if p.peers == nil {
		return
	}

	p.lock.Lock()
	defer p.lock.Unlock()

	p.shouldAdvertise = false
	for partition := range p.local {
		p.zkWatcher.RemoveEphemeral(p.partitionZKNode(partition))
	}
}

func (p *Partitions) updateRemote(nodes []string) {
	if p.peers == nil {
		log.Printf("p.peers is nil?! for %s of %s", p.version, p.db)
		return
	}

	p.lock.Lock()
	defer p.lock.Unlock()

	log.Printf("Got nodes for %s of %s: %+v", p.version, p.db, nodes)
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
	p.updateReplicationStatus()
}

func (p *Partitions) updateReplicationStatus() {
	// Check for each partition. If every one is available on at least minReplication node,
	// then we're ready to rumble.
	missing := 0
	for i := 0; i < p.numPartitions; i++ {
		replication := 0
		if _, ok := p.local[i]; ok {
			replication++
		}

		if remotes, ok := p.remote[i]; ok {
			replication += len(remotes)
		}

		if replication < p.minReplication {
			missing += 1
		}
	}

	if missing == 0 && !p.readyClosed {
		close(p.Ready)
		p.readyClosed = true
	}
}

// partitionZKNode returns the node to write out to advertize that we have the
// given partition.
func (p *Partitions) partitionZKNode(partition int) string {
	return path.Join(p.zkPath, fmt.Sprintf("%05d@%s", partition, p.peers.address))
}

// partitionId returns a string id for the given partition, to be used for the
// consistent hashing ring. It's not really meant to be unique, but it should be
// different for different versions with the same number of partitions, so that
// they don't shard identically.
func (p *Partitions) partitionId(partition int) string {
	return fmt.Sprintf("%s:%05d", p.zkPath, partition)
}

func (p *Partitions) Close() {
	if p.peers != nil {
		p.Unadvertise()

		p.lock.Lock()
		defer p.lock.Unlock()

		p.zkWatcher.RemoveWatch(p.zkPath)
	}
}
