package sharding

import (
	"fmt"
	"path"
	"sort"
	"strconv"
	"strings"
	"sync"

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

	NumPartitions int
	Replication   int

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
		NumPartitions:  numPartitions,
		Replication:    replication,
		minReplication: minReplication,
		selected:       make(map[int]bool),
		local:          make(map[int]bool),
		remote:         make(map[int][]string),
	}

	p.pickLocal()

	if peers != nil {
		updates, _ := zkWatcher.WatchChildren(p.zkPath)
		p.updateRemote(<-updates)
		go p.sync(updates)
	}

	p.updateReplicationStatus()
	return p
}

// pickLocal selects which partitions are local by creating an array of partition IDs
// in order, each repeated by the target replication, and sequentially assigning each
// partition to a node.
func (p *Partitions) pickLocal() {
	selected := make(map[int]bool, p.NumPartitions)

	if p.peers == nil {
		for i := 0; i < p.NumPartitions; i++ {
			selected[i] = true
		}
	} else {
		toAssign := make([]int, 0, p.NumPartitions*p.Replication)
		for i := 0; i < p.NumPartitions; i++ {
			for j := 0; j < p.Replication; j++ {
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
	}

	p.SetSelected(selected)
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

// SetSelected sets the list of selected partitions to the one given.
func (p *Partitions) SetSelected(selected map[int]bool) {
	p.lock.Lock()
	defer p.lock.Unlock()

	p.selected = make(map[int]bool)
	for partition, sel := range selected {
		p.selected[partition] = sel
	}
}

// UpdateSelected udpates the list of selected partitions to include the given
// list.
func (p *Partitions) UpdateSelected(selected map[int]bool) {
	p.lock.Lock()
	defer p.lock.Unlock()

	for partition, sel := range selected {
		p.selected[partition] = sel
	}
}

// UpdateLocal updates the list of local partitions to include the given list.
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

// Selected returns the set of partitions that were selected to have
// locally.
func (p *Partitions) Selected() []int {
	p.lock.Lock()
	defer p.lock.Unlock()

	selected := make([]int, 0, len(p.selected))
	for partition, sel := range p.selected {
		if sel {
			selected = append(selected, partition)
		}
	}
	return selected
}

// HaveSelected returns true if the partitions is selected to be available
// locally.
func (p *Partitions) HaveSelected(partition int) bool {
	p.lock.Lock()
	defer p.lock.Unlock()

	return p.selected[partition]
}

// Local returns the set of partitions that are in the blockstore.
func (p *Partitions) Local() []int {
	p.lock.Lock()
	defer p.lock.Unlock()

	local := make([]int, 0, len(p.local))
	for partition, sel := range p.local {
		if sel {
			local = append(local, partition)
		}
	}
	return local
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
	p.updateReplicationStatus()
}

// GlobalReplication returns a mapping of partitions to their replication
// factor across all nodes in the cluster and the number of missing partitions
func (p *Partitions) GlobalReplication() map[int]int {
	p.lock.Lock()
	defer p.lock.Unlock()

	replication, _ := p.globalReplicationMap()
	return replication
}

func (p *Partitions) globalReplicationMap() (map[int]int, int) {
	replicationMap := make(map[int]int)
	missing := 0

	for i := 0; i < p.NumPartitions; i++ {
		if _, ok := p.local[i]; ok {
			replicationMap[i]++
		}

		if remotes, ok := p.remote[i]; ok {
			replicationMap[i] += len(remotes)
		}

		if replicationMap[i] < p.minReplication {
			missing++
		}
	}

	return replicationMap, missing
}

func (p *Partitions) updateReplicationStatus() {
	// Check for each partition. If every one is available on at least minReplication node,
	// then we're ready to rumble.
	_, missing := p.globalReplicationMap()

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
