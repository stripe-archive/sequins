package sharding

import (
	"fmt"
	"path"
	"strconv"
	"strings"
	"sync"

	"log"

	pb "github.com/stripe/sequins/rpc"
	"github.com/stripe/sequins/zk"
	"google.golang.org/grpc"
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

	selected        map[int]bool
	local           map[int]bool
	remote          map[int][]string
	numMissing      int
	readyClosed     bool
	shouldAdvertise bool

	lock sync.RWMutex
}

// WatchPartitions creates a watch on the partitions prefix in zookeeper, and
// returns a Partitions object for managing local and remote partitions.
func WatchPartitions(zkWatcher *zk.Watcher, peers *Peers, db, version string, numPartitions, replication int) *Partitions {
	p := &Partitions{
		Ready: make(chan bool),

		peers:         peers,
		zkWatcher:     zkWatcher,
		db:            db,
		version:       version,
		zkPath:        path.Join("partitions", db, version),
		numPartitions: numPartitions,
		replication:   replication,
		local:         make(map[int]bool),
		remote:        make(map[int][]string),
	}

	p.pickLocal()

	if peers != nil {
		updates, _ := zkWatcher.WatchChildren(p.zkPath)
		p.updateRemote(<-updates)
		go p.sync(updates)
	}

	p.updateMissing()
	return p
}

// pickLocal selects which partitions are local by iterating through
// them all, and checking the hashring to see if this peer is one of the
// replicas.
func (p *Partitions) pickLocal() {
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

// FindPeers returns the list of peers who have the given partition available.
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

// Find GRPC peers
func (p *Partitions) FindGRPCPeers(partition int) []pb.SequinsRpcClient {
	if p.peers == nil {
		return nil
	}
	p.lock.RLock()
	defer p.lock.RUnlock()

	peers := make([]pb.SequinsRpcClient, len(p.remote[partition]))
	for i, peer := range p.remote[partition] {
		splitPeer := strings.SplitN(peer, ":", 2)
		port, err := strconv.Atoi(splitPeer[1])
		if err != nil {
			log.Fatalf("Failed to find GRPC port: %v", err)
		}
		port += 1
		conn, err := grpc.Dial(fmt.Sprintf("%s:%d", splitPeer[0], port), nil)
		if err != nil {
			log.Fatalf("fail to dial: %v", err)
		}
		defer conn.Close()
		client := pb.NewSequinsRpcClient(conn)
		peers[i] = client
	}
	return peers
}

// Update updates the list of local partitions to the given list.
func (p *Partitions) UpdateLocal(local map[int]bool) {
	p.lock.Lock()
	defer p.lock.Unlock()

	for partition := range local {
		p.local[partition] = true
	}
	p.updateMissing()

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

// Missing returns the number of missing partitions from the complete set; in
// other words, the number of partitions which do not exist either locally or
// with peers.
func (p *Partitions) Missing() int {
	p.lock.Lock()
	defer p.lock.Unlock()

	return p.numMissing
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
	p.updateMissing()
}

func (p *Partitions) updateMissing() {
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
