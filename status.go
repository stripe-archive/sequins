package main

import (
	"encoding/json"
	"fmt"
	"html/template"
	"log"
	"net/http"
	"sort"
	"strings"
	"time"
)

var statusTemplate *template.Template

var templateFns = template.FuncMap{
	"marshal": func(v status) template.JS {
		a, _ := json.Marshal(v)
		return template.JS(a)
	},
	"castToFloat32": func(v int) float32 {
		return float32(v)
	},
	"isListView": func(s status) bool {
		return len(s.DBs) > 1
	},
	"replicationKeys": func(v versionStatus) string {
		maxKey := 0
		for k := range v.ReplicationHistogram {
			if k > maxKey {
				maxKey = k
			}
		}

		keys := make([]string, maxKey+1, maxKey+1)
		for i := 0; i <= maxKey; i++ {
			keys[i] = fmt.Sprintf("%dx", maxKey-i)
		}

		return strings.Join(keys, "/")
	},
	"replicationValues": func(v versionStatus) string {
		maxKey := 0
		for k := range v.ReplicationHistogram {
			if k > maxKey {
				maxKey = k
			}
		}

		values := make([]string, maxKey+1, maxKey+1)
		for i := 0; i <= maxKey; i++ {
			value := v.ReplicationHistogram[maxKey-i]
			values[i] = fmt.Sprintf("%v", value)
		}

		return strings.Join(values, "/")
	},
}

func init() {
	// This is chunked into the binary with go-bindata. See the Makefile for more
	// information.
	raw := string(MustAsset("status.tmpl"))
	statusTemplate = template.Must(template.New("status").Funcs(templateFns).Parse(raw))
}

type status struct {
	DBs     map[string]dbStatus `json:"dbs"`
	ShardID string              `json:"shard_id"`
}

type dbStatus struct {
	Versions map[string]versionStatus `json:"versions"`
}

type versionStatus struct {
	Path              string `json:"path"`
	NumPartitions     int    `json:"num_partitions"`
	TargetReplication int    `json:"target_replication"`

	// Values that are recalculated with calculateReplicationStats
	ReplicationHistogram map[int]int `json:"replication_histogram"`
	AverageReplication   float32     `json:"average_replication"`

	// For backwards compatibility
	MissingPartitions         int `json:"missing_partitions"`
	UnderreplicatedPartitions int `json:"underreplicated_partitions"`
	OverreplicatedPartitions  int `json:"overreplicated_partitions"`

	Nodes map[string]nodeVersionStatus `json:"nodes"`
}

type nodeVersionStatus struct {
	Name       string       `json:"name"`
	CreatedAt  time.Time    `json:"created_at"`
	ActiveAt   time.Time    `json:"active_at,omitempty"`
	Current    bool         `json:"current"`
	State      versionState `json:"state"`
	Partitions []int        `json:"partitions"`
	ShardID    string       `json:"shard_id"`
}

type versionState string

const (
	versionActive   versionState = "ACTIVE"
	versionRemoving              = "REMOVING"
	versionBuilding              = "BUILDING"
	versionError                 = "ERROR"
)

func (s *sequins) serveHealth(w http.ResponseWriter, r *http.Request) {
	s.dbsLock.RLock()

	status := status{DBs: make(map[string]dbStatus)}
	for name, db := range s.dbs {
		status.DBs[name] = copyDBStatus(db.status())
	}

	s.dbsLock.RUnlock()

	if s.config.Sharding.Enabled {
		w.Header().Set("X-Sequins-Shard-ID", s.peers.ShardID)
	}

	hostname := "localhost"
	if s.peers != nil {
		hostname = s.address
	}

	// Create a mapping of db -> version -> versionStatus for this node only
	statuses := make(map[string]map[string]nodeVersionStatus)
	for dbName, db := range status.DBs {
		for versionName, version := range db.Versions {
			if _, ok := statuses[dbName]; !ok {
				statuses[dbName] = make(map[string]nodeVersionStatus)
			}
			statuses[dbName][versionName] = version.Nodes[hostname]
		}
	}

	// We return a 200 when any database has an ACTIVE or BUILDING version
	versionsAvailable := false
	for _, db := range statuses {
		for _, version := range db {
			if version.State == versionActive || version.State == versionBuilding {
				versionsAvailable = true
				break
			}
		}

		if versionsAvailable {
			break
		}
	}

	jsonBytes, err := json.Marshal(statuses)
	if err != nil {
		log.Printf("Error encoding response to JSON: %v", jsonBytes)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	if versionsAvailable {
		w.WriteHeader(http.StatusOK)
	} else {
		w.WriteHeader(http.StatusNotFound)
	}
	w.Write(jsonBytes)
}

func (s *sequins) serveStatus(w http.ResponseWriter, r *http.Request) {
	s.dbsLock.RLock()

	status := status{DBs: make(map[string]dbStatus)}
	for name, db := range s.dbs {
		status.DBs[name] = copyDBStatus(db.status())
	}

	s.dbsLock.RUnlock()

	// By default, serve our peers' statuses merged with ours. We take
	// extra care not to mutate local status structs.
	if r.URL.Query().Get("proxy") == "" && s.peers != nil {
		for _, p := range s.peers.GetAddresses() {
			peerStatus, err := s.getPeerStatus(p, "")
			if err != nil {
				log.Printf("Error fetching status from peer %s: %s", p, err)
				continue
			}

			merged := peerStatus
			for db := range status.DBs {
				if _, ok := merged.DBs[db]; ok {
					merged.DBs[db] = mergeDBStatus(merged.DBs[db], status.DBs[db])
				} else {
					merged.DBs[db] = status.DBs[db]
				}
			}

			status = merged
		}

		for _, db := range status.DBs {
			for versionName := range db.Versions {
				vst := db.Versions[versionName]
				vst.calculateReplicationStats()
				db.Versions[versionName] = vst
			}
		}
	}

	if s.config.Sharding.Enabled {
		status.ShardID = s.peers.ShardID
	}

	if acceptsJSON(r) {
		jsonBytes, err := json.Marshal(status)
		if err != nil {
			log.Println("Error serving status:", err)
			w.WriteHeader(http.StatusInternalServerError)
			return
		}

		w.Header()["Content-Type"] = []string{"application/json"}
		w.Write(jsonBytes)
	} else {
		err := statusTemplate.Execute(w, status)
		if err != nil {
			log.Println("Error rendering status:", err)
		}
	}
}

func (db *db) serveStatus(w http.ResponseWriter, r *http.Request) {
	s := db.status()

	// By default, serve our peers' statuses merged with ours.
	if r.URL.Query().Get("proxy") == "" && db.sequins.peers != nil {
		for _, p := range db.sequins.peers.GetAddresses() {
			peerStatus, err := db.sequins.getPeerStatus(p, db.name)
			if err != nil {
				log.Printf("Error fetching status from peer %s: %s", p, err)
				continue
			}

			peerDBStatus := peerStatus.DBs[db.name]
			s = mergeDBStatus(peerDBStatus, s)
		}

		for versionName := range s.Versions {
			vst := s.Versions[versionName]
			vst.calculateReplicationStats()
			s.Versions[versionName] = vst
		}
	}

	if acceptsJSON(r) {
		jsonBytes, err := json.Marshal(s)
		if err != nil {
			log.Println("Error serving status:", err)
			w.WriteHeader(http.StatusInternalServerError)
			return
		}

		w.Header()["Content-Type"] = []string{"application/json"}
		w.Write(jsonBytes)
	} else {
		status := status{DBs: make(map[string]dbStatus)}
		status.DBs[db.name] = s
		err := statusTemplate.Execute(w, status)
		if err != nil {
			log.Println("Error rendering status:", err)
		}
	}
}

// getPeerStatus fetches a peer's status for the given db. If db is empty, it
// returns the status for all dbs.
func (s *sequins) getPeerStatus(peer string, db string) (status, error) {
	url := fmt.Sprintf("http://%s/%s?proxy=status", peer, db)
	if !strings.HasSuffix(url, "/") {
		url += "/"
	}

	status := status{}
	req, err := http.NewRequest("GET", url, nil)
	req.Header.Set("Accept", "application/json")
	if err != nil {
		return status, err
	}

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return status, err
	}

	defer resp.Body.Close()
	decoder := json.NewDecoder(resp.Body)
	if db == "" {
		err = decoder.Decode(&status)
	} else {
		s := dbStatus{Versions: make(map[string]versionStatus)}
		err = decoder.Decode(&s)
		if err != nil {
			return status, err
		}

		status.DBs = map[string]dbStatus{db: s}
	}

	return status, err
}

// mergeDBStatus merges two dbStatus objects, mutating only the
// left one.
func mergeDBStatus(left, right dbStatus) dbStatus {
	for v, vst := range right.Versions {
		if _, ok := left.Versions[v]; !ok {
			left.Versions[v] = versionStatus{
				Nodes:                make(map[string]nodeVersionStatus),
				ReplicationHistogram: vst.ReplicationHistogram,
				Path:                 vst.Path,
				NumPartitions:        vst.NumPartitions,
				TargetReplication:    vst.TargetReplication,
			}
		}

		for hostname, node := range right.Versions[v].Nodes {
			left.Versions[v].Nodes[hostname] = node
		}
	}

	return left
}

// copyDBStatus does a deep copy of a dbStatus object and returns it.
func copyDBStatus(status dbStatus) dbStatus {
	fresh := dbStatus{Versions: make(map[string]versionStatus)}
	return mergeDBStatus(fresh, status)
}

func acceptsJSON(r *http.Request) bool {
	for _, accept := range r.Header["Accept"] {
		if accept == "application/json" {
			return true
		}
	}

	return false
}

func (db *db) status() dbStatus {
	status := dbStatus{Versions: make(map[string]versionStatus)}
	for _, vs := range db.mux.getAll() {
		status.Versions[vs.name] = vs.status()
	}

	hostname := "localhost"
	if db.sequins.peers != nil {
		hostname = db.sequins.address
	}

	current := db.mux.getCurrent()
	db.mux.release(current)
	for name := range status.Versions {
		st := status.Versions[name].Nodes[hostname]
		st.Current = (current != nil && name == current.name)

		status.Versions[name].Nodes[hostname] = st
	}

	return status
}

func (vs *version) status() versionStatus {
	vs.stateLock.Lock()
	defer vs.stateLock.Unlock()

	st := versionStatus{
		Nodes:                make(map[string]nodeVersionStatus),
		NumPartitions:        vs.numPartitions,
		Path:                 vs.sequins.backend.DisplayPath(vs.db.name, vs.name),
		ReplicationHistogram: make(map[int]int),
		TargetReplication:    vs.sequins.config.Sharding.Replication,
	}

	partitions := make([]int, 0, len(vs.partitions.SelectedLocal()))
	for p := range vs.partitions.SelectedLocal() {
		partitions = append(partitions, p)
	}

	hostname := "localhost"
	shardID := ""
	if vs.sequins.peers != nil {
		hostname = vs.sequins.address
		shardID = vs.sequins.peers.ShardID
	}

	sort.Ints(partitions)
	nodeStatus := nodeVersionStatus{
		Name:       hostname,
		CreatedAt:  vs.created.UTC().Truncate(time.Second),
		State:      vs.state,
		Partitions: partitions,
		ShardID:    shardID,
	}

	if !vs.active.IsZero() {
		nodeStatus.ActiveAt = vs.active.UTC().Truncate(time.Second)
	}

	st.Nodes[hostname] = nodeStatus
	st.calculateReplicationStats()

	return st
}

func (vs *version) setState(state versionState) {
	vs.stateLock.Lock()
	defer vs.stateLock.Unlock()

	if vs.state != versionError {
		vs.state = state
		if state == versionActive {
			vs.active = time.Now()

			if vs.stats != nil {
				tags := []string{fmt.Sprintf("sequins_db:%s", vs.db.name)}
				duration := vs.active.Sub(vs.created)
				vs.stats.Timing("db_creation_time", duration, tags, 1)
			}
		}
	}
}

type nodeVersionStatuses []nodeVersionStatus

func (nvs nodeVersionStatuses) Len() int {
	return len(nvs)
}

func (nvs nodeVersionStatuses) Swap(i, j int) {
	nvs[i], nvs[j] = nvs[j], nvs[i]
}

func (nvs nodeVersionStatuses) Less(i, j int) bool {
	if cmp := strings.Compare(nvs[i].ShardID, nvs[j].ShardID); cmp != 0 {
		return cmp < 0
	}
	return strings.Compare(nvs[i].Name, nvs[j].Name) < 0
}

// calculateReplicationStats will populate the versionStatus with replication
// information based on all of the nodes it contains. This should only be
// called once for a given versionStatus.
func (vs *versionStatus) calculateReplicationStats() {
	// Reset the initial values
	vs.ReplicationHistogram = make(map[int]int)
	vs.MissingPartitions = 0
	vs.UnderreplicatedPartitions = 0
	vs.OverreplicatedPartitions = 0

	partitionReplication := make(map[int]int)
	for _, node := range vs.Nodes {
		if node.State == versionBuilding || node.State == versionActive || node.State == versionRemoving {
			for _, p := range node.Partitions {
				partitionReplication[p]++
			}
		}
	}

	totalReplication := 0
	for _, replication := range partitionReplication {
		vs.ReplicationHistogram[replication]++
		totalReplication += replication
	}

	if vs.NumPartitions == 0 {
		vs.AverageReplication = 0
	} else {
		vs.AverageReplication = float32(totalReplication) / float32(vs.NumPartitions)
	}

	for replication, count := range vs.ReplicationHistogram {
		if replication == 0 {
			vs.MissingPartitions += count
		} else if replication < vs.TargetReplication {
			vs.UnderreplicatedPartitions += count
		} else if replication > vs.TargetReplication {
			vs.OverreplicatedPartitions += count
		}
	}
}

func (vs versionStatus) SortedNodes() nodeVersionStatuses {
	nvs := make(nodeVersionStatuses, 0, len(vs.Nodes))
	for _, n := range vs.Nodes {
		nvs = append(nvs, n)
	}
	sort.Sort(nvs)
	return nvs
}
