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
	Path                 string      `json:"path"`
	NumPartitions        int         `json:"num_partitions"`
	ReplicationHistogram map[int]int `json:"replication_histogram"`
	TargetReplication    int         `json:"target_replication"`
	AverageReplication   float32     `json:"average_replication"`

	// For backwards compatibility
	MissingPartitions         int `json:"missing_partitions"`
	UnderreplicatedPartitions int `json:"underreplicated_partitions"`
	OverreplicatedPartitions  int `json:"overreplicated_partitions"`

	Nodes map[string]nodeVersionStatus `json:"nodes"`
}

type nodeVersionStatus struct {
	CreatedAt   time.Time    `json:"created_at"`
	AvailableAt time.Time    `json:"available_at,omitempty"`
	Current     bool         `json:"current"`
	State       versionState `json:"state"`
	Partitions  []int        `json:"partitions"`
}

type versionState string

const (
	versionActive    versionState = "ACTIVE"
	versionAvailable              = "AVAILABLE"
	versionRemoving               = "REMOVING"
	versionBuilding               = "BUILDING"
	versionError                  = "ERROR"
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

	// We return a 200 when any database has an ACTIVE or AVAILABLE version
	versionsAvailable := false
	for _, db := range statuses {
		for _, version := range db {
			if version.State == versionActive || version.State == versionAvailable {
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
			for versionName, version := range db.Versions {
				db.Versions[versionName] = calculateReplicationStats(version)
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

		for versionName, version := range s.Versions {
			s.Versions[versionName] = calculateReplicationStats(version)
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

func calculateReplicationStats(vst versionStatus) versionStatus {
	partitionReplication := make(map[int]int)
	for _, node := range vst.Nodes {
		if node.State == versionActive || node.State == versionAvailable || node.State == versionRemoving {
			for _, p := range node.Partitions {
				partitionReplication[p]++
			}
		}
	}

	totalReplication := 0
	for _, replication := range partitionReplication {
		vst.ReplicationHistogram[replication]++
		totalReplication += replication
	}

	if vst.NumPartitions == 0 {
		vst.AverageReplication = 0
	} else {
		vst.AverageReplication = float32(totalReplication) / float32(vst.NumPartitions)
	}

	for replication, count := range vst.ReplicationHistogram {
		if replication == 0 {
			vst.MissingPartitions += count
		} else if replication < vst.TargetReplication {
			vst.UnderreplicatedPartitions += count
		} else if replication > vst.TargetReplication {
			vst.OverreplicatedPartitions += count
		}
	}

	return vst
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

	sort.Ints(partitions)
	nodeStatus := nodeVersionStatus{
		CreatedAt:  vs.created.UTC().Truncate(time.Second),
		State:      vs.state,
		Partitions: partitions,
	}

	if !vs.available.IsZero() {
		nodeStatus.AvailableAt = vs.available.UTC().Truncate(time.Second)
	}

	hostname := "localhost"
	if vs.sequins.peers != nil {
		hostname = vs.sequins.address
	}

	st.Nodes[hostname] = nodeStatus
	return st
}

func (vs *version) setState(state versionState) {
	vs.stateLock.Lock()
	defer vs.stateLock.Unlock()

	if vs.state != versionError {
		vs.state = state
		if state == versionAvailable || state == versionActive {
			vs.available = time.Now()

			if vs.stats != nil {
				tags := []string{fmt.Sprintf("sequins_db:%s", vs.db.name)}
				duration := vs.available.Sub(vs.created)
				vs.stats.Timing("db_creation_time", duration, tags, 1)
			}
		}
	}
}
