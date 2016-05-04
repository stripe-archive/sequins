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
}

func init() {
	// This is chunked into the binary with go-bindata. See the Makefile for more
	// information.
	raw := string(MustAsset("status.tmpl"))
	statusTemplate = template.Must(template.New("status").Funcs(templateFns).Parse(raw))
}

type status struct {
	DBs map[string]dbStatus `json:"dbs"`
}

type dbStatus struct {
	Versions map[string]versionStatus `json:"versions",omitempty`
}

type versionStatus struct {
	Path                      string  `json:"path"`
	NumPartitions             int     `json:"num_partitions"`
	UnderreplicatedPartitions int     `json:"underreplicated_partitions"`
	OverreplicatedPartitions  int     `json:"overreplicated_partitions"`
	MissingPartitions         int     `json:"missing_partitions"`
	TargetReplication         int     `json:"target_replication"`
	AverageReplication        float32 `json:"average_replication"`

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
	versionAvailable versionState = "AVAILABLE"
	versionRemoving               = "REMOVING"
	versionBuilding               = "BUILDING"
	versionError                  = "ERROR"
)

func (s *sequins) serveStatus(w http.ResponseWriter, r *http.Request) {
	s.dbsLock.RLock()

	status := status{DBs: make(map[string]dbStatus)}
	for name, db := range s.dbs {
		status.DBs[name] = db.status()
	}

	s.dbsLock.RUnlock()

	// By default, serve our peers' statuses merged with ours. We take
	// extra care not to mutate local status structs.
	if r.URL.Query().Get("proxy") == "" && s.peers != nil {
		for _, p := range s.peers.getAll() {
			peerStatus, err := s.getPeerStatus(p, "")
			if err != nil {
				log.Printf("Error fetching status from peer %s: %s", p, err)
				continue
			}

			merged := peerStatus
			for db := range status.DBs {
				if _, ok := merged.DBs[db]; ok {
					merged.DBs[db] = mergePeerDBStatus(merged.DBs[db], status.DBs[db])
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
			w.WriteHeader(500)
		}
	}
}

func (db *db) serveStatus(w http.ResponseWriter, r *http.Request) {
	s := db.status()

	// By default, serve our peers' statuses merged with ours.
	if r.URL.Query().Get("proxy") == "" && db.sequins.peers != nil {
		for _, p := range db.sequins.peers.getAll() {
			peerStatus, err := db.sequins.getPeerStatus(p, db.name)
			if err != nil {
				log.Printf("Error fetching status from peer %s: %s", p, err)
				continue
			}

			peerDBStatus := peerStatus.DBs[db.name]
			s = mergePeerDBStatus(peerDBStatus, s)
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
		return
	} else {
		status := status{DBs: make(map[string]dbStatus)}
		status.DBs[db.name] = s
		err := statusTemplate.Execute(w, status)
		if err != nil {
			log.Println("Error rendering status:", err)
			w.WriteHeader(500)
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

	resp, err := s.proxyClient.Do(req)
	if err != nil {
		return status, err
	}

	defer resp.Body.Close()
	decoder := json.NewDecoder(resp.Body)
	if db == "" {
		err = decoder.Decode(&status)
	} else {
		s := dbStatus{}
		err = decoder.Decode(&s)
		if err != nil {
			return status, err
		}

		status.DBs = map[string]dbStatus{db: s}
	}

	return status, err
}

// mergePeerDBStatus merges two dbStatus objects, mutating only the
// left one.
func mergePeerDBStatus(left, right dbStatus) dbStatus {
	for v := range right.Versions {
		if vst, ok := left.Versions[v]; !ok {
			left.Versions[v] = versionStatus{
				Nodes:             make(map[string]nodeVersionStatus),
				Path:              vst.Path,
				NumPartitions:     vst.NumPartitions,
				TargetReplication: vst.TargetReplication,
			}
		}

		for p, n := range right.Versions[v].Nodes {
			left.Versions[v].Nodes[p] = n
		}
	}

	return left
}

func calculateReplicationStats(vst versionStatus) versionStatus {
	replication := make(map[int]int)
	for _, node := range vst.Nodes {
		if node.State == versionAvailable || node.State == versionRemoving {
			for _, p := range node.Partitions {
				replication[p] += 1
			}
		}
	}

	total := 0
	for p := 0; p < vst.NumPartitions; p++ {
		r := replication[p]
		total += r
		if r == 0 {
			vst.MissingPartitions += 1
		} else if r < vst.TargetReplication {
			vst.UnderreplicatedPartitions += 1
		} else if r > vst.TargetReplication {
			vst.OverreplicatedPartitions += 1
		}
	}

	vst.AverageReplication = float32(total) / float32(vst.NumPartitions)
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
	db.versionStatusLock.RLock()
	defer db.versionStatusLock.RUnlock()

	status := dbStatus{Versions: make(map[string]versionStatus)}
	for name, versionStatus := range db.versionStatus {
		status.Versions[name] = versionStatus
	}

	return status
}

func (db *db) trackVersion(version *version, state versionState) {
	db.versionStatusLock.Lock()
	defer db.versionStatusLock.Unlock()

	hostname := "localhost"
	if db.sequins.peers != nil {
		hostname = db.sequins.peers.address
	}

	st, ok := db.versionStatus[version.name]
	if !ok {
		st = versionStatus{
			Path:          db.sequins.backend.DisplayPath(db.name, version.name),
			NumPartitions: version.numPartitions,
			Nodes:         make(map[string]nodeVersionStatus),
		}

		if version.partitions != nil {
			st.TargetReplication = version.partitions.replication
		}

		partitions := make([]int, 0, len(version.selectedLocalPartitions))
		for p := range version.selectedLocalPartitions {
			partitions = append(partitions, p)
		}

		sort.Ints(partitions)
		nodeStatus := nodeVersionStatus{
			CreatedAt:  time.Now().UTC().Truncate(time.Second),
			State:      state,
			Partitions: partitions,
		}

		if state == versionAvailable {
			nodeStatus.AvailableAt = time.Now().UTC().Truncate(time.Second)
		}

		st.Nodes[hostname] = nodeStatus
	} else {
		nodeStatus := st.Nodes[hostname]
		nodeStatus.State = state
		if state == versionAvailable {
			nodeStatus.AvailableAt = time.Now().UTC().Truncate(time.Second)
		}

		st.Nodes[hostname] = nodeStatus
	}

	db.versionStatus[version.name] = st

	current := db.mux.getCurrent()
	db.mux.release(current)
	for name := range db.versionStatus {
		st := db.versionStatus[name].Nodes[hostname]
		st.Current = (current != nil && name == current.name)

		db.versionStatus[name].Nodes[hostname] = st
	}
}

func (db *db) untrackVersion(version *version) {
	db.versionStatusLock.Lock()
	defer db.versionStatusLock.Unlock()

	delete(db.versionStatus, version.name)
}
