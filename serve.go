package main

import (
	"io"
	"log"
	"math/rand"
	"net/http"
	"strconv"

	"github.com/stripe/sequins/blocks"
)

// serveKey is the entrypoint for incoming HTTP requests. It looks up the value
// locally, for, failing that, asks a peer that has it. If the request was
// already proxied to us, it is not proxied further.
func (vs *version) serveKey(w http.ResponseWriter, r *http.Request, key string) {
	// If we don't have any data for this version at all, that's a 404.
	if vs.numPartitions == 0 {
		vs.serveNotFound(w)
		return
	}

	partition, alternatePartition := blocks.KeyPartition([]byte(key), vs.numPartitions)
	if vs.partitions.HaveLocal(partition) || vs.partitions.HaveLocal(alternatePartition) {
		record, err := vs.blockStore.Get(key)
		if err != nil {
			vs.serveError(w, key, err)
			return
		}

		vs.serveLocal(w, key, record)
	} else if r.URL.Query().Get("proxy") == "" {
		vs.serveProxied(w, r, key, partition, alternatePartition)
	} else {
		vs.serveError(w, key, errProxiedIncorrectly)
	}
}

func (vs *version) serveLocal(w http.ResponseWriter, key string, record *blocks.Record) {
	if record == nil {
		vs.serveNotFound(w)
		return
	}

	defer record.Close()
	w.Header().Set(versionHeader, vs.name)
	w.Header().Set("Content-Length", strconv.FormatUint(record.ValueLen, 10))
	w.Header().Set("Last-Modified", vs.created.UTC().Format(http.TimeFormat))
	_, err := io.Copy(w, record)
	if err != nil {
		// We already wrote a 200 OK, so not much we can do here except log.
		log.Printf("Error streaming response for /%s/%s (version %s): %s", vs.db.name, key, vs.name, err)
	}
}

func (vs *version) serveProxied(w http.ResponseWriter, r *http.Request,
	key string, partition, alternatePartition int) {

	// Shuffle the peers, so we try them in a random order.
	// TODO: We don't want to blacklist nodes, but we can weight them lower
	peers := shuffle(vs.partitions.FindPeers(partition))
	if len(peers) == 0 {
		log.Printf("No peers available for /%s/%s (version %s)", vs.db.name, key, vs.name)
		w.WriteHeader(http.StatusBadGateway)
		return
	}

	resp, peer, err := vs.proxy(r, peers)
	if err == nil && resp.StatusCode == 404 && alternatePartition != partition {
		log.Println("Trying alternate partition for pathological key", key)

		resp.Body.Close()
		alternatePeers := shuffle(vs.partitions.FindPeers(alternatePartition))
		resp, peer, err = vs.proxy(r, alternatePeers)
	}

	if err == errNoAvailablePeers {
		// Either something is wrong with sharding, or all peers errored for some
		// other reason. 502
		log.Printf("No peers available for /%s/%s (version %s)", vs.db.name, key, vs.name)
		w.WriteHeader(http.StatusBadGateway)
		return
	} else if err == errProxyTimeout {
		// All of our peers failed us. 504.
		log.Printf("All peers timed out for /%s/%s (version %s)", vs.db.name, key, vs.name)
		w.WriteHeader(http.StatusGatewayTimeout)
		return
	} else if err == errRequestCanceled {
		// The connection was closed by the client. 499.
		log.Printf("Connection closed by client for /%s/%s (version %s)", vs.db.name, key, vs.name)
		w.WriteHeader(499)
		return
	} else if err != nil {
		// Some other error. 500.
		vs.serveError(w, key, err)
		return
	}

	// Proxying can produce inconsistent versions if something is broken. Use the
	// one the peer set.
	w.Header().Set(versionHeader, resp.Header.Get(versionHeader))
	w.Header().Set(proxyHeader, peer)
	w.Header().Set("Content-Length", resp.Header.Get("Content-Length"))
	w.Header().Set("Last-Modified", vs.created.UTC().Format(http.TimeFormat))
	w.WriteHeader(resp.StatusCode)

	// TODO: Apparently in 1.7 the client always asks for gzip by default. If our
	// client asks for gzip too, we should be able to pass through without
	// decompressing.
	defer resp.Body.Close()
	_, err = io.Copy(w, resp.Body)
	if err != nil {
		// We already wrote a 200 OK, so not much we can do here except log.
		log.Printf("Error copying response from peer for /%s/%s (version %s): %s", vs.db.name, key, vs.name, err)
	}
}

func (vs *version) serveNotFound(w http.ResponseWriter) {
	w.Header().Set(versionHeader, vs.name)
	w.WriteHeader(http.StatusNotFound)
}

func (vs *version) serveError(w http.ResponseWriter, key string, err error) {
	log.Printf("Error fetching value for /%s/%s: %s\n", vs.db.name, key, err)
	w.WriteHeader(http.StatusInternalServerError)
}

func shuffle(vs []string) []string {
	shuffled := make([]string, len(vs))
	perm := rand.Perm(len(vs))
	for i, v := range perm {
		shuffled[v] = vs[i]
	}

	return shuffled
}
