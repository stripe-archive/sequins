// package zktest provides utilities for testing against a live zookeeper.
package zktest

import (
	"math/rand"
	"testing"
	"time"

	"github.com/samuel/go-zookeeper/zk"
	"github.com/stretchr/testify/require"
)

// New returns a new TestCluster rooted at a temporary dir.
func New(t *testing.T) *zk.TestCluster {
	tzk, err := zk.StartTestCluster(1, nil, nil)
	require.NoError(t, err, "zk setup")
	return tzk
}

// RandomPort returns a port in the range 16000-22000.
func RandomPort() int {
	rand.Seed(time.Now().UnixNano())
	return int(rand.Int31n(6000) + 16000)
}
