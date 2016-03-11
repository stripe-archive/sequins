package main

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/samuel/go-zookeeper/zk"
)

type zkLogWriter struct {
	t *testing.T
}

func (lw zkLogWriter) Write(b []byte) (int, error) {
	lw.t.Logf("[ZK] %s", string(b))
	return len(b), nil
}

func createTestZkCluster(t *testing.T) ([]string, *zk.TestCluster) {
	zk, err := zk.StartTestCluster(3, nil, zkLogWriter{t})
	require.NoError(t, err, "zk setup")

	hosts := make([]string, len(zk.Servers))
	for i, srv := range zk.Servers {
		hosts[i] = fmt.Sprintf("127.0.0.1:%d", srv.Port)
	}

	return hosts, zk
}

func connectZookeeperTest(t *testing.T) (*zkWatcher, *zk.TestCluster) {
	hosts, zk := createTestZkCluster(t)
	zkWatcher, err := connectZookeeper(hosts, "/test-sequins")
	require.NoError(t, err, "zk setup")

	return zkWatcher, zk
}

func TestZKWatcher(t *testing.T) {
	t.Skip() // TODO this test needs to be fleshed out

	connectZookeeperTest(t)
}
