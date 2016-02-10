package main

import (
	"fmt"
	"testing"

	"github.com/samuel/go-zookeeper/zk"
)

type zkLogWriter struct {
	t *testing.T
}

func (lw zkLogWriter) Write(b []byte) (int, error) {
	lw.t.Logf("[ZK] %s", string(b))
	return len(b), nil
}

func connectZookeeperTest(t *testing.T) (*zkWatcher, *zk.TestCluster) {
	ts, err := zk.StartTestCluster(3, nil, zkLogWriter{t})
	if err != nil {
		t.Fatal(err)
	}

	hosts := make([]string, len(ts.Servers))
	for i, srv := range ts.Servers {
		hosts[i] = fmt.Sprintf("127.0.0.1:%d", srv.Port)
	}

	zkWatcher, err := connectZookeeper([]string{"localhost:2181"}, "test-sequins")

	return zkWatcher, ts
}

func TestZKWatcher(t *testing.T) {
	connectZookeeperTest(t)
}
