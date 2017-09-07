package sharding

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestSmallestAvailableShardID(t *testing.T) {
	t.Parallel()

	peers := &Peers{
		peers: map[peer]bool{
			peer{
				shardID: "2",
			}: true,
			peer{
				shardID: "4",
			}: true,
		},
	}
	result, err := peers.SmallestAvailableShardID()
	assert.NoError(t, err)
	assert.Equal(t, "1", result)

	peers = &Peers{
		peers: map[peer]bool{
			peer{
				shardID: "1",
			}: true,
			peer{
				shardID: "3",
			}: true,
		},
	}
	result, err = peers.SmallestAvailableShardID()
	assert.NoError(t, err)
	assert.Equal(t, "2", result)

	peers = &Peers{
		peers: map[peer]bool{
			peer{
				shardID: "1",
			}: true,
			peer{
				shardID: "2",
			}: true,
		},
	}
	result, err = peers.SmallestAvailableShardID()
	assert.NoError(t, err)
	assert.Equal(t, "3", result)

	peers = &Peers{
		peers: map[peer]bool{
			peer{
				shardID: "1",
			}: true,
			peer{
				shardID: "4",
			}: true,
			peer{
				shardID: "5",
			}: true,
		},
	}
	result, err = peers.SmallestAvailableShardID()
	assert.NoError(t, err)
	assert.Equal(t, "2", result)

	peers = &Peers{
		peers: map[peer]bool{},
	}
	result, err = peers.SmallestAvailableShardID()
	assert.NoError(t, err)
	assert.Equal(t, "1", result)
}
