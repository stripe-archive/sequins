package sharding

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestSmallestAvailableShardID(t *testing.T) {
	t.Parallel()

	peers := &Peers{
		Peers: map[peer]bool{
			peer{
				ShardID: "2",
			}: true,
			peer{
				ShardID: "4",
			}: true,
		},
	}
	result, err := peers.SmallestAvailableShardID()
	assert.NoError(t, err)
	assert.Equal(t, "1", result)

	peers = &Peers{
		Peers: map[peer]bool{
			peer{
				ShardID: "1",
			}: true,
			peer{
				ShardID: "3",
			}: true,
		},
	}
	result, err = peers.SmallestAvailableShardID()
	assert.NoError(t, err)
	assert.Equal(t, "2", result)

	peers = &Peers{
		Peers: map[peer]bool{
			peer{
				ShardID: "1",
			}: true,
			peer{
				ShardID: "2",
			}: true,
		},
	}
	result, err = peers.SmallestAvailableShardID()
	assert.NoError(t, err)
	assert.Equal(t, "3", result)

	peers = &Peers{
		Peers: map[peer]bool{
			peer{
				ShardID: "1",
			}: true,
			peer{
				ShardID: "4",
			}: true,
			peer{
				ShardID: "5",
			}: true,
		},
	}
	result, err = peers.SmallestAvailableShardID()
	assert.NoError(t, err)
	assert.Equal(t, "2", result)

	peers = &Peers{
		Peers: map[peer]bool{},
	}
	result, err = peers.SmallestAvailableShardID()
	assert.NoError(t, err)
	assert.Equal(t, "1", result)
}
