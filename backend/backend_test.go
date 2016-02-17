package backend

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestBackend(t *testing.T) {
	backend := NewLocalBackend("../test")
	versions, err := backend.ListVersions("names", false)
	require.NoError(t, err)
	assert.Equal(t, versions, []string{"0", "1"})
}

func TestBackendCheckForSuccess(t *testing.T) {
	backend := NewLocalBackend("../test")
	versions, err := backend.ListVersions("names", true)
	require.NoError(t, err)
	assert.Equal(t, versions, []string{"0"})
}
