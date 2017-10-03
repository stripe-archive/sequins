package backend

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestBackend(t *testing.T) {
	backend := NewLocalBackend("../test_databases/healthy")
	versions, err := backend.ListVersions("baby-names", "", false)
	require.NoError(t, err)
	assert.Equal(t, versions, []string{"1"})
}

func TestBackendCheckForSuccess(t *testing.T) {
	backend := NewLocalBackend("../test_databases/healthy")
	versions, err := backend.ListVersions("baby-names", "", true)
	require.NoError(t, err)
	assert.Empty(t, versions)
}
