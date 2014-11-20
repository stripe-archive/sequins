package backend

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestBackend(t *testing.T) {
	backend := NewLocalBackend("../test_data")
	version, err := backend.LatestVersion(false)
	assert.Nil(t, err)
	assert.Equal(t, version, "1")
}

func TestBackendCheckForSuccess(t *testing.T) {
	backend := NewLocalBackend("../test_data")
	version, err := backend.LatestVersion(true)
	assert.Nil(t, err)
	assert.Equal(t, version, "0")
}
