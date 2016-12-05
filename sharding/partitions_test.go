package sharding

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestDedupeRandom(t *testing.T) {
	dupe := []string{"4", "1", "2", "1", "3", "2"}
	expected := []string{"4", "1", "2", "3"}

	deduped := dedupe(dupe)
	assert.Equal(t, expected, deduped)
}

func TestDedupe(t *testing.T) {
	dupe := []string{"1", "1", "1", "2", "2", "3", "3", "3", "4", "5", "5"}
	expected := []string{"1", "2", "3", "4", "5"}

	deduped := dedupe(dupe)
	assert.Equal(t, expected, deduped)
}
