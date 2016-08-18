package main

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"os"
	"regexp"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var commentedDefaultRegex = regexp.MustCompile(`# (\w+ = .+)\n(?:#.*\n)*\n*`)

func loadAndValidateConfig(path string) (sequinsConfig, error) {
	config, err := loadConfig(path)
	if err != nil {
		return config, err
	}

	return validateConfig(config)
}

func createTestConfig(t *testing.T, conf string) string {
	tmpfile, err := ioutil.TempFile("", "sequins-conf-test")
	if err != nil {
		t.Fatal(err)
	}

	_, err = tmpfile.WriteString(conf)
	if err != nil {
		t.Fatal(err)
	}

	err = tmpfile.Close()
	if err != nil {
		t.Fatal(err)
	}

	return tmpfile.Name()
}

func TestExampleConfig(t *testing.T) {
	config, err := loadAndValidateConfig("sequins.conf.example")
	require.NoError(t, err, "sequins.conf.example should exist and be valid")

	defaults := defaultConfig()
	defaults.Source = config.Source
	assert.Equal(t, defaults, config, "sequins.conf.example should eval to the default config")
}

// TestExampleConfigDefaults uncomments the defaults in sequins.conf.example,
// and then checks that against the actual defaults. It tries to skip any
// options annotated with "Unset by default".
func TestExampleConfigDefaults(t *testing.T) {
	raw, err := ioutil.ReadFile("sequins.conf.example")
	require.NoError(t, err, "sequins.conf.example should be readable")

	// Uncomment all the commented defaults, and make sure they're the actual
	// defaults. But we have to skip ones that we document as "Unset by default".
	replaced := commentedDefaultRegex.ReplaceAllFunc(raw, func(match []byte) []byte {
		if bytes.Index(match, []byte("Unset by default.")) == -1 {
			return append(commentedDefaultRegex.FindSubmatch(match)[1], '\n')
		} else {
			return nil
		}
	})

	t.Logf("---replaced config---\n%s\n---replaced config---", string(replaced))
	path := createTestConfig(t, string(replaced))
	config, err := loadAndValidateConfig(path)
	require.NoError(t, err, "the uncommented sequins.conf.example should exist and be valid")

	defaults := defaultConfig()
	defaults.Source = config.Source
	assert.Equal(t, defaults, config, "the uncommented sequins.conf.example should eval to the default config")

	os.Remove(path)
}

func TestSimpleConfig(t *testing.T) {
	path := createTestConfig(t, `
		source = "s3://foo/bar"
		require_success_file = true
		refresh_period = "1h"

		[zk]
		servers = ["zk:2181"]
	`)

	config, err := loadAndValidateConfig(path)
	require.NoError(t, err, "loading a basic config should work")

	assert.Equal(t, "s3://foo/bar", config.Source, "Source should be set")
	assert.Equal(t, true, config.RequireSuccessFile, "RequireSuccessFile should be set")
	assert.Equal(t, time.Hour, config.RefreshPeriod.Duration, "RefreshPeriod (a duration) should be set")
	assert.Equal(t, []string{"zk:2181"}, config.ZK.Servers, "ZK.Servers should be set")

	defaults := defaultConfig()
	defaults.Source = config.Source
	defaults.RequireSuccessFile = config.RequireSuccessFile
	defaults.RefreshPeriod = config.RefreshPeriod
	defaults.ZK.Servers = config.ZK.Servers
	assert.Equal(t, defaults, config, "the configuration should otherwise be the default")

	os.Remove(path)
}

func TestEmptyConfig(t *testing.T) {
	path := createTestConfig(t, "source = \"/foo\"")

	config, err := loadAndValidateConfig(path)
	require.NoError(t, err, "loading an empty config should work")

	assert.Equal(t, "/foo", config.Source, "the root should be set")

	config.Source = ""
	assert.Equal(t, defaultConfig(), config, "an empty config should eval to the default config")
}

func TestConfigSearchPath(t *testing.T) {
	path := createTestConfig(t, "source = \"/foo\"")

	_, err := loadAndValidateConfig(fmt.Sprintf("%s:/this/doesnt/exist.conf", path))
	assert.NoError(t, err, "it should find the config file on the search path")

	_, err = loadAndValidateConfig(fmt.Sprintf("/this/doesnt/exist.conf:%s", path))
	assert.NoError(t, err, "it should find the config file on the search path")

	os.Remove(path)
}

func TestConfigExtraKeys(t *testing.T) {
	path := createTestConfig(t, `
		source = "s3://foo/bar"
		require_success_file = true
		refresh_period = "1h"

		[zk]
		servers = ["zk:2181"]

		foo = "bar" # not a real config property!
	`)

	_, err := loadAndValidateConfig(path)
	assert.Error(t, err, "it should throw an error if there are extra config properties")

	os.Remove(path)
}

func TestConfigInvalidCompression(t *testing.T) {
	path := createTestConfig(t, `
    source = "s3://foo/bar"
    require_success_file = true
    refresh_period = "1h"

    [storage]
    compression = "notacompression"
  `)

	_, err := loadAndValidateConfig(path)
	assert.Error(t, err, "it should throw an error if an invalid compression is specified")

	os.Remove(path)
}

func TestConfigRelativeSource(t *testing.T) {
	path := createTestConfig(t, `
    source = "foo/bar"
    local_store = "/foo/bar/baz"
  `)

	_, err := loadAndValidateConfig(path)
	assert.Error(t, err, "it should throw an error if the source root is a relative path")
}

func TestConfigRelativeSourceURL(t *testing.T) {
	path := createTestConfig(t, `
    source = "file://foo/bar"
    local_store = "/foo/bar/baz"
  `)

	_, err := loadAndValidateConfig(path)
	assert.Error(t, err, "it should throw an error if the source root is a relative path")
}

func TestConfigRelativeLocalStore(t *testing.T) {
	path := createTestConfig(t, `
    source = "/foo/bar"
    local_store = "bar/baz"
  `)

	_, err := loadAndValidateConfig(path)
	assert.Error(t, err, "it should throw an error if the local store is a relative path")
}

func TestConfigLocalStoreWithinRoot(t *testing.T) {
	path := createTestConfig(t, `
    source = "/foo/bar"
    local_store = "/foo/bar/baz"
  `)

	_, err := loadAndValidateConfig(path)
	assert.Error(t, err, "it should throw an error if the local store is within the source root")
}
