package main

import (
	"errors"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/BurntSushi/toml"

	"github.com/stripe/sequins/blocks"
)

const defaultSearchPath = "sequins.conf:/etc/sequins.conf"

var errNoConfig = errors.New("no config file found")

type sequinsConfig struct {
	Source             string   `toml:"source"`
	Bind               string   `toml:"bind"`
	MaxParallelLoads   int      `toml:"max_parallel_loads"`
	ThrottleLoads      duration `toml:"throttle_loads"`
	LocalStore         string   `toml:"local_store"`
	RefreshPeriod      duration `toml:"refresh_period"`
	RequireSuccessFile bool     `toml:"require_success_file"`
	ContentType        string   `toml:"content_type"`

	Storage  storageConfig  `toml:"storage"`
	S3       s3Config       `toml:"s3"`
	Sharding shardingConfig `toml:"sharding"`
	ZK       zkConfig       `toml:"zk"`
	Debug    debugConfig    `toml:"debug"`
	Test     testConfig     `toml:"test"`
}

type storageConfig struct {
	Compression blocks.Compression `toml:"compression"`
	BlockSize   int                `toml:"block_size"`
}

type s3Config struct {
	Region          string `toml:"region"`
	AccessKeyId     string `toml:"access_key_id"`
	SecretAccessKey string `toml:"secret_access_key"`
}

type shardingConfig struct {
	Enabled            bool     `toml:"enabled"`
	Replication        int      `toml:"replication"`
	TimeToConverge     duration `toml:"time_to_converge"`
	ProxyTimeout       duration `toml:"proxy_timeout"`
	ProxyStageTimeout  duration `toml:"proxy_stage_timeout"`
	ClusterName        string   `toml:"cluster_name"`
	AdvertisedHostname string   `toml:"advertised_hostname"`
	ShardID            string   `toml:"shard_id"`
}

type zkConfig struct {
	Servers        []string `toml:"servers"`
	ConnectTimeout duration `toml:"connect_timeout"`
	SessionTimeout duration `toml:"session_timeout"`
}

type debugConfig struct {
	Bind    string `toml:"bind"`
	Expvars bool   `toml:"expvars"`
	Pprof   bool   `toml:"pprof"`
}

// testConfig has some options used in functional tests to slow sequins down
// and make it more observable.
type testConfig struct {
	UpgradeDelay         duration `toml:"upgrade_delay"`
	AllowLocalCluster    bool     `toml:"allow_local_cluster"`
	VersionRemoveTimeout duration `toml:"version_remove_timeout"`
	S3                   s3Config `toml:"s3"`
}

func defaultConfig() sequinsConfig {
	return sequinsConfig{
		Source:             "",
		Bind:               "0.0.0.0:9599",
		LocalStore:         "/var/sequins/",
		MaxParallelLoads:   0,
		RefreshPeriod:      duration{time.Duration(0)},
		RequireSuccessFile: false,
		ContentType:        "",
		Storage: storageConfig{
			Compression: blocks.SnappyCompression,
			BlockSize:   4096,
		},
		S3: s3Config{
			Region:          "",
			AccessKeyId:     "",
			SecretAccessKey: "",
		},
		Sharding: shardingConfig{
			Enabled:            false,
			Replication:        2,
			TimeToConverge:     duration{10 * time.Second},
			ProxyTimeout:       duration{100 * time.Millisecond},
			ProxyStageTimeout:  duration{time.Duration(0)},
			ClusterName:        "sequins",
			AdvertisedHostname: "",
			ShardID:            "",
		},
		ZK: zkConfig{
			Servers:        []string{"localhost:2181"},
			ConnectTimeout: duration{1 * time.Second},
			SessionTimeout: duration{10 * time.Second},
		},
		Debug: debugConfig{
			Bind:    "",
			Expvars: true,
			Pprof:   false,
		},
		Test: testConfig{
			UpgradeDelay:         duration{time.Duration(0)},
			VersionRemoveTimeout: duration{time.Duration(0)},
			S3: s3Config{
				Region:          "",
				AccessKeyId:     "",
				SecretAccessKey: "",
			},
		},
	}
}

func loadConfig(searchPath string) (sequinsConfig, error) {
	if searchPath == "" {
		searchPath = defaultSearchPath
	}

	config := defaultConfig()
	paths := filepath.SplitList(searchPath)
	for _, path := range paths {
		md, err := toml.DecodeFile(path, &config)
		if os.IsNotExist(err) {
			continue
		} else if err != nil {
			return config, err
		} else if len(md.Undecoded()) > 0 {
			return config, fmt.Errorf("found unrecognized properties: %v", md.Undecoded())
		}

		return config, nil
	}

	return config, errNoConfig
}

func validateConfig(config sequinsConfig) (sequinsConfig, error) {
	if !filepath.IsAbs(config.LocalStore) {
		return config, fmt.Errorf("local store path must be absolute: %s", config.LocalStore)
	}

	if config.Source == "" {
		return config, errors.New("source must be set")
	}

	parsed, err := url.Parse(config.Source)
	if err != nil {
		return config, fmt.Errorf("parsing source: %s", err)
	}

	if parsed.Scheme == "" || parsed.Scheme == "file" {
		if parsed.Host != "" {
			return config, fmt.Errorf("local source path is invalid (likely missing a '/'): %s", config.Source)
		}

		if !filepath.IsAbs(parsed.Path) {
			return config, fmt.Errorf("local source path must be absolute: %s", config.Source)
		}

		if strings.HasPrefix(filepath.Clean(config.LocalStore), filepath.Clean(parsed.Path)) {
			return config, fmt.Errorf("local store can't be within source root: %s", config.LocalStore)
		}

		if config.Sharding.Enabled && !config.Test.AllowLocalCluster {
			return config, errors.New("you can't run sequins with sharding enabled on local paths")
		}
	}

	switch config.Storage.Compression {
	case blocks.SnappyCompression, blocks.NoCompression:
	default:
		return config, fmt.Errorf("lnrecognized compression option: %s", config.Storage.Compression)
	}

	if config.Sharding.Replication <= 0 {
		return config, fmt.Errorf("lnvalid replication factor: %d", config.Sharding.Replication)
	}

	return config, nil
}

type duration struct {
	time.Duration
}

func (d *duration) UnmarshalText(text []byte) error {
	var err error
	d.Duration, err = time.ParseDuration(string(text))
	return err
}

func (d duration) MarshalText() ([]byte, error) {
	return []byte(d.Duration.String()), nil
}
