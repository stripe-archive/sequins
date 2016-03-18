package main

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/BurntSushi/toml"
)

const defaultSearchPath = "sequins.conf:/etc/sequins.conf"

var errNoConfig = errors.New("no config file found")

type sequinsConfig struct {
	Root               string   `toml:"root"`
	Bind               string   `toml:"bind"`
	MaxParallelLoads   int      `toml:"max_parallel_loads"`
	ThrottleLoads      duration `toml:"throttle_loads"`
	LocalStore         string   `toml:"local_store"`
	RefreshPeriod      duration `toml:"refresh_period"`
	RequireSuccessFile bool     `toml:"require_success_file"`
	ContentType        string   `toml:"content_type"`

	S3    s3Config    `toml:"s3"`
	ZK    zkConfig    `toml:"zk"`
	Debug debugConfig `toml:"debug"`
	Test  testConfig  `toml:"test"`
}

type s3Config struct {
	Region          string `toml:"region"`
	AccessKeyId     string `toml:"access_key_id"`
	SecretAccessKey string `toml:"secret_access_key"`
}

type zkConfig struct {
	Servers            []string `toml:"servers"`
	Replication        int      `toml:"replication"`
	TimeToConverge     duration `toml:"time_to_converge"`
	ProxyTimeout       duration `toml:"proxy_timeout"`
	ClusterName        string   `toml:"cluster_name"`
	AdvertisedHostname string   `toml:"advertised_hostname"`
	ShardID            string   `toml:"shard_id"`
}

type debugConfig struct {
	Bind    string `toml:"bind"`
	Expvars bool   `toml:"expvars"`
	Pprof   bool   `toml:"pprof"`
}

// testConfig has some options used in functional tests to slow sequins down
// and make it more observable.
type testConfig struct {
	UpgradeDelay      duration `toml:"upgrade_delay"`
	AllowLocalCluster bool     `toml:"allow_local_cluster"`
}

func defaultConfig() sequinsConfig {
	return sequinsConfig{
		Root:               "",
		Bind:               "0.0.0.0:9599",
		LocalStore:         "/var/sequins/",
		MaxParallelLoads:   0,
		RefreshPeriod:      duration{time.Duration(0)},
		RequireSuccessFile: false,
		ContentType:        "",
		S3: s3Config{
			Region:          "",
			AccessKeyId:     "",
			SecretAccessKey: "",
		},
		ZK: zkConfig{
			Servers:            nil,
			Replication:        2,
			TimeToConverge:     duration{10 * time.Second},
			ProxyTimeout:       duration{100 * time.Millisecond},
			ClusterName:        "sequins",
			AdvertisedHostname: "",
			ShardID:            "",
		},
		Debug: debugConfig{
			Bind:    "",
			Expvars: true,
			Pprof:   false,
		},
		Test: testConfig{
			UpgradeDelay: duration{time.Duration(0)},
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
