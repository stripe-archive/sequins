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
	LocalStore         string   `toml:"local_store"`
	RequireSuccessFile bool     `toml:"require_success_file"`
	RefreshPeriod      duration `toml:"refresh_period"`
	MaxParallelLoads   int      `toml:"max_parallel_loads"`
	ContentType        string   `toml:"content_type"`

	S3 s3Config `toml:"s3"`
	ZK zkConfig `toml:"zk"`
}

type s3Config struct {
	Region          string `toml:"region"`
	AccessKeyId     string `toml:"access_key_id"`
	SecretAccessKey string `toml:"secret_access_key"`
}

type zkConfig struct {
	Servers            []string `toml:"servers"`
	ClusterName             string   `toml:"cluster_name"`
	TimeToConverge     duration `toml:"time_to_converge"`
	ProxyTimeout       duration `toml:"proxy_timeout"`
	AdvertisedHostname string   `toml:"advertised_hostname"`
}

func defaultConfig() sequinsConfig {
	return sequinsConfig{
		Root:               "",
		Bind:               ":9599",
		LocalStore:         "/var/sequins/",
		RequireSuccessFile: false,
		RefreshPeriod:      duration{time.Duration(0)},
		ContentType:        "",
		S3: s3Config{
			Region:          "",
			AccessKeyId:     "",
			SecretAccessKey: "",
		},
		ZK: zkConfig{
			Servers:            nil,
			ClusterName:             "/sequins",
			TimeToConverge:     duration{10 * time.Second},
			ProxyTimeout:       duration{100 * time.Millisecond},
			AdvertisedHostname: "",
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
