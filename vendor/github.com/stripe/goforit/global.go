package goforit

import (
	"context"
	"time"
)

var globalGoforit *goforit

func init() {
	globalGoforit = newWithoutInit()
}

func Enabled(ctx context.Context, name string) (enabled bool) {
	return globalGoforit.Enabled(ctx, name)
}

func RefreshFlags(backend Backend) {
	globalGoforit.RefreshFlags(backend)
}

func SetStalenessThreshold(threshold time.Duration) {
	globalGoforit.SetStalenessThreshold(threshold)
}

func Init(interval time.Duration, backend Backend) {
	globalGoforit.init(interval, backend)
}

func Close() error {
	return globalGoforit.Close()
}
