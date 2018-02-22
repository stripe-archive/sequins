package goforit_test

import (
	"context"
	"fmt"
	"time"

	"github.com/stripe/goforit"
)

func Example() {
	ctx := context.Background()

	// flags.csv contains comma-separated flag names and sample rates.
	// See: fixtures/flags_example.csv
	backend := goforit.BackendFromFile("flags.csv")
	goforit.Init(30*time.Second, backend)

	if goforit.Enabled(ctx, "go.sun.mercury") {
		fmt.Println("The go.sun.mercury feature is enabled for 100% of requests")
	}
	// Same thing.
	if goforit.Enabled(nil, "go.sun.mercury") {
		fmt.Println("The go.sun.mercury feature is enabled for 100% of requests")
	}

	if goforit.Enabled(ctx, "go.stars.money") {
		fmt.Println("The go.stars.money feature is enabled for 50% of requests")
	}
}
