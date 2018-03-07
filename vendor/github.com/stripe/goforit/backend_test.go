package goforit

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestParseFlagsCSV(t *testing.T) {
	t.Parallel()

	filename := filepath.Join("fixtures", "flags_example.csv")

	type testcase struct {
		Name     string
		Filename string
		Expected []Flag
	}

	cases := []testcase{
		{
			Name:     "BasicExample",
			Filename: filepath.Join("fixtures", "flags_example.csv"),
			Expected: []Flag{
				{
					"go.sun.money",
					0,
				},
				{
					"go.moon.mercury",
					1,
				},
				{
					"go.stars.money",
					0.5,
				},
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.Name, func(t *testing.T) {
			f, err := os.Open(filename)
			assert.NoError(t, err)
			defer f.Close()

			flags, _, err := parseFlagsCSV(f)

			assertFlagsEqual(t, flagsToMap(tc.Expected), flags)
		})
	}
}

func TestParseFlagsJSON(t *testing.T) {
	t.Parallel()

	filename := filepath.Join("fixtures", "flags_example.json")

	type testcase struct {
		Name     string
		Filename string
		Expected []Flag
	}

	cases := []testcase{
		{
			Name:     "BasicExample",
			Filename: filepath.Join("fixtures", "flags_example.json"),
			Expected: []Flag{
				{
					"sequins.prevent_download",
					0,
				},
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.Name, func(t *testing.T) {
			f, err := os.Open(filename)
			assert.NoError(t, err)
			defer f.Close()

			flags, _, err := parseFlagsJSON(f)

			assertFlagsEqual(t, flagsToMap(tc.Expected), flags)
		})
	}
}

func TestMultipleDefinitions(t *testing.T) {
	t.Parallel()

	const repeatedFlag = "go.sun.money"
	const lastValue = 0.7

	backend := BackendFromFile(filepath.Join("fixtures", "flags_multiple_definitions.csv"))
	g, _ := testGoforit(0, backend)
	g.RefreshFlags(backend)

	flag := g.flags[repeatedFlag]
	assert.Equal(t, flag, Flag{repeatedFlag, lastValue})

}
