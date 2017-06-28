package main

import (
	"regexp"
)

var validPathRegexp = regexp.MustCompile("^[-_a-zA-Z0-9]+$")

// Given a list of potential path components, filter out any that
// contain invalid characters.
//
// Sequins only supports datasets that are alphanumeric + hyphen +
// underscore.
func filterPaths(paths []string) []string {
	filtered := make([]string, 0, len(paths))

	for _, p := range paths {
		if validPathRegexp.MatchString(p) {
			filtered = append(filtered, p)
		}

	}

	return filtered
}
