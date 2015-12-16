package index

import (
	"bytes"
	"math"
)

// This goes into the manifest JSON, so make it human readable.
type partitionType string

const (
	noPartitionType   = "none"
	javaHashPartition = "java_string_hash_code"
)

// partitionDetector tries to optimize a set of keys in a dataset
// particular to one file, with the goal of being able to fail fast if
// it knows that a key definitely isn't part of a dataset. It does
// this by tracking the min and max keys, as well as trying to detect
// some of the default partitioning schemes in hadoop. In particular,
// this common case:
//
//     (key.hashCode & Integer.MAX_VALUE) % numFiles
//
// This is pretty easy to test for. To make extra sure we don't get
// false positives, this will only ever exclude keys based on hash
// partitioning once it's seen 1000 keys from a dataset. However, the
// caller must also ensure that both the maximum and minimum keys have
// been passed to update before calling test.
type partitionDetector struct {
	numPartitions int
	numKeys       int

	minKey []byte
	maxKey []byte

	usingHashPartition bool
	partitionNumber    int
}

func newPartitionDetector(numPartitions int) *partitionDetector {
	return &partitionDetector{
		numPartitions:      numPartitions,
		usingHashPartition: true,
		partitionNumber:    -1,
	}
}

func newPartitionDetectorFromManifest(numPartitions int, entry manifestEntry) *partitionDetector {
	pd := &partitionDetector{
		numPartitions: numPartitions,
		// To avoid triggering the "not enough keys" detection
		numKeys: 1000,
		minKey:  entry.IndexProperties.MinKey,
		maxKey:  entry.IndexProperties.MaxKey,
	}

	partitionType := partitionType(entry.IndexProperties.PartitionType)
	if partitionType == javaHashPartition {
		pd.usingHashPartition = true
		pd.partitionNumber = entry.IndexProperties.PartitionNumber
	}

	return pd
}

// update updates the assumptions with a key from the dataset.
func (pd *partitionDetector) update(key []byte) {
	// Update the minimum and maximum keys seen.
	if pd.maxKey == nil || bytes.Compare(key, pd.maxKey) > 0 {
		pd.maxKey = key
	}

	if pd.minKey == nil || bytes.Compare(key, pd.minKey) < 0 {
		pd.minKey = key
	}

	if pd.usingHashPartition {
		// Check for pathological string/Tuple hashCode keys (see giant
		// comment for isPathologicalHashCode).
		hc := hashCode(key)
		if isPathologicalHashCode(hc) {
			return
		}

		pd.numKeys++
		part := int(hc&math.MaxInt32) % pd.numPartitions
		if pd.partitionNumber == -1 {
			pd.partitionNumber = part
		} else if part != pd.partitionNumber {
			pd.usingHashPartition = false
		}
	}
}

// test returns whether a given key is assumped to be part of the
// dataset, prefering to return true unless it is very, very sure that
// it can exclude the key. This can be used to fail quickly on a get
// to an index.
func (pd *partitionDetector) test(key []byte) bool {
	if bytes.Compare(key, pd.minKey) < 0 {
		return false
	} else if bytes.Compare(key, pd.maxKey) > 0 {
		return false
	}

	// Before we check hash partitioning, make sure we have a big enough
	// dataset.
	if pd.numKeys < 1000 || !pd.usingHashPartition {
		return true
	}

	// Check for pathological string/Tuple hashCode keys (see giant
	// comment for isPathologicalHashCode).
	hc := hashCode(key)
	if isPathologicalHashCode(hc) {
		return true
	}

	part := int(hc&math.MaxInt32) % pd.numPartitions
	if part != pd.partitionNumber {
		return false
	}

	return true
}

// updateManifest saves detected partition information to the
// given manifestEntry, to be loaded next time.
func (pd *partitionDetector) updateManifest(m *manifestEntry) {
	m.IndexProperties.MinKey = pd.minKey
	m.IndexProperties.MaxKey = pd.maxKey
	if pd.usingHashPartition {
		m.IndexProperties.PartitionType = javaHashPartition
		m.IndexProperties.PartitionNumber = pd.partitionNumber
	} else {
		m.IndexProperties.PartitionType = noPartitionType
	}
}

// hashCode implements java.lang.String#hashCode.
func hashCode(b []byte) int32 {
	var v int32
	for _, b := range b {
		v = (v * 31) + int32(b)
	}

	return v
}

// isPathologicalHashCode filters out keys where the partition returned by the
// java.lang.String#hashCode would be consistent with other keys, but the
// partition returned by cascading.tuple.Tuple would not. I'm going to explain
// what that means, but first, consider how much you value your sanity.
//
// Still here? Alright.
//
// While "key".hashCode % numPartitions is the default partitioning for
// vanilla hadoop jobs, many people (including yours truly) use scalding,
// which in turn uses cascading. Cascading uses almost exactly the same
// method to partition (pre 2.7, anyway), but with a very slight change.
// The key is wrapped in a cascading.tuple.Tuple, and the hashCode of that
// Tuple is used instead of the hashCode of the key itself. In other words,
// instead of:
//
//     (key.hashCode & Integer.MAX_VALUE) % numPartitions
//
// It's:
//
//     (new Tuple(key).hashCode & Integer.MAX_VALUE) % numPartitions
//
// The Tuple#hashCode implementation combines the hashCodes of the
// constituent parts using the same algorithm as the regular string
// hashCode in a loop, ie:
//
//     (31 * h) + item.hashCode
//
// For single-value tuples, this should be identical to just the hashCode of
// the first part, right? Wrong. It's almost identical. It's exactly 31
// higher, because h starts at 1 instead of 0.
//
// For the vast majority of values, using the string hashCode intsead of the
// Tuple hashCode works fine; the partition numbers are different, but
// consistently so. But this breaks down when the hashCode of the string is
// within 31 of the max int32 value, or within 31 of 0, because we wrap before
// doing the mod. One inconsistent partition number could cause us to assume a
// whole file isn't actually partitioned, or, worse, ignore a key that's
// actually in the dataset but that we skipped over while building a sparse
// index.
//
// Luckily, we can test for these pathological values, and just pretend those
// keys don't exist. We have to also then give them a pass when testing against
// our partitioning scheme.
func isPathologicalHashCode(hc int32) bool {
	return (hc > (math.MaxInt32-32) || (hc > -32 && hc < 0))
}
