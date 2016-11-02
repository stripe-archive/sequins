package blocks

import "math"

// hashCode implements java.lang.String#hashCode.
func hashCode(b []byte) int32 {
	var v int32
	for _, b := range b {
		v = (v * 31) + int32(b)
	}

	return v
}

// KeyPartition grabs the partition for a key. Most of the time, this will
// match the way hadoop shuffles keys to reducers. It sometimes returns a second
// partition where the key may be; see the comment for
// alternatePathologicalKeyPartition below.
func KeyPartition(key []byte, totalPartitions int) (int, int) {
	hc := hashCode(key)
	normal := int(hashCode(key)&math.MaxInt32) % totalPartitions
	alternate := alternatePathologicalKeyPartition(hc, totalPartitions)

	if alternate == -1 {
		return normal, normal
	} else {
		return normal, alternate
	}
}

// alternatePathologicalKeyPartition can tell you about keys where the partition
// returned by java.lang.String#hashCode would be consistent with other keys,
// but the partition returned by cascading.tuple.Tuple#hashCode would not, and
// provide an alternate partition that the key could possibly be in. I'm going
// to explain what that means, but first, consider how much you value your
// sanity.
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
// For the vast majority of values, using the string hashCode instead of the
// Tuple hashCode works fine; the partition numbers are different, but
// consistently so. But this breaks down when the hashCode of the string is
// within 31 of the max int32 value, or within 31 of 0, because we wrap before
// doing the mod. One inconsistent partition number could cause us to assume a
// whole file isn't actually partitioned, or, worse, ignore a key that's
// actually in the dataset but that we skipped over heuristically.
//
// Fortunately, we can handle this case correctly. If the hashcode looks like
// it would be pathological, then we can figure out what the cascading hashcode
// would have been; and from there what the regular hashcode of the other keys
// with that Tuple hash could would be. Which gives us another partition to
// look for it.
//
// If the key is not pathological, alternatePathologicalKeyPartition returns -1.
func alternatePathologicalKeyPartition(hc int32, totalPartitions int) int {
	if hc > (math.MaxInt32 - 32) {
		tupleHC := int((hc+31)&math.MaxInt32) % totalPartitions
		rotate := (totalPartitions - (31 % totalPartitions))
		return (tupleHC + rotate) % totalPartitions
	} else {
		return -1
	}
}
