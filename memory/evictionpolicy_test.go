package memory

import (
	"container/list"
	"fmt"
	"testing"
)

func Test_recordAndEvict(t *testing.T) {
	lruK := LruKReplacer{
		k:             2,
		maxSize:       7,
		metadataStore: make(map[int]LruKFrameAccessMetadata, 0),
		lru:           list.New(),
	}
	// Add six frames to the replacer and set all but the 6th as evictable
	// Now the ordering is [1,2,3,4,5,6]
	lruK.recordAccess(1)
	lruK.recordAccess(2)
	lruK.recordAccess(3)
	lruK.recordAccess(4)
	lruK.recordAccess(5)
	lruK.recordAccess(6)
	assertEqual(t, 0, lruK.size,
		"size of replacer is currently 0 since none of the frames are evictable")
	lruK.setEvictable(1, true)
	lruK.setEvictable(2, true)
	lruK.setEvictable(3, true)
	lruK.setEvictable(4, true)
	lruK.setEvictable(5, true)
	lruK.setEvictable(6, false)
	assertEqual(t, 5, lruK.size,
		"size of replacer is the number of frames that can be evicted, not the total of frames tracked")

	// Record another access for frame 1.
	// Now frame 1 has two accesses total -> [2,3,4,5,6*,1]
	// The replacer knows this because it tracks each frame's access history.
	// All other frames now share the maximum backward k-distance. Since we use timestamps to break ties, where the
	// first to be evicted is the frame with the oldest timestamp, the order of eviction should be [2,3,4,5,1]
	lruK.recordAccess(1)
	assertEqual(t, 2, len(lruK.metadataStore[1].history), fmt.Sprintf("history %+v", lruK.metadataStore[1].history))

	// Evict three pages from the replacer -> [5,6*,1]
	// Should use LRU with respect to the oldest timestamp
	fid, err := lruK.evict()
	assertEqual(t, 2, fid, errMessage(err))
	fid, err = lruK.evict()
	assertEqual(t, 3, fid, errMessage(err))
	fid, err = lruK.evict()
	assertEqual(t, 4, fid, errMessage(err))

	// Now the replacer has evictable frames [5,6*,1].
	assertEqual(t, 2, lruK.size, "")

	// Insert new frames [3,4], and update the access history for 5 and 4.
	// Now the ordering is [5,6*,1,3*,4*] -> [6*,1,3*,4*,5] -> [6*,1,3*,5,4*]
	// Now, the ordering is [6*,1,3*,5,4*].
	lruK.recordAccess(3)
	lruK.recordAccess(4)
	lruK.recordAccess(5)
	lruK.recordAccess(4)
	assertEqual(t, 2, lruK.size, "")

	// Now the ordering is [6*,1,3,5,4]
	lruK.setEvictable(3, true)
	lruK.setEvictable(4, true)
	assertEqual(t, 4, lruK.size, "no. of evictable frames should be 4")

	// Look for a frame to evict. We expect frame 3 to be evicted next -> [6*,1,5,4]
	fid, err = lruK.evict()
	assertEqual(t, 3, fid, errMessage(err))
	assertEqual(t, 3, lruK.size, "no. of evictable frames should be 3")

	// Set 6 to be evictable. 6 Should be evicted next since it has the maximum backward k-distance.
	// Now ordering is [1,5,4]
	lruK.setEvictable(6, true)
	assertEqual(t, 4, lruK.size, "no. of evictable frames should be 4")
	fid, err = lruK.evict()
	assertEqual(t, 6, fid, errMessage(err))
	assertEqual(t, 3, lruK.size, "no. of evictable frames should be 3")

	// Set frame 1 as non-evictable and evict a frame.
	// We expect 5 to be evicted next -> [1*,4]
	lruK.setEvictable(1, false)
	assertEqual(t, 2, lruK.size, "no. of evictable frames should be 2")
	fid, err = lruK.evict()
	assertEqual(t, 5, fid, errMessage(err))
	assertEqual(t, 1, lruK.size, "no. of evictable frames should be 1")

}

func assertEqual[T comparable](t *testing.T, expected T, actual T, msg string) {
	t.Helper()
	if expected == actual {
		return
	}
	if msg != "" {
		t.Errorf("expected (%+v) is not equal to actual (%+v): (%v)", expected, actual, msg)
	} else {
		t.Errorf("expected (%+v) is not equal to actual (%+v)", expected, actual)
	}
}

func errMessage(err error) string {
	if err == nil {
		return ""
	} else {
		return err.Error()
	}
}
