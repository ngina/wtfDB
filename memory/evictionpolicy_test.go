package memory

import (
	"container/list"
	"fmt"
	"testing"
)

func Test_recordAndEvict(t *testing.T) {
	// Add six frames to the replacer, and set the
	// 6th frame as evictable

	lruK := LruKReplacer{
		k:             2,
		maxSize:       7,
		metadataStore: make(map[int]LruKFrameMetadata, 0),
		lru:           list.New(),
	}
	lruK.recordAccess(1)
	lruK.recordAccess(2)
	lruK.recordAccess(3)
	lruK.recordAccess(4)
	lruK.recordAccess(5)
	lruK.recordAccess(6)

	assertEqual(t, 0, lruK.size,
		"size of replacer is currently 0 since none of the frames have been set to evictable")
	lruK.setEvictable(1, true)
	lruK.setEvictable(2, true)
	lruK.setEvictable(3, true)
	lruK.setEvictable(4, true)
	lruK.setEvictable(5, true)
	lruK.setEvictable(6, false)

	assertEqual(t, 5, lruK.size,
		"size of replacer is the number of frames that can be evicted, not the total of frames tracked")

	// Record another access for frame 1. Now frame 1 has two accesses total.
	// The replacer knows this because it tracks the last k historical accesses of a frame/page.
	// All other frames now share the maximum backward k-distance. Since we use timestamps to break ties, where the
	// first to be evicted is the frame with the oldest timestamp, the order of eviction should be [2, 3, 4, 5, 1]
	lruK.recordAccess(1)
	assertEqual(t, 2, len(lruK.metadataStore[1].history), fmt.Sprintf("history %+v", lruK.metadataStore[1].history))

	// Evict three pages from the replacer.
	// Should use LRU with respect to the oldest timestamp, or the least recently used frame.
	fid, err := lruK.evict()
	assertEqual(t, 2, fid, getErrMessage(err))
	fid, err = lruK.evict()
	assertEqual(t, 3, fid, getErrMessage(err))
	fid, err = lruK.evict()
	assertEqual(t, 4, fid, getErrMessage(err))

	// Now the replacer has the frames [5, 1].
	assertEqual(t, 2, lruK.size, "")
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

func getErrMessage(err error) string {
	if err == nil {
		return ""
	} else {
		return err.Error()
	}
}
