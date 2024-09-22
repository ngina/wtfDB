package memory

import (
	"fmt"
	"testing"
)


func Test_recordAccess(t *testing.T) {
	// Add six frames to the replacer, and set the 
	// 6th frame as evictable

	lruKReplacer := LRUKReplacer{
		k: 2,
		maxSize: 7,
		candidates: make(map[int]LRUKNode, 0),
	}
	lruKReplacer.recordAccess(1)
	lruKReplacer.recordAccess(2)
	lruKReplacer.recordAccess(3)
	lruKReplacer.recordAccess(4)
	lruKReplacer.recordAccess(5)
	lruKReplacer.recordAccess(6)

	assertEqual(t, 0, lruKReplacer.size, 
		"size of replacer is currently 0 since none of the frames have been set to evictable")
	lruKReplacer.setEvictable(1, true)
	lruKReplacer.setEvictable(2, true)
	lruKReplacer.setEvictable(3, true)
	lruKReplacer.setEvictable(4, true)
	lruKReplacer.setEvictable(5, true)
	lruKReplacer.setEvictable(6, false)

	assertEqual(t, 5, lruKReplacer.size, 
		"size of replacer is the number of frames that can be evicted, not the total of frames tracked")

	// Record another access for frame 1. Now frame 1 has two accesses total.
	// The replacer knows this because it tracks the last k historical accesses of a frame/page.
	lruKReplacer.recordAccess(1)
	assertEqual(t, 2, len(lruKReplacer.candidates[1].history), fmt.Sprintf("history %+v", lruKReplacer.candidates[1].history))
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
