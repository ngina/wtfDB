package memory

import (
	"container/list"
	"fmt"
	"math"
	"time"
)

/*
Interface for an eviction policy.

When the database server needs to free up a frame to make room for a new page,
it must decide which page to evict from the buffer pool. Eviction Policy decides
which page/frame to evict out of the buffer pool, when the pool is full.
*/
type EvictionPolicy interface {
	evict([]Frame) (int, error)
}

// Implements the clock eviction policy, which works by adding a reference (ref)
// bit to each frame. The ref bit determines if the frame has been accessed since the last time
// the system checked.
//
// The clock eviction policy organizes pages in circular buffer with a clock hand that sweeps
// over pages in order. As the hand visits each page, it checks if its ref bit
// is set to 1. If yes, set to zero. If no, then evict.
type ClockEvictionPolicy struct {
	hand int
}

// Called when a page needs to be evicted. Returns frame index of
// page to be evicted. Visits each page, checks if its ref bit is set to 1.
// If yes, set to zero. If no, then evict.
func (c *ClockEvictionPolicy) evict(frames []Frame) (int, error) {
	frameSize := len(frames)
	var iterations int
	for iterations = 0; iterations < 2*frameSize &&
		((frames[c.hand].refBit == true) || frames[c.hand].isPinned()); iterations++ {

		frames[c.hand].refBit = false
		c.hand = (c.hand + 1) % frameSize
	}

	if iterations == 2*frameSize {
		return 0, fmt.Errorf("cannot perform eviction. all frames are pinned")
	}
	toEvict := frames[c.hand].id
	c.hand = (c.hand + 1) % frameSize
	return toEvict, nil
}
