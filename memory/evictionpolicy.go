package memory

import "fmt"

// When the database server needs to free up a frame to make room
// for a new page, it must decide which page to evict from the buffer pool
// Eviction Policy decides which page to evict from the buffer pool, when the pool is full.
type EvictionPolicy interface {
	Evict([]Frame) (int, error)
}

// Maintains a single timestamp of when each page was last accessed.
// When the DBMS needs to evict a page, the eviction policy evicts
// the page with the oldest timestamp.
type LRUEvictionPolicy struct {
}

// Implements the clock eviction policy, which works by adding a reference
// bit to each frame, and running the algorithm.
// The clock eviction policy organizes pages in circular buffer with a clock hand that sweeps
// over pages in order. As the hand visits each page, it checks if its ref bit
// is set to 1. If yes, set to zero. If no, then evict.
type ClockEvictionPolicy struct {
	hand int
}

// Called when a page needs to be evicted. Returns frame index of
// page to be evicted. Visits each page, checks if its ref bit is set to 1.
// If yes, set to zero. If no, then evict.
func (c *ClockEvictionPolicy) Evict(frames []Frame) (int, error) {
	frameSize := len(frames)
	var iterations int
	for iterations = 0; iterations < 2*frameSize &&
		((frames[c.hand].refBit == 1) || frames[c.hand].isPinned()); iterations++ {

		frames[c.hand].refBit = 0
		c.hand = (c.hand + 1) % frameSize
	}

	if iterations == 2*frameSize {
		return 0, fmt.Errorf("Cannot perform eviction. All frames are pinned")
	}
	toEvict := frames[c.hand].id
	c.hand = (c.hand + 1) % frameSize
	return toEvict, nil
}
