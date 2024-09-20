package memory

import (
	"fmt"
	"time"
	"container/list"
)

// Interface for an eviction policy.
//
// When the database server needs to free up a frame to make room
// for a new page, it must decide which page to evict from the buffer pool.
// Eviction Policy decides which page/frame to evict out of the buffer pool,
// when the pool is full.
type EvictionPolicy interface {
	evict([]Frame) (int, error)
}

// LRUKEvictionPolicy implements the LRU-k replacement policy.
//
// The LRU-K algorithm evicts a frame whose backward k-distance is the
// maximum of all frames in the buffer pool. Backward k-distance is
// computed as the difference in time between the current timestamp and
// the timestamp of kth previous access.
//
// A frame with less than k historical references is given
// +inf as its backward k-distance. When multiple frames have
// +inf backward k-distance, classical LRU algorithm is used to choose victim.
//
// Tracks the history of the last K references to each page as timestamps
// and computes the interval between subsequent accesses. It uses this history
// to estimate the next time that page is going to be accessed.

type LRUKNode struct {
	history     []int64
	k           int
	frameId     int
	isEvictable bool
}

type LRUKEvictionPolicy struct {
	k          int
	size       int              // tracks the number of evictable frames
	candidates map[int]LRUKNode // map of frame id to lru-k node
	leastRecentlyUsed list.List // doubly-linked list between frames in ascending access/use order
}

// Evict the frame that has the largest backward k-distance compared
// to all other evictable frames being tracked. Return frame id.
// If no frames can be evicted, return an error.
//
// Calculate backward k-distance as the difference in time between the current
// timestamp and the timestamp of kth previous access
func (lru *LRUKEvictionPolicy) evict(frames []Frame) (int, error) {
	frameId, err := lru.hasLargestKDist()
	if err != nil {
		return frameId, nil
	}
	curr := lru.leastRecentlyUsed.Front()
	for curr != nil && (!curr.Value.(LRUKNode).isEvictable) {
		curr = curr.Next() 
	}
	return curr.Value.(LRUKNode).frameId, nil
}

func (lru *LRUKEvictionPolicy) hasLargestKDist() (int, error) {
	longestK := -1
	frameId := -1
	for i := 0; i < len(lru.candidates); i++ {
		node := lru.candidates[i]
		n := len(node.history)
		if node.isEvictable && n >= lru.k {
			kInterval := int(node.history[n-1]) - int(node.history[lru.k-1])
			if longestK < kInterval {
				longestK = kInterval
				frameId = node.frameId
			}
		}
	}
	if longestK > -1 {
		return frameId, nil
	}
	return frameId, fmt.Errorf("cannot evict anything -- everything is pinned or no access history")
}

// Record that the given frame has been accessed at the current timestamp
// This method should be called after a page has been pinned in the buffer pool,
// when the frame/page that is being read from/written to.
func (lru *LRUKEvictionPolicy) recordAccess(id int) {
	current_timestamp := time.Now().UTC().UnixMilli()
	node, ok := lru.candidates[id]
	if ok {
		node.history = append(node.history, current_timestamp)
	} else {
		node = LRUKNode{
			frameId: id,
			history: []int64{current_timestamp},
			k:       lru.k,
		}
		lru.candidates[id] = node
	}
	// Move accessed page that is being read to/written from 
	// to the back of the list
	lru.leastRecentlyUsed.MoveToBack(&list.Element{Value: node})
}

func (lru *LRUKEvictionPolicy) initLRUKNode(id int) {
	current_timestamp := time.Now().UTC().UnixMilli()
	lru.candidates[id] = LRUKNode{
			frameId: id,
			history: []int64{current_timestamp},
			k:       lru.k,
	}
	lru.leastRecentlyUsed.PushBack(lru.candidates[id])
}  

// Clear all access history associated with a frame. This method should be
// called only when a page is deleted in the buffer pool.
func (lru *LRUKEvictionPolicy) remove(id int) {
	node, ok := lru.candidates[id]
	if ok {
		if !node.isEvictable {
			lru.setEvictable(id, true)
		}
	}
	delete(lru.candidates, id)
	lru.leastRecentlyUsed.Remove(&list.Element{Value: node})
}

// Controls whether a frame is evictable or not. It also controls the LRUKReplacer's size.
// When the pin count of a page hits 0, its corresponding frame should be marked as evictable.
func (lru *LRUKEvictionPolicy) setEvictable(id int, b bool) {
	if node, ok := lru.candidates[id]; ok {
		node.isEvictable = b
		if b {
			lru.size++
		} else {
			lru.size--
		}
	}
}

// Implements the clock eviction policy, which works by adding a reference (ref)
// bit to each frame. The ref bit determines if the frame has been accessed since the last time
// the system checked.
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
