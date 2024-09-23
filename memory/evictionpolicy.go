package memory

import (
	"container/list"
	"fmt"
	"math"
	"strings"
	"time"
)

// Interface for an eviction policy.
//
// When the database server needs to free up a frame to make room for a new page,
// it must decide which page to evict from the buffer pool. Eviction Policy decides
// which page/frame to evict out of the buffer pool, when the pool is full.
type EvictionPolicy interface {
	evict([]Frame) (int, error)
}

// LRUKEvictionPolicy implements the LRU-k replacement/eviction policy.
//
// LRUKEvictionPolicy keeps track of when pages are accessed to that
// it can decide which frame to evict when it must make room for a new page.
//
// The LRU-K algorithm evicts a frame whose backward k-distance is the
// maximum of all frames in the buffer pool. Backward k-distance is
// computed as the difference in time between the current timestamp and
// the timestamp of kth previous access.
//
// A frame with less than k historical references is given
// +inf as its backward k-distance. When multiple frames have
// +inf backward k-distance, classical LRU algorithm is used to choose victim.
type LruKFrameMetadata struct {
	history     []int64       // History of last seen K timestamps of this page. Least recent timestamp stored in front.
	isEvictable bool          // true if frame is not pinned
	e           *list.Element // a pointer to the frame id in the lru list
}

type LruKReplacer struct {
	k             int
	maxSize       int                       // the maximum number of frames lruK can track/store
	size          int                       // tracks the number of evictable frames
	metadataStore map[int]LruKFrameMetadata // map of frame id to lru-k frame metadata
	lru           *list.List                // doubly-linked list between frames in ascending access/use order
}

// Evict the frame that has the largest backward k-distance compared
// to all other evictable frames being tracked. Return frame id.
// If no frames can be evicted, return an error.
//
// Calculate backward k-distance as the difference in time between the current
// timestamp and the timestamp of kth previous access
func (lruK *LruKReplacer) evict() (int, error) {
	frameId, err := lruK.hasLargestKInterval()
	if err == nil {
		return frameId, nil
	}
	if strings.HasPrefix(err.Error(), "insufficient historical access data") {
		lruFrameId, err := lruK.getLRUFrame()
		if err == nil {
			lruK.cleanup(lruFrameId)
			return lruFrameId, err
		}
	}
	return -1, err
}

// A frame with fewer than k historical accesses is given +inf as its backward k-distance.
// If multiple frames have +inf backward k-distance, the replacer evicts the frame
// with the earliest overall timestamp.
func (lruK *LruKReplacer) hasLargestKInterval() (int, error) {
	longestK, frameId := -1, -1
	countInf := 0 // count num of backward k distance that are +inf
	for k := range lruK.metadataStore {
		if countInf >= lruK.size {
			break
		}
		fmt.Printf("---frame metadata %+v %+v\n", k, lruK.metadataStore[k])
		kInterval, err := lruK.getBackwardKDist(k)
		fmt.Printf("backward k distance: %+v %+v\n", kInterval, err)
		// Has fewer than k historical accesses is given +inf as its backward k-distance
		if err == nil && kInterval == math.MaxInt {
			countInf++
			continue
		}
		if longestK < kInterval {
			longestK = kInterval
			frameId = k
		}
		countInf++
	}

	// Check if there is multiple frames with +inf backward k distance
	fmt.Printf("count inf: %d\n", countInf)
	if countInf == lruK.size {
		return -1, fmt.Errorf("insufficient historical access data; multiple frames have +inf backward k distance")
	}
	if longestK > -1 {
		return frameId, nil
	}
	return frameId, fmt.Errorf("cannot evict anything -- everything is pinned or no access history")
}

// Calculate the backward k-distance of the frame/page with the given frame id.
// If the frame has fewer than k historical accesses is given +inf as its backward k-distance.
// If the frame is pinned, return an error.
func (lruK *LruKReplacer) getBackwardKDist(frameId int) (int, error) {
	node := lruK.metadataStore[frameId]
	n := len(node.history)
	backwardKDist := math.MaxInt // has fewer than k historical accesses is given +inf
	if node.isEvictable {
		if n >= lruK.k { // contains at least k historical accesses
			backwardKDist = int(node.history[n-1]) - int(node.history[lruK.k-1])
		}
	} else {
		return -1, fmt.Errorf("cannot be evicted -- in use/pinned")
	}
	return backwardKDist, nil
}

// Return the frame id of the frame that has been least recently used.
// If all frames are pinned, return an error.
func (lruK *LruKReplacer) getLRUFrame() (int, error) {
	curr := lruK.lru.Front()
	for curr != nil {
		if frameId, ok := curr.Value.(int); ok {
			fmt.Printf("underlying type of metadata: %+v %+v\n", frameId, ok)
			if lruK.metadataStore[frameId].isEvictable {
				return frameId, nil
			}
		}
		curr = curr.Next()
	}
	return -1, fmt.Errorf("cannot evict anything")
}

// Record that the given frame/page has been accessed at the current timestamp
// This method should be called after a page has been pinned in the buffer pool,
// for the frame/page that is being read from/written to.
func (lruK *LruKReplacer) recordAccess(frameId int) {
	fmt.Printf("frame id: %d, current time: %+v\n", frameId, time.Now().UTC())
	current_timestamp := time.Now().UTC().UnixMilli()
	_, ok := lruK.metadataStore[frameId]
	if ok {
		meta := lruK.metadataStore[frameId]
		meta.history = append(meta.history, current_timestamp)
		lruK.metadataStore[frameId] = meta

		// Move accessed page that is being read /written to the back of the list
		lruK.lru.MoveToBack(meta.e)
	} else {
		e := lruK.lru.PushBack(frameId)
		lruK.metadataStore[frameId] = LruKFrameMetadata{
			history: []int64{current_timestamp},
			e:       e,
		}
		fmt.Printf("inserted into lru: %+v\n", e.Value)
	}
}

// Clear all access history associated with a frame. This method should be
// called only when a page is deleted in the buffer pool.
func (lruK *LruKReplacer) remove(frameId int) {
	m, ok := lruK.metadataStore[frameId]
	if !ok { // page does not exist in metadata store
		return
	}
	if m.isEvictable {
		lruK.size--
	}
	v := lruK.lru.Remove(m.e)
	delete(lruK.metadataStore, frameId)
	fmt.Printf("lruk element removed: %+v\n", v)
}

// Controls whether a frame is evictable or not. It also controls the LRUKReplacer's size.
// When the pin count of a page hits 0, its corresponding frame should be marked as evictable.
func (lruK *LruKReplacer) setEvictable(frameId int, b bool) {
	if m, ok := lruK.metadataStore[frameId]; ok {
		m.isEvictable = b
		lruK.metadataStore[frameId] = m
		if b {
			lruK.size++
		}
	}
}

func (lruK *LruKReplacer) cleanup(frameId int) {
	v := lruK.lru.Remove(lruK.metadataStore[frameId].e)
	delete(lruK.metadataStore, frameId)
	lruK.size--
	fmt.Printf("lruk element removed: %+v\n", v)
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
		((frames[c.hand].refBit == 1) || frames[c.hand].isPinned()); iterations++ {

		frames[c.hand].refBit = 0
		c.hand = (c.hand + 1) % frameSize
	}

	if iterations == 2*frameSize {
		return 0, fmt.Errorf("cannot perform eviction. all frames are pinned")
	}
	toEvict := frames[c.hand].id
	c.hand = (c.hand + 1) % frameSize
	return toEvict, nil
}
