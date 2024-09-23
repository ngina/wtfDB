package memory

import (
	"container/list"
	"fmt"
	"math"
	"strings"
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

/*
LRUKEvictionPolicy implements the LRU-k replacement/eviction policy.

LRUKEvictionPolicy keeps track of when pages are accessed to that
it can decide which frame to evict when it must make room for a new page.

The LRU-K algorithm evicts a frame whose backward k-distance is the
maximum of all frames in the buffer pool. Backward k-distance is
computed as the difference in time between the current timestamp and
the timestamp of kth previous access.
*/
type LruKFrameAccessMetadata struct {
	history     []int64       // Access history of last seen K timestamps of this page. Least recent timestamp stored in front.
	isEvictable bool          // true if frame is not pinned
	e           *list.Element // a pointer to the frame id in the lru list
}

type LruKReplacer struct {
	k             int
	maxSize       int                             // the maximum number of frames lruK can track/store
	size          int                             // tracks the number of evictable frames
	metadataStore map[int]LruKFrameAccessMetadata // map of frame id to lru-k frame metadata
	lru           *list.List                      // doubly-linked list between frames in ascending access/use order
}

/*
Evict the frame that has the largest backward k-distance compared
to all other evictable frames being tracked. Return frame id.
If no frames can be evicted, return an error.

A frame with less than k historical accesses is given +inf as its backward k-distance.
If multiple frames have +inf backward k-distance, use LRU algorithm to evict a frame with
the earliest timestamp.

Successful eviction of a frame decrements the size of replacer and removes the frame's
access history.
*/
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

/*
Record that the given frame/page has been accessed at the current timestamp.
Create a new metadata entry for access history, if the page does not exist in the metadata store.
If the page exists in the metadata store, the current access timestamp is appended to the access history list.

This method should be called after a page has been pinned in the buffer pool,
for the frame/page that is being read from/written to.
*/
func (lruK *LruKReplacer) recordAccess(frameId int) {
	fmt.Printf("frame id: %d, current time: %+v\n", frameId, time.Now().UTC())
	current_timestamp := time.Now().UTC().UnixMilli()
	_, ok := lruK.metadataStore[frameId]
	if ok {
		m := lruK.metadataStore[frameId]
		m.history = append(m.history, current_timestamp)
		lruK.metadataStore[frameId] = m

		// Move accessed page that is being read /written to the back of the list
		lruK.lru.MoveToBack(m.e)
		fmt.Printf("updated in lru: %+v\n", m.e.Value)
	} else {
		e := lruK.lru.PushBack(frameId)
		lruK.metadataStore[frameId] = LruKFrameAccessMetadata{
			history: []int64{current_timestamp},
			e:       e,
		}
		fmt.Printf("inserted into lru: %+v\n", e.Value)
	}
}

// A frame with fewer than k historical accesses is given +inf as its backward k-distance.
// If multiple frames have +inf backward k-distance, the replacer evicts the frame
// with the earliest overall timestamp.
func (lruK *LruKReplacer) hasLargestKInterval() (int, error) {
	fmt.Println("#### beginning hasLargestKInterval()")
	longestK, frameId := -1, -1
	countInf := 0 // count num of backward k distance that are +inf
	for k := range lruK.metadataStore {
		fmt.Printf("--frame metadata: %+v %+v\n", k, lruK.metadataStore[k])
		// Exit loop early if there exists at least half the number of evictable pages with +inf backward k-distance
		if countInf >= lruK.size/2 {
			break
		}
		kInterval, err := lruK.getBackwardKDistance(k)
		fmt.Printf("--backward k distance: %+v %+v\n", kInterval, err)
		// Has fewer than k historical accesses is given +inf as its backward k-distance
		if err == nil && kInterval == math.MaxInt {
			// move frames with +inf backward k distance to the front
			countInf++
			continue
		}
		if longestK < kInterval {
			longestK = kInterval
			frameId = k
		}
	}

	// Check if there is multiple frames with +inf backward k distance
	fmt.Printf("count inf: %d\n", countInf)
	if countInf > 0 && countInf <= lruK.size {
		fmt.Printf("count inf is: %d\n", countInf)
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
func (lruK *LruKReplacer) getBackwardKDistance(frameId int) (int, error) {
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
	fmt.Println("## beginning getLRUFrame()")
	curr := lruK.lru.Front()
	for curr != nil {
		if frameId, ok := curr.Value.(int); ok {
			if lruK.metadataStore[frameId].isEvictable && len(lruK.metadataStore[frameId].history) < lruK.k {
				fmt.Printf("lru selected candidate: %d\n", frameId)
				return frameId, nil
			}
		}
		curr = curr.Next()
	}
	return -1, fmt.Errorf("cannot evict anything")
}

/*
Remove an evictable frame from replacer, along with its access history.
Clear all access history associated with a frame and decrement replacer's size
if removal is successful.

This is different from evicting a frame, which always removes the frame
with largest backward k-distance. This function removes specified frame id,
no matter what its backward k-distance is.

If Remove is called on a non-evictable frame, return an error.

If specified frame is not found in metadata store, directly return.

This method should be called only when a page is deleted in the buffer pool.
*/
func (lruK *LruKReplacer) remove(frameId int) error {
	m, ok := lruK.metadataStore[frameId]
	if !ok {
		return nil
	}
	if !m.isEvictable {
		return fmt.Errorf("attempting to remove a non-evictable frame")
	}
	v := lruK.lru.Remove(m.e)
	delete(lruK.metadataStore, frameId)
	lruK.size--
	fmt.Printf("remove access history for element: %+v\n", v)
	return nil
}

// Controls whether a frame is evictable or not. It also controls the replacers's size.
// Decrements replacer's size when marking an evictable frame as non-evictable and 
// increments size when marking a non-evictable frame as evictable.
// When the pin count of a page hits 0, its corresponding frame should be marked as evictable.
func (lruK *LruKReplacer) setEvictable(frameId int, setEvictable bool) {
	if m, ok := lruK.metadataStore[frameId]; ok {
		if m.isEvictable && !setEvictable {
			m.isEvictable = setEvictable
			lruK.metadataStore[frameId] = m
			lruK.size--
		}
		if !m.isEvictable && setEvictable {
			m.isEvictable = setEvictable
			lruK.metadataStore[frameId] = m
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
