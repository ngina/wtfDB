package memory

import (
	"fmt"
	"time"
	"wtfDB/io"
)

// BufferPool is a data structure page-sized extenstions called Frames. This data structure
// provides the high levels of the system the illusion of addressing and
// modifying pages as though they are disk pages that exist in RAM (in memory).
//
// BufferPool manages a set of paged-size extension memory called frames.
//
// BufferPool moves pages from the disk into and out of frames into the buffer pool.
type BufferPool struct {
	frames         []Frame     // large range of memory created a server time which is abstracted as frames
	pageToFrame    map[int]int // buffer manager hash table on page id to frame id
	freeFrameIndex int         // index of first free fram
	diskManager    io.DiskManager
	EvictionPolicy
}

// Buffer frame contains information about the loaded page,
// wrapped around the underlying byte array.
type Frame struct {
	id         int
	pageNumber int
	isDirty    bool      // page has been modified/written
	refBit     bool      // allows page to be referenced once before it is eligible for eviction
	pinCount   int       // number of tasks/queries that are working with the page in memory
	contents   []byte    // page contents
	lastUsed   time.Time // in unix micro (int64)
}

// Pin pins a buffer frame to indicate the page is "in use".
// A frame's page cannot be evicted while pinned.
func (f *Frame) isPinned() bool {
	return f.pinCount > 0
}

func (f *Frame) pin() {
	f.pinCount++
}

// Unpin buffer frame.
func (f *Frame) unpin() error {
	if f.pinCount == 0 {
		return fmt.Errorf("frame is unpinned")
	}
	f.pinCount--
	return nil
}

// GetPage returns a Page object that represents the page with the given page number
// in the buffer pool. If the page is not in the buffer pool, it is read from disk
// and placed in a frame in the buffer pool. The page is pinned in memory until it is
// unpinned by the requestor(caller), at which point it is eligible for eviction
// by the buffer pool's eviction policy.
func (p BufferPool) GetPage(pageNum int) (Page, error) {
	f := p.getPageFrame(pageNum)
	return Page{
		pageId: pageNum,
		buf:    f.contents,
	}, nil
}

func (b BufferPool) WritePage(pageNumber int, contents []byte) error {
	return nil
}

// Returns a buffer frame with the specified page. Returns existing buffer frame if
// the page already exists in the buffer pool, that is, the page has been loaded in memory.
// Otherwise, it fetches the page from disk and loads it into an available buffer frame.
// Pins the buffer frame.
func (p BufferPool) getPageFrame(pageNumber int) Frame {
	// case 1: page is loaded in memory
	if frameIndex, ok := p.pageToFrame[pageNumber]; ok {
		frame := p.frames[frameIndex]
		frame.pin()
		return frame
	}

	// case 2. page is not in memory, therefore it does not exist in buffer pool
	// 1.1 send request to disk manager to fetch page from disk
	// 1.11 fetches page into empty frame
	// 1.12 fetch page into existing frame: replace an existing page using an eviction policy
	// 1.121 case 1. handle dirty pages.
	// -- if frame is dirty write current page to disk, mark frame clean
	// -- read requested page into frame from disk manager
	// -- pin page and return pointer to page to the requester
	// -- requestor sets dirty page if modified
	// -- requestore unpins page when done modifying
	// 1.122 case 2. page replacement

	// Get the next free frame before deciding to evict an existing frame
	var availableFrame Frame
	if p.freeFrameIndex >= 0 && p.freeFrameIndex < len(p.frames) {
		availableFrame = p.frames[p.freeFrameIndex]
	} else {
		// Evict a buffer frame if the buffer pool is full and there are no free buffer frames
		evictedFrame, err := p.Evict()
		if err != nil {
			return Frame{}
		}
		delete(p.pageToFrame, int(evictedFrame.pageNumber))
		availableFrame = evictedFrame
	}

	p.flush(availableFrame)
	availableFrame.pageNumber = pageNumber
	p.diskManager.ReadPage(pageNumber, availableFrame.contents)
	availableFrame.pin()
	return availableFrame
}

func (p BufferPool) flush(f Frame) error {
	if !f.isDirty {
		return nil
	}
	err := p.diskManager.WritePage(int(f.pageNumber), f.contents)
	if err != nil {
		return fmt.Errorf("error writing page number [%d] to disk", f.pageNumber)
	}
	f.isDirty = false
	return nil
}
