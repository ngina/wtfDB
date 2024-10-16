package memory

import (
	"fmt"
	"log"
	"slices"
	"wtfDB/io"
)

/*
The BufferPoolManager is responsible for moving physical pages of data between disk and memory.
It manages frames of memory and related metadata. A frame represents in memory a physical page on disk.

Responsibilities:
* The buffer pool is responsible for moving physical pages of data back and forth from buffers in
main memory to persistent storage.

* It also behaves as a cache, keeping frequently used pages
in memory for faster access, and evicting unused or cold pages back out to storage.

* It allows a DBMS to support databases that are larger than the amount of memory available to the system.
Consider a computer with 1 GB of memory (RAM). If we want to manage a 2 GB database, a buffer pool manager
gives us the ability to interact with this database without needing to fit its entire contents in memory.
*/
type BufferPoolManager struct {
	frames       []*Frame    // list of frame metadata of the frames that the buffer pool manages
	pageToFrame  map[int]int // buffer manager hash table on page id to frame id
	nextPageId   int         // the next page id to be allocated -- monotonically increasing counter
	freeFrames   []int       // list of free frames that do not hold any page data
	size         int         // the number of frames the buffer pool manages
	diskManager  io.DiskManager
	lrukreplacer *LruKReplacer
}

// Buffer frame metadata stores metadata about a frame / page in memory.
// It contains a pointer/index to the actual frame / page data in the buffer.
type FrameMetadata struct {
	Id       int  // The frame id/index of the frame in the buffer pool
	PageId   int  // page id
	IsDirty  bool // flag to track whether a page has been modified/written
	refBit   bool // allows page to be referenced once before it is eligible for eviction
	pinCount int  // number of tasks/queries that are working with the page in memory
}

// A buffer frame store metadata and page data.
type Frame struct {
	FrameMetadata
	Data []byte // page data
}

const InvalidPageId = int(-1)

func newFrame(i int) *Frame {
	return &Frame{
		FrameMetadata: FrameMetadata{
			Id:     i,
			PageId: InvalidPageId,
		},
		Data: make([]byte, io.PageSize), // buffer frame size determined by page size
	}
}

// Pin pins a buffer frame to indicate the page is "in use".
// A frame's page cannot be evicted while pinned.
func (f *Frame) IsPinned() bool {
	return f.pinCount > 0
}

func (m *BufferPoolManager) Pin(f *Frame) {
	// fmt.Printf("Buffer manager: pinning frame: frameId=%d, pinCount=%d\n", f.Id, f.pinCount)
	f.pinCount++
	// fmt.Printf("Buffer manager: updated pin count: %d\n", f.pinCount)
	m.lrukreplacer.recordAccess(f.Id)
	m.lrukreplacer.setEvictable(f.Id, false)
}

// Unpin buffer frame.
func (m *BufferPoolManager) Unpin(f *Frame) {
	// fmt.Printf("Buffer manager: unpin frame: frameId=%d, pinCount=%d\n", f.Id, f.pinCount)
	if f.pinCount <= 0 {
		return
	}
	f.pinCount--
	m.lrukreplacer.setEvictable(f.Id, f.pinCount == 0)
	// fmt.Printf("Buffer manager: unpinned frame: frameId=%d, pinCount=%d, isEvictable=%v\n", f.Id, f.pinCount, m.lrukreplacer.metadataStore[f.Id].isEvictable)
}

func (f *Frame) ZeroBuffer() {
	for i := range f.Data {
		f.Data[i] = 0
	}
}

func NewBufferPoolManager(dsm io.DiskManager, size int) *BufferPoolManager {
	freeFrames := make([]int, size)
	frames := make([]*Frame, size)
	for i := range size {
		freeFrames[i] = i
		frames[i] = newFrame(i)
	}
	return &BufferPoolManager{
		frames:       frames,
		freeFrames:   freeFrames, // todo: maybe should be a queue ??/
		pageToFrame:  make(map[int]int),
		diskManager:  dsm,
		lrukreplacer: NewLruKReplacer(),
		size:         size,
	}
}

/*
Creates a new pinned page in memory.
The page is loaded onto a buffer frame.
*/
func (m *BufferPoolManager) GetNewPageFrame() (*Frame, error) {
	return m.GetPage(m.newPage())
}

/*
Creates a new page that is loaded onto a buffer frame.
A new page is allocated via the nextPageId counter.
Returns the page id of the newly created page or an InvalidPageId if unable to create a new page.
The page is not pinned in memory.
*/
func (m *BufferPoolManager) newPage() int {
	newPageId := m.nextPageId
	m.nextPageId++

	// need to persist new page to a buffer frame
	if len(m.freeFrames) > 0 {
		frameIdx := m.freeFrames[0]
		m.freeFrames = slices.Delete(m.freeFrames, 0, 1) // todo: maybe use a queue/stack ?
		m.pageToFrame[newPageId] = frameIdx
		m.frames[frameIdx].PageId = newPageId
	} else {
		// no available frames. evict a frame
		isEvicted, i := m.evict()
		if !isEvicted {
			return InvalidPageId // cannot create a new page
		}
		m.frames[i].FrameMetadata = FrameMetadata{
			Id:     i,
			PageId: newPageId,
		}
		m.pageToFrame[newPageId] = i
	}
	return newPageId
}

func (m *BufferPoolManager) DeletePage(pageId int) (bool, error) {
	return false, nil
}

// GetPage returns a Page object that represents the page with the given page number
// in the buffer pool. If the page is not in the buffer pool, it is read from disk
// and placed in a frame in the buffer pool. The page is pinned in memory until it is
// unpinned by the requestor(caller), at which point it is eligible for eviction
// by the buffer pool's eviction policy.
func (m *BufferPoolManager) GetPage(pageId int) (*Frame, error) {
	f, err := m.getPageFrame(pageId)
	if err != nil {
		return nil, err
	}
	m.Pin(f)
	return f, nil
}

func (m *BufferPoolManager) WritePage(pageId int, contents []byte) error {
	return nil
}

/*
Returns a buffer frame with the specified page. This method also pins the page.

This method handles 3 cases:
  - Case 1. the page exists in memory, therefore no need for additional i/o to fetch page
  - Case 2: the page does not exist in memory and there exists available/free buffer frames in memory,
    in which case this method assigns the specified page to a free buffer frame
  - Case 3: the page does not exist in and memory/buffer is full, the buffer manager therefore has to evict
    a page in memory, using lru-k to find a candidate frame for eviction, in order to bring
    in the specified page into a frame.
*/
func (m *BufferPoolManager) getPageFrame(pageId int) (*Frame, error) {
	// case 1: page is loaded in memory
	if i, ok := m.pageToFrame[pageId]; ok {
		frame := m.frames[i]
		return frame, nil
	}

	// handles case 2 and 3 when the page is not found in memory
	// case 2: page is not in memory, and there exists free frame/s
	if len(m.freeFrames) > 0 {
		i := m.freeFrames[0]
		frame := m.frames[i]
		m.pageToFrame[pageId] = i
		frame.PageId = pageId
		m.diskManager.ReadPage(pageId, frame.Data)
		return frame, nil
	}

	// case 3: page is not in memory, and memory/buffer is full
	evicted, i := m.evict()
	if !evicted {
		return nil, fmt.Errorf("internal error: memory is full - retry")
	}
	frame := m.frames[i]
	frame.FrameMetadata = FrameMetadata{
		Id:     i,
		PageId: pageId,
	}
	m.pageToFrame[pageId] = i
	m.diskManager.ReadPage(pageId, frame.Data) // read new page into frame
	return frame, nil
}

// Returns true if a page was successfully evicted from the buffer pool. If true,
// the index of the evicted/free buffer frame is returned, otherwise -1.
func (m *BufferPoolManager) evict() (bool, int) {
	i, err := m.lrukreplacer.evict() // get candidate pool to evict
	if err != nil {
		log.Println("cannot perform eviction")
		log.Println("memory is full - retry")
		return false, -1
	}
	frame := m.frames[i]
	if !m.FlushPage(frame.PageId) {
		log.Printf("unable to flush data to disk for page id: %d - retry", frame.PageId)
		return false, -1
	}
	delete(m.pageToFrame, frame.PageId) // a frame can only map to a single page
	return true, i
}

/*
Flush page data out to disk.

Writes a page's data out to disk if it has been modified in memory/buffer.
If the given page is not in memory, this function will return false. If there
is an error returned by the disk manager, the function will return false.
Returns true, if the frame/page was not modified or the page was successfully
written to disk.
*/
func (m *BufferPoolManager) FlushPage(pageId int) bool {
	frameId, ok := m.pageToFrame[pageId]
	if !ok {
		log.Printf("page id %d not found in buffer", pageId)
		return false
	}
	f := m.frames[frameId]
	if !f.IsDirty {
		return true
	}
	err := m.diskManager.WritePage(int(pageId), f.Data)
	if err != nil {
		log.Printf("error flushing page to disk: %d", f.PageId)
		return false
	}
	f.IsDirty = false
	return true
}

// Flushes all page data that is in memory to disk
// Fixme: needs to perform some sanity checks
func (m *BufferPoolManager) FlushAllPages() bool {
	allFlushed := true
	for pageId, _ := range m.pageToFrame {
		allFlushed = allFlushed && m.FlushPage(pageId)
	}
	return allFlushed
}
