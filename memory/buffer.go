package memory

import (
	"fmt"
	"wtfDB/io"
)

// Buffer frame contains information about the loaded page,
// wrapped around the underlying byte array.
type Frame struct {
	id         uint32
	pageNumber uint32
	isDirty    bool
	refBit     bool
	pinCount   uint32
	contents   []byte
}

// Pin pins a buffer frame.
// A frame's page cannot be evicted while pinned.
func (f Frame) pin() {
	f.pinCount++
}

// Unpin buffer frame.
func (f Frame) unpin() error {
	if f.pinCount == 0 {
		return fmt.Errorf("frame is unpinned")
	}
	f.pinCount--
	return nil
}

func (f Frame) flush(buf Buffer) {
	if !f.isDirty {
		return
	}

	err := buf.diskManager.writePage(f.pageNumber, f.contents)
	if err != nil {
		fmt.Errorf("error writing page number [%d] to disk", f.pageNumber)
	}
	f.isDirty = false
}

func (f Frame) readBytes() {

}

func (f Frame) writeBytes() {

}

// Buffer provides the high levels of the system the illusion
// of addressing and modifying pages disk pages that exist in RAM (in memory).
//
// A BufferManager manages a buffer pool, a set of paged-size extension memory called frames.
//
// BufferManager moves pages from the disk into and out of frames in the buffer pool
type Buffer struct {
	pool           []Frame        // large range of memory created a server time which is abstracted as frames
	metadata       map[int]uint16 // buffer manager hash table on page id to frame id
	freeFrameIndex int            // index of first free fram
	diskManager    io.DiskManager
}

func allocate() (Buffer, error) {
	return Buffer{
		pool:           make([]Frame, 3),
		metadata:       make(map[int]uint16),
		freeFrameIndex: 0,
		diskManager:    io.DefaultDiskManager{},
	}, nil
}

// If page is in buffer pool, pin page and return page address to requestor.
// If page is not in buffer pool, fetch page from disk manager,
// pin it and return page address to requestor.
func (b Buffer) read(pageNumber int) (int, error) {
	return 0, nil
}

func (b Buffer) write(pageNumber int) error {
	return nil
}
