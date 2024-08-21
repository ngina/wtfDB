package memory

type Frame struct {
	id uint16
	pageNumber uint32
	dirty bool
	refBit bool
	pinCount uint32
}

// Buffer provides the high levels of the system the illusion 
// of addressing and modifying pages disk pages that exist in RAM (in memory).
//
// A BufferManager manages a buffer pool, a set of paged-size extension memory called frames.
//
// BufferManager moves pages from the disk into and out of frames in the buffer pool
type Buffer struct {
	pool []Frame // large range of memory created a server time which is abstracted as frames
	metadata map[int]uint16 // buffer manager hash table on pageId to frame idi
}

func allocate() (Buffer, error) {
	return Buffer{}, nil
}

// If page is in buffer pool, pin page and return page address to requestor.
// If page is not in buffer pool, fetch page from disk manager, 
// pin it and return page address to requestor.
func (b Buffer) read(pageNumber int) (int, error) {
	return 0, nil
}

func (b Buffer) write(pageNumber int) (error) {
	return nil
}

