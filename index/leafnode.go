package index

import (
	"encoding/binary"
	"fmt"
	"log"
	"slices"
	"wtfDB/io"
	"wtfDB/memory"
)

/*
A leaf of a B+ tree. A leaf node in a B+ tree is a node with no descendants that
stores pairs of n keys and n record ids that point to the relevant records in the table,
as well a pointer to its right sibling. A record id (which is a page id and a slot id) points to where
the actual tuple is stored.

Some implementation details:
	* Every leaf node is serialized and persisted on a single page
	* The keys are always sorted in ascenting order
	* The record id at index i corresponds to the key at index i

The layout of a leaf page is as follows:
	1. page type (leaf or internal), literal value 1 indicates that this node is a leaf node (4 bytes)
	2. current size, the number of key/pointer pairs the leaf node contains (4 bytes)
	3. max size, the max number of key/pointer pairs (4 bytes)
	4. the page id of the right sibling (or -1 if node doesn't have a right sibling) (4 bytes)
	5. list of keys
	6. list of record ids

--------------(Leaf page structure/layout copied from the CMU db impl)------------------------
* Leaf page format (keys are stored in order) (structure copied from the CMU db impl):
 *  ---------
 * | HEADER |
 *  ---------
 *  ---------------------------------
 * | KEY(1) | KEY(2) | ... | KEY(n) |
 *  ---------------------------------
 *  ---------------------------------
 * | RID(1) | RID(2) | ... | RID(n) |
 *  ---------------------------------
 *
 *  Header format (size in byte, 16 bytes in total):
 *  -----------------------------------------------
 * | PageType (4) | CurrentSize (4) | MaxSize (4) |
 *  -----------------------------------------------
 *  -----------------
 * | NextPageId (4) |
 *  -----------------
 -----------------------------------------------------------------------------------------------
*/

// All sizes are in bytes
const (
	LeafPageHeaderSize = 16
	LeafPageSlotCount  = io.PageSize - LeafPageHeaderSize/(KeySize+ValueTypeSize)
)

var ErrBufferFrameTooSmall = fmt.Errorf("buffer frame size cannot be less leaf page header size")

var LeafNode leafNode

type leafNode struct {
	treeMetadata  *BPlusTreeMetadata
	bufferManager *memory.BufferPoolManager
	keys          []int
	recordIds     []int         // TODO: update to RecordId type
	rightSibling  int           // page number of the leaf's right sibling
	frame         *memory.Frame // page on which this node is serialized on
}

/*
Returns a pointer to a new leaf node which is associated with a newly created
page within the buffer frame. The page is pinned.
*/
func newLeafNode(m *memory.BufferPoolManager, metadata *BPlusTreeMetadata) *leafNode {
	f, err := m.GetNewPageFrame()
	if err != nil {
		log.Printf("unable to get a new page frame: %+v", err)
		return nil
	}
	return &leafNode{
		treeMetadata:  metadata,
		bufferManager: m,
		keys:          make([]int, 0),
		recordIds:     make([]int, 0),
		rightSibling:  memory.InvalidPageId,
		frame:         f,
	}
}

// Constructs a leafNode object using the page's data.
func createLeafNodeFromPage(b *memory.BufferPoolManager, m *BPlusTreeMetadata, f *memory.Frame) *leafNode {
	leaf := &leafNode{
		bufferManager: b,
		treeMetadata: m,
		// keys:          []int{math.MinInt},
		// children:      make([]uint64, 0),
		rightSibling: memory.InvalidPageId,
		frame:        f,
	}
	_, _ = leaf.fromBytes(f.Data) // modifies new inner node
	return leaf
}

func (l *leafNode) isLeaf() bool {
	return true
}

// Get the number of key/value pairs stored in the leaf
func (l *leafNode) getSize() int {
	return len(l.keys) + len(l.recordIds)
}

// Returns the max number of key/pointer pairs stored in the leaf
// assuming (4k page size - 16 page header size)/ (64+64) ~~ approx. 255 keys
func (l *leafNode) getMaxSize() int {
	// return LeafPageSlotCount
	return 4 * 2
}

func (l *leafNode) getPageId() int {
	return l.frame.PageId
}

// Returns a pointer to the inner parent node and nil when the node does not have a parent
// This method also removes the parent from the ancestor seen list (constructed durind downwards tree traversal)
func (l *leafNode) getParent() *innerNode {
	return l.treeMetadata.removeAncestor()
}

func (l *leafNode) getSeparatorKey() (int, bool) {
	if len(l.keys) < 1 {
		return InvalidKey, false
	}
	return l.keys[0], true
}

/*
Inserts a key and record id into the B+ tree. This B+Tree index supports only unique keys.
Returns true when inserting a new key. Otherwise false, when inserting an
existing key into the B+ tree index tree.

Invariant: at any given time, each leaf page is at least half full.

There are two cases to consider:
1. Inserting the pair (k,r) into a leaf with space
2. Inserting the pair (k,r) into a leaf without space which causes an overflow. This results
in splitting n into a left and right node. The right node is the newly created right node, whose split
key is copied into the parent inner ndoe.
*/
func (l *leafNode) insert(k int, rid int) bool {
	// leaf node is nil
	if l == nil {
		return false
	}

	fmt.Printf("Leafnode: inserting k,v pair: %d, %d\n", k, rid)
	// case 1. l has enough space
	if l.getMaxSize()-l.getSize() >= 1 {
		fmt.Println("Leafnode: leaf node is not full, inserting...")
		l.insertSort(k, rid)
		l.toBytes()
		fmt.Printf("Leafnode: updated leafnode: %+v\n\n", l)
		return true
	}
	fmt.Printf("Before split: buffer manager: %+v\n", *l.bufferManager)

	// case 2. l is full, split leaf node into two when full
	// split l keys into L and a new node l2
	// redistribute entries evenly, copy up middle key
	// insert index entry pointing to l2 into parent of l

	// create a new node serialized on the new page
	// append the new k to current list of keys
	// copy half of the keys into the new node
	fmt.Println("Leafnode: leaf node is full, inserting k,v pair...")
	newL := newLeafNode(l.bufferManager, l.treeMetadata)
	if newL == nil {
		return false
	}
	l.insertSort(k, rid)

	// copy half of the keys/record ids into the new leaf node
	mid := len(l.keys) / 2
	fmt.Printf("Leaf node: split key: %d\n", mid)
	newL.keys = l.keys[mid:]
	newL.recordIds = l.recordIds[mid:]
	newL.toBytes()
	newL.frame.FrameMetadata.IsDirty = true
	fmt.Printf("Leafnode: new leafnode: %+v\n\n", newL)
	fmt.Printf("Leafnode: new leafnode frame: %+v\n\n", *newL.frame)

	// update current l node to keep half the existing keys and record ids
	l.keys = l.keys[:mid]
	l.recordIds = l.recordIds[:mid]
	l.rightSibling = newL.frame.PageId
	l.toBytes()
	l.frame.FrameMetadata.IsDirty = true
	fmt.Printf("Leafnode: existing leafnode: %+v\n\n", l)
	fmt.Printf("Leafnode: existing leafnode frame: %+v\n\n", *l.frame)
	fmt.Printf("After split: buffer manager: %+v\n", *l.bufferManager)

	// copy new split key into parent inner node
	l.getParent().insert(newL.keys[0], newL.frame.PageId)
	return true
}

func (l *leafNode) insertSort(k int, rid int) {
	pos, found := slices.BinarySearch(l.keys, k) // keys are sorted in ascending order
	if found {
		// overwrite record id
		return
	}
	l.keys = slices.Insert(l.keys, pos, k)
	l.recordIds = slices.Insert(l.recordIds, pos, rid)
}

// Return the value associated with a given key, otherwise -1.
// Also returns true of if the key exists in the leaf node.
// For a leaf node, returns the record id associated with the key.
func (l *leafNode) get(key int) (int, bool) {
	pos, ok := slices.BinarySearch(l.keys, key)
	if !ok {
		return -1, false
	}
	// todo: decode 64-bit record id
	v := l.recordIds[pos] // encoded as a 64-bit unsigned integer
	// &v = &RecordId{}
	return v, true
}

func (l *leafNode) search(k int) (*leafNode, bool) {
	_, ok := slices.BinarySearch(l.keys, k)
	if ok {
		return l, true
	}
	return l, false
}

/*
Serializes a leaf node into a byte sequence.
This method is used to serialize the leaf node into a leaf page that is
stored on-disk as a sequence of bytes.

We serialize a leaf node into a page as follows:
 1. page type (leaf or internal), literal value 1 indicates that this node is a leaf node (4 bytes)
 2. current size, the number of key/pointer pairs the leaf node contains (4 bytes)
 3. max size, the max number of key/pointer pairs (4 bytes)
 4. the page id of the right sibling (or -1 if node doesn't have a right sibling) (4 bytes)
 5. list of keys
 6. list of record ids
*/
func (l *leafNode) toBytes() error {
	if l == nil {
		log.Println("cannot convert nil pointer")
		return ErrNilNode
	}
	if len(l.frame.Data) < LeafPageHeaderSize {
		return ErrBufferFrameTooSmall
	}
	if len(l.keys) != len(l.recordIds) {
		return fmt.Errorf("number of keys and record ids have to be equal")
	}
	// clear buffer contents before write
	l.frame.ZeroBuffer()

	binary.BigEndian.PutUint32(l.frame.Data[0:], uint32(1))
	binary.BigEndian.PutUint32(l.frame.Data[4:], uint32(l.getSize()))
	binary.BigEndian.PutUint32(l.frame.Data[8:], uint32(l.getMaxSize()))
	binary.BigEndian.PutUint32(l.frame.Data[12:], uint32(l.rightSibling))

	for i := range l.keys {
		binary.BigEndian.PutUint64(l.frame.Data[LeafPageHeaderSize+(KeySize*i):], uint64(l.keys[i])) // todo: dynamically set key size based on key type
	}
	ridOffset := LeafPageHeaderSize + len(l.keys)*KeySize
	for i := range l.recordIds {
		binary.BigEndian.PutUint64(l.frame.Data[ridOffset+(ValueTypeSize*i):], uint64(l.recordIds[i]))
	}
	return nil
}

/*
Deserialize leaf page (in bytes) into a leaf node structure.
This method translates a leaf page encoded as a byte sequence into a
leaf node data structure.
*/
func (l *leafNode) fromBytes(data []byte) (BPlusTreeNode, error) {
	if len(data) < LeafPageHeaderSize {
		return nil, fmt.Errorf("leaf page has less than the fixed-size page header")
	}

	pageType := binary.BigEndian.Uint32(data[0:4])
	if pageType != 1 {
		return nil, fmt.Errorf("internal error -- not a leaf page")
	}

	currentSize := binary.BigEndian.Uint32(data[4:8])
	// maxSize := binary.BigEndian.Uint32(data[8:12])
	UrightSibling := binary.BigEndian.Uint32(data[12:16])
	// todo: dynamically determine key type
	keys, recordIds := []int{}, []int{}
	keyOffset, ridOffset := LeafPageHeaderSize, LeafPageHeaderSize+(int(currentSize)/2*KeySize)
	for i := keyOffset; i < ridOffset; i = i + KeySize {
		k := binary.BigEndian.Uint64(data[i : i+KeySize])
		keys = append(keys, int(k))
	}

	count := 0
	for i := ridOffset; count < int(currentSize)/2; i = i + ValueTypeSize {
		r := binary.BigEndian.Uint64(data[i : i+ValueTypeSize])
		recordIds = append(recordIds, int(r))
		count++
	}
	l.keys = keys
	l.recordIds = recordIds
	l.rightSibling = int(int32(UrightSibling))
	return l, nil
}
