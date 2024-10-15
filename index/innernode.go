package index

import (
	"encoding/binary"
	"fmt"
	"log"
	"math"
	"slices"
	"wtfDB/io"
	"wtfDB/memory"
)

/*
A inner node of a B+ tree.
An inner node stores n ordered keys and n+1 child pointers (ie. page ids/number) to other B+Tree pages.

The keys and pointers are internally representeed as an array of key/page_id pairs.
As the number of child pointers is one more than the number of keys, the first key in the key_array is set
to be invalid and lookups should always start from the second key.
Each pointer/page id i points to a subtree in which all keys K satisfy K(i) <= K < K(i+1).

To satisfy the B+ tree invariant:
(1) At any time, an internal page should be at least half full (ie it should contain at least half the keys)
(2) During insertion, one full internal page is split into two, to redistribute the keys/pointers
(3) During deletion, two half-full internal pages are  merged, to ensure the node is at least half-full

A inner node includes:
	1. header (12 bytes);
		1.1 the type of node (leaf or internal) (4 bytes),
		1.2 the number of keys (4 bytes),
		1.3 right sibling pointer (4 bytes)
	2. a list of n keys
	3. a list of pointers to n+1 children.

-----(Internal page structure/layout copied from the CMU db impl)------
 * Internal page format (keys are stored in increasing order):
 *  ---------
 * | HEADER |
 *  ---------
 *  ------------------------------------------
 * | KEY(1)(INVALID) | KEY(2) | ... | KEY(n) |
 *  ------------------------------------------
 *  ---------------------------------------------
 * | PAGE_ID(1) | PAGE_ID(2) | ... | PAGE_ID(n) |
 *  ---------------------------------------------
--------------------------------------------------------------------
*/

// All sizes are in bytes
const InternalPageHeaderSize = 12
const InternalPageSlotCount = io.PageSize - InternalPageHeaderSize/(KeySize+ValueTypeSize)
const NonExistentSiblingLink = math.MaxInt

// For use with methods that do not need a non-nil pointer/value receiver
var InnerNode innerNode

type innerNode struct {
	treeMetadata  *BPlusTreeMetadata
	bufferManager *memory.BufferPoolManager
	keys          []int
	children      []uint64 // page numbers of child nodes
	rightSibling  int
	frame         *memory.Frame // page on which this node is serialized on
}

/*
Returns a pointer to a new inner node.
This method persists the new inner node onto a buffer frame.
*/
func newInnerNode(b *memory.BufferPoolManager, m *BPlusTreeMetadata) *innerNode {
	f, err := b.GetNewPageFrame()
	if err != nil {
		log.Printf("unable to get a new page frame: %+v", err)
		return nil
	}
	return &innerNode{
		treeMetadata:  m,
		bufferManager: b,
		keys:          []int{math.MinInt},
		children:      make([]uint64, 0),
		rightSibling:  memory.InvalidPageId,
		frame:         f,
	}
}

func createInnerNodeFromPage(b *memory.BufferPoolManager, m *BPlusTreeMetadata, f *memory.Frame) *innerNode {
	inner := &innerNode{
		treeMetadata:  m,
		bufferManager: b,
		// keys:          []int{math.MinInt},
		// children:      make([]uint64, 0),
		// rightSibling:  memory.InvalidPageId,
		frame: f,
	}
	_, _ = inner.fromBytes(f.Data) // modifies new inner node
	return inner
}

func (i *innerNode) isLeaf() bool {
	return false
}

// Get the number of key/value pairs stored in the leaf
func (n *innerNode) getSize() int {
	return len(n.keys) + len(n.children)
}

// Returns the max number of key/pointer pairs stored in the leaf
// assuming (4k page size - 16 page header size)/ (64+64) ~~ approx. 255 keys
func (i *innerNode) getMaxSize() int {
	// return InternalPageSlotCount
	return 4 * 2
}

func (i *innerNode) getPageId() int {
	return i.frame.PageId
}

func (i *innerNode) getParent() *innerNode {
	return i.treeMetadata.removeAncestor()
}

func (i *innerNode) getSeparatorKey() (int, bool) {
	if len(i.keys) < 2 {
		return InvalidKey, false
	}
	return i.keys[1], true // get the second item in the list because the first is a null key
}

func (n *innerNode) get(key int) (int, bool) {
	pos, _ := slices.BinarySearch(n.keys, key)
	childPageId := n.children[pos-1]
	childPage, err := n.bufferManager.GetPage(int(childPageId))
	if err != nil {
		log.Println(err)
		return -1, false
	}
	node, err := n.fromBytes(childPage.Data)
	if err != nil {
		log.Println(err)
		return -1, false
	}
	return node.get(key)
}

/*
Finds the leaf node in which k is located or can be inserted into.

Keys are stored in sorted order to allow binary search. A subtree is found by locating
a key and following a corresponding pointer from the higher to the lower level.

The first pointer in the node points to the subtree holding items less than the first key, and the last
pointer in the node points to the subtree holding items greater than or equal to the last key.

Other pointers are reference subtrees between the two keys: Ki-1 ≤ Ks < Ki, where K is a set of
keys, and Ks is a key that belongs to the subtree.
*/
func (n *innerNode) search(k int) (*leafNode, bool) {
	var currNode *innerNode
	currPageFrame := n.frame
	currNode = n
	// perform lookup in inner node for the next page pointer
	for getPageType(currPageFrame) == 0 {
		// mark current node as seen
		n.treeMetadata.seen = append(n.treeMetadata.seen, n) // append node to seen nodes (this includes any inner root node)
		// get next page pointer/id using binary search
		pos, _ := slices.BinarySearch(currNode.keys, k)
		fmt.Printf("Inner node: getting corresponding pointer for key at position: %d\n", pos)
		var nextPageId int
		if pos == 0 || (pos < len(currNode.keys) && k >= currNode.keys[pos]) {
			nextPageId = int(currNode.children[pos])
		} else {
			nextPageId = int(currNode.children[pos-1])
		}
		fmt.Printf("Inner node: got corresponding page pointer: %d\n", nextPageId)
		// load next page into memory
		currPageFrame, _ = n.bufferManager.GetPage(nextPageId) // load next page into memory, if not already in memory
		if getPageType(currPageFrame) == 0 {
			currNode = createInnerNodeFromPage(n.bufferManager, n.treeMetadata, currPageFrame)
		}
	}
	return createLeafNodeFromPage(n.bufferManager, n.treeMetadata, currPageFrame).search(k)
}

// Insert a key and page pointer pair into node.
// Returns true, if key/child pointer insertion was successful. Otherwise false,
// if insertion failed.
func (n *innerNode) insert(key int, pageId int) bool {
	// perform lookup of where to insert
	fmt.Printf("Inner node: inserting k,v pair: %+v,%+v\n", key, pageId)
	// case 0. internal node is nil
	if n == nil {
		log.Println(ErrNilNode.Error())
		return false
	}

	// case 1. internal node is not full
	if n.getMaxSize()-n.getSize() >= 1 {
		fmt.Printf("Innernode: is not full inserting k,v pair: %d,%d\n", key, pageId)
		n.sInsert(key, uint64(pageId))
		n.toBytes()
		fmt.Printf("Innernode: updated inner node: %+v\n", n)
		return true
	}

	// case 2. internal node is full
	// to split inner node, redistribute keys evenly, but push up middle key
	newPageFrame, err := n.bufferManager.GetNewPageFrame()
	if err != nil {
		return false
	}
	newNode := newInnerNode(n.bufferManager, n.treeMetadata)
	newNode.frame = newPageFrame

	// create new right node and redistribute keys
	n.sInsert(key, uint64(pageId))
	mid := len(n.keys) / 2
	newNode.keys = n.keys[mid+1:]
	newNode.children = n.children[mid+1:]
	newNode.rightSibling = NonExistentSiblingLink
	// todo: set parent pointer
	splitKey := n.keys[mid]
	// update the split node
	n.keys = n.keys[:mid]
	n.children = n.children[:mid]
	n.rightSibling = newPageFrame.PageId

	n.getParent().insert(splitKey, newNode.frame.PageId)

	// persist changes to frame/page in memory
	newNode.toBytes()
	n.toBytes()
	return true
}

func (n *innerNode) sInsert(k int, pageId uint64) {
	pos, found := slices.BinarySearch(n.keys, k)
	if found {
		return // only support unique keys
	}
	n.keys = slices.Insert(n.keys, pos, k)
	n.children = slices.Insert(n.children, pos, pageId) // there's n+1 children for n keys
}

// toBytes serializes an inner node to a slice of bytes
func (n *innerNode) toBytes() error {
	if len(n.children) != len(n.keys) {
		return fmt.Errorf("number of children equal to the number of keys")
	}
	// clear buffer contents before write
	n.frame.ZeroBuffer()
	// insert header values
	binary.BigEndian.PutUint32(n.frame.Data[0:], uint32(0))
	binary.BigEndian.PutUint32(n.frame.Data[4:], uint32(n.getSize()))
	binary.BigEndian.PutUint32(n.frame.Data[8:], uint32(n.rightSibling))
	for i := range n.keys {
		binary.BigEndian.PutUint64(n.frame.Data[12+i*8:], uint64(n.keys[i])) // todo: dynamically set key size based on key type
	}
	childrenOffset := 12 + (8 * len(n.keys))
	for i := range n.children {
		binary.BigEndian.PutUint64(n.frame.Data[childrenOffset+i*8:], uint64(n.children[i]))
	}
	return nil
}

// fromBytes deserializes page (keys, values) bytes into an inner node representation
// It returns the deserialized a pointer to an inner node and an
// error if unable to deserialize the byte sequence.
func (n *innerNode) fromBytes(data []byte) (BPlusTreeNode, error) {
	if len(data) < InternalPageHeaderSize {
		return nil, fmt.Errorf("inner node page has less than the required page fixed size header")
	}

	pageType := binary.BigEndian.Uint32(data[0:])
	if pageType != uint32(0) {
		return nil, fmt.Errorf("not an inner node")
	}
	keyCount := binary.BigEndian.Uint32(data[4:])
	rightSibling := binary.BigEndian.Uint32(data[8:])
	// parse keys
	keys, pagePointers := []int{}, []uint64{}
	for i := 0; i < int(keyCount/2); i++ {
		keys = append(keys, int(binary.BigEndian.Uint64(data[InternalPageHeaderSize+i*8:])))
	}
	// parse page pointers
	childrenOffset := int(InternalPageHeaderSize + keyCount*8)
	for i := 0; i < int(keyCount/2); i++ {
		pagePointers = append(pagePointers, binary.LittleEndian.Uint64(data[childrenOffset+i*8:]))
	}
	n.keys = keys
	n.children = pagePointers
	n.rightSibling = int(rightSibling)

	// return &innerNode{
	// 	keys:          keys,
	// 	children:      pagePointers,
	// 	rightSibling:  int(rightSibling),
	// 	bufferManager: n.bufferManager,
	// 	treeMetadata:  n.treeMetadata,
	// }, nil
	return n, nil
}
