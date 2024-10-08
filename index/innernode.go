package index

import (
	"encoding/binary"
	"fmt"
	"math"
	"slices"
	"wtfDB/io"
	"wtfDB/memory"
	"log"
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
const InternalPageSlotCount = io.PageSize - InternalPageHeaderSize/(KeySize+ValueType)
const NonExistentSiblingLink = math.MaxInt

// For use with methods that do not need a non-nil pointer/value receiver
var InnerNode innerNode

type innerNode struct {
	bPlusTreeNode
	keys         []int
	children     []uint64
	rightSibling int
	frame        *memory.Frame // page on which this node is serialized on
	parent       *innerNode
}

func basicInnerNode(m *memory.BufferPoolManager) *innerNode {
	return &innerNode{
		bPlusTreeNode: bPlusTreeNode{
			pageType:      0,
			bufferManager: m,
		},
	}
}

func newInnerNode(m *memory.BufferPoolManager) *innerNode {
	return &innerNode{
		bPlusTreeNode: bPlusTreeNode{
			pageType:      0,
			bufferManager: m,
		},
		keys:     []int{math.MaxInt},
		children: make([]uint64, 0),
		rightSibling: memory.InvalidPageId,
	}
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

func (n *innerNode) get(key int) (int, bool) {
	idx, _ := slices.BinarySearch(n.keys, key)
	childPageId := n.children[idx-1]
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

// Insert a key and page pointer pair into node.
// Returns true, if key/child pointer insertion was successful. Otherwise false,
// if insertion failed.
func (n *innerNode) insert(key int, pageId int) bool {
	// case 1. internal node is not full
	if n.getMaxSize()-n.getSize() >= 1 {
		n.sInsert(key, uint64(pageId))
		n.toBytes(n.frame.Data)
		return true
	}

	// case 2. internal node is full
	// to split inner node, redistribute keys evenly, but push up middle key
	newPageFrame, err := n.bPlusTreeNode.bufferManager.GetNewPageFrame()
	if err != nil {
		return false
	}
	newNode := newInnerNode(n.bufferManager)
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

	// recursively perform insertions upwards in the tree
	n.parent.insert(splitKey, newNode.frame.PageId)

	// persist changes to frame/page in memory
	newNode.toBytes(newNode.frame.Data)
	n.toBytes(n.frame.Data)
	return true
}

func (n *innerNode) sInsert(k int, pageId uint64) {
	pos, found := slices.BinarySearch(n.keys, k)
	if found {
		return
	}
	n.keys = slices.Insert(n.keys, pos, k)
	n.children = slices.Insert(n.children, pos-1, pageId) // there's n+1 children for n keys
}

// toBytes serializes an inner node to a slice of bytes
func (n *innerNode) toBytes(buf []byte) error {
	if len(n.children) != len(n.keys) {
		return fmt.Errorf("number of children equal to the number of keys")
	}

	// insert header values
	binary.BigEndian.PutUint32(buf[0:], uint32(0))
	binary.BigEndian.PutUint32(buf[4:], uint32(n.getSize()))
	binary.BigEndian.PutUint32(buf[8:], uint32(n.rightSibling))
	for i := range n.keys {
		// todo: dynamically set key size based on key type
		binary.BigEndian.PutUint64(buf[12+i*8:], uint64(n.keys[i]))
	}
	childrenOffset := 12 + (8 * len(n.keys))
	for i := range n.children {
		binary.BigEndian.PutUint64(buf[childrenOffset+i*8:], uint64(n.children[i]))
	}
	return nil
}

// fromBytes deserializes an inner node from a byte sequence.
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
	keys := []int{}
	for i := 0; i < int(keyCount/2); i++ {
		keys = append(keys, int(binary.BigEndian.Uint64(data[InternalPageHeaderSize+i*8:])))
	}
	pagePointers := []uint64{}
	childrenOffset := int(InternalPageHeaderSize + keyCount*8)
	for i := 0; i < int(keyCount/2); i++ {
		pagePointers = append(pagePointers, binary.LittleEndian.Uint64(data[childrenOffset+i*8:]))
	}

	return &innerNode{
		keys:         keys,
		children:     pagePointers,
		rightSibling: int(rightSibling),
		bPlusTreeNode: bPlusTreeNode{
			pageType: 0,
			size:     int(keyCount * 2),
			bufferManager: n.bufferManager,
		},
	}, nil
}
