package index

import (
	"encoding/binary"
	"fmt"
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
const LeafPageHeaderSize = 16
const KeyTypeSize = 64 // Todo: get dynamically
const RecordIdSize = 64
const LeafPageSlotCount = io.PageSize - LeafPageHeaderSize/(KeyTypeSize+RecordIdSize)

var LeafNode leafNode

type leafNode struct {
	bPlusTreeNode
	keys         []int
	recordIds    []int         // TODO: update to RecordId type
	rightSibling int           // page number of the leaf's right sibling
	frame        *memory.Frame // page on which this node is serialized on
	parent       *innerNode    //
}

func newLeafNode(m memory.BufferPoolManager) *leafNode {
	return &leafNode{
		bPlusTreeNode: bPlusTreeNode{
			pageType:      1,
			bufferManager: m,
		},
		keys:      make([]int, 0),
		recordIds: make([]int, 0),
	}
}

// Get the number of key/value pairs stored in the leaf
func (l *leafNode) getSize() int {
	return len(l.keys) + len(l.recordIds)
}

// Returns the max number of key/pointer pairs stored in the leaf
// assuming (4k page size - 16 page header size)/ (64+64) ~~ approx. 255 keys
func (l *leafNode) getMaxSize() int {
	return LeafPageSlotCount
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
func (l *leafNode) put(k int, recordId int) bool {
	// case 1. l has enough space
	if l.getMaxSize()-l.getSize() >= 1 {
		l.insertSort(k, recordId)
		return true
	}

	// case 2. l is full, split leaf node into two when full
	// split l keys into L and a new node l2
	// redistribute entries evenly, copy up middle key
	// insert index entry pointing to l2 into parent of l
	newPageId := l.bPlusTreeNode.bufferManager.NewPage()
	frame, err := l.bPlusTreeNode.bufferManager.GetPage(newPageId)
	if err != nil {
		return false
	}
	// create a new node serialized on the new page
	// append the new k to current list of keys
	// copy half of the keys into the new node
	newL := newLeafNode(l.bufferManager)
	newL.frame = frame
	l.insertSort(k, recordId)

	// copy half of the keys/record ids into the new leaf node
	mid := len(l.keys) / 2
	newL.keys = append(newL.keys, l.keys[mid:]...)
	newL.recordIds = append(newL.recordIds, l.recordIds[mid:]...)
	newL.pageType = 1
	newL.toBytes(newL.frame.Data)

	// update current l node to keep the half the existing list
	l.keys = l.keys[:mid]
	l.recordIds = l.recordIds[:mid]
	l.rightSibling = newL.frame.PageId
	l.toBytes(l.frame.Data)

	// copy new split key into parent inner node
	l.parent.put(newL.keys[0], newPageId)
	return true
}

func (l *leafNode) insertSort(k int, rid int) {
	l.keys = append(l.keys, k)
	slices.Sort(l.keys)
	i := slices.Index(l.keys, k) // get index of inserted key, we only store unique keys
	l.recordIds = slices.Insert(l.recordIds, i, rid)
}

func (leafNode) get(key int) (leafNode, error) {
	return leafNode{}, nil
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
func (l *leafNode) toBytes(buf []byte) error {
	if len(l.keys) != len(l.recordIds) {
		return fmt.Errorf("number of keys and record ids have to be equal")
	}

	buf = binary.BigEndian.AppendUint32(buf, uint32(1))
	buf = binary.BigEndian.AppendUint32(buf, uint32(l.getSize()))
	buf = binary.BigEndian.AppendUint32(buf, uint32(l.getMaxSize()))
	buf = binary.BigEndian.AppendUint32(buf, uint32(l.rightSibling))
	for i := range l.keys {
		// todo: dynamically set key size based on key type
		buf = binary.BigEndian.AppendUint64(buf, uint64(l.keys[i]))
	}
	for i := range l.recordIds {
		buf = binary.BigEndian.AppendUint64(buf, uint64(l.recordIds[i]))
	}
	return nil
}

/*
Deserialize leaf page into leaf node structure.
This method translates a leaf page encoded as a byte sequence into a
leaf node (Go data structures).
*/
func (leafNode) fromBytes(data []byte) (*leafNode, error) {
	fmt.Printf("provided data %#v", data)
	if len(data) < LeafPageHeaderSize {
		return nil, fmt.Errorf("leaf page has less than the fixed-size page header")
	}

	pageType := binary.BigEndian.Uint32(data[0:4])
	if pageType != 1 {
		return nil, fmt.Errorf("internal error -- not a leaf page")
	}

	currentSize := binary.BigEndian.Uint32(data[4:8])
	maxSize := binary.BigEndian.Uint32(data[8:12])
	rightSibling := binary.BigEndian.Uint32(data[12:16])
	// todo: dynamically determine key type
	keys, recordIds := []int{}, []int{}
	keyOffset, ridOffset := LeafPageHeaderSize, LeafPageHeaderSize+(int(currentSize)/2*KeySize)
	for i := keyOffset; i < ridOffset; i = i + KeySize {
		k := binary.BigEndian.Uint64(data[i : i+KeySize])
		keys = append(keys, int(k))
	}

	count := 0
	for i := ridOffset; count < int(currentSize)/2; i = i + RecordIdSize {
		r := binary.BigEndian.Uint64(data[i : i+RecordIdSize])
		recordIds = append(recordIds, int(r))
		count++
	}

	return &leafNode{
		bPlusTreeNode: bPlusTreeNode{
			pageType: 1,
			maxSize:  int(maxSize),
			size:     int(currentSize),
		},
		rightSibling: int(rightSibling),
		keys:         keys,
		recordIds:    recordIds,
	}, nil
}
