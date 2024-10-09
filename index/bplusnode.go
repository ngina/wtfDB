package index

import (
	"encoding/binary"
	"fmt"
	"log"
	"wtfDB/memory"
)

const MaxPageSize = 256 * 1024
const MaxKeySize = 64 * 1024
const MaxRecordIdSize = 128 * 1024
const KeySize = 8       // bytes
const ValueTypeSize = 8 // bytes
var ErrPageTypeHeader = fmt.Errorf("invalid page type")
var ErrNilNode = fmt.Errorf("node is nil")

/*
We must implement three Page classes that store the data of the B+ Tree.
1. bPlusTreeNode represents the base page struct that contains common information between the inner and leaf nodes/pages.
2. Inner node represents an internal page which contains key, page pointer pairs.
3. Leaf node represents a leaf page which contains key, record id pairs.
*/
type BPlusTreeNode interface {

	// Return the value associated with a given key
	get(int) (int, bool)

	// Insert a key-value pair into the B+ tree
	insert(int, int) bool

	// Serializes B+ tree node to sequence of bytes
	toBytes(buf []byte) error

	// Deserializes B+ tree byte sequence to tree node data structure
	fromBytes([]byte) (BPlusTreeNode, error)
}

// Deserialize root page into a b+ tree node that is loaded into the buffer
func fromBytes(b *memory.BufferPoolManager, t *BPlusTreeMetadata) (BPlusTreeNode, error) {
	page, err := b.GetPage(t.rootPageId)
	if err != nil {
		return nil, err
	}
	var node BPlusTreeNode
	pageType := binary.BigEndian.Uint32(page.Data[0:])
	if int(pageType) == 1 {
		node, _ = newLeafNode(b, t).fromBytes(page.Data)
	} else if int(pageType) == 0 {
		node, _ = newInnerNode(b, t).fromBytes(page.Data)
	} else {
		log.Printf("Unexpected byte in page header %d", pageType)
		return nil, ErrPageTypeHeader
	}
	page.Unpin()
	return node, err
}
