package index

import (
	"encoding/binary"
	"fmt"
	"log"
	"wtfDB/memory"
)

const (
	MaxPageSize     = 256 * 1024
	MaxKeySize      = 64 * 1024
	MaxRecordIdSize = 128 * 1024
	KeySize         = 8 // bytes
	ValueTypeSize   = 8 // bytes
	InvalidKey      = -1
)

var (
	ErrInvalidPageTypeHeader = fmt.Errorf("invalid page type")
	ErrNilNode               = fmt.Errorf("node is nil")
)

/*
This interface defines the behavior of B+ Tree nodes.
We must implement two Pages that store the data of the B+ Tree index. These pages (sequences of bytes)
are represented in memory as nodes of the B+ tree. There's two kinds of nodes:
 1. Inner node represents an internal page which contains key, page pointer pairs. The first pointer in the node points

to the subtree holding items less than the first key, and the last pointer in the node points to the subtree
holding items greater than or equal to the last key. Other pointers are reference subtrees between the two keys:
Ki-1 ≤ Ks < Ki, where K is a set ofkeys, and Ks is a key that belongs to the subtree.
 2. Leaf node represents a leaf page which contains key, record id pairs.
*/
type BPlusTreeNode interface {
	// Return the value associated with a given key
	get(int) (int, bool)

	// Returns the number of keys and values in the node
	getSize() int

	// Returns the max number of keys and values for a given node
	getMaxSize() int

	// Returns the page id of the associated node
	getPageId() int

	// Returns a pointer to the inner parent node and nil when the node is a root node or does not have a parent
	// This method also removes the parent from the ancestor seen list (constructed durind downwards tree traversal)
	getParent() *innerNode

	// Returns the first key in a B+ tree node if the key list is not empty.
	// This method also returns true if the key list is non-empty, otherwise returns false if empty.
	// These keys are also referred to as index entries/separator keys and
	// they split the tree into subtrees, holding corresponding key ranges.
	// Keys are stored in sorted order to allow binary search.
	getSeparatorKey() (int, bool) // also the first key in a node

	// Returns true if leaf node, otherwise false.
	isLeaf() bool

	// Insert a key-value pair into the B+ tree
	insert(int, int) bool

	// Serializes B+ tree node to sequence of bytes
	toBytes() error

	// Deserializes B+ tree byte sequence to tree node data structure
	fromBytes([]byte) (BPlusTreeNode, error)
}

// Deserialize root page into a b+ tree node that is pinned and loaded into a buffer frame
func fromBytes(b *memory.BufferPoolManager, m *BPlusTreeMetadata) (BPlusTreeNode, error) {
	page, err := b.GetPage(m.rootPageId)
	if err != nil {
		return nil, err
	}
	var node BPlusTreeNode
	pageType := int(getPageType(page))
	if pageType == 1 {
		node, _ = newLeafNode(b, m).fromBytes(page.Data)
	} else if pageType == 0 {
		node, _ = newInnerNode(b, m).fromBytes(page.Data)
	} else {
		log.Printf("Unexpected byte in page header %d", pageType)
		return nil, ErrInvalidPageTypeHeader
	}
	return node, err
}

func fetchNodeByPage(b *memory.BufferPoolManager, m *BPlusTreeMetadata, pageId int) (BPlusTreeNode, error) {
	f, err := b.GetPage(pageId)
	if err != nil {
		log.Printf("unable to fetch node frame: %+v", err)
		return nil, err
	}
	var node BPlusTreeNode
	switch pageType := int(getPageType(f)); pageType {
	case 1: // Leaf node
		node = createLeafNodeFromPage(b, m, f)
	case 0: // Inner node
		node = createInnerNodeFromPage(b, m, f)
	default:
		log.Printf("Unknown node type: %d", pageType)
		return nil, fmt.Errorf("unknown node type: %d", pageType)
	}
	return node, nil
}

// Returns 1 if page is leaf, 0 if inner and -1 if invalid page
func getPageType(page *memory.Frame) int {
	return int(binary.BigEndian.Uint32(page.Data[0:]))
}
