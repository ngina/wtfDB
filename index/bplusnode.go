package index

import "wtfDB/memory"

const MaxPageSize = 256 * 1024
const MaxKeySize = 64 * 1024
const MaxRecordIdSize = 128 * 1024
const KeySize = 4
const ValueType = 4

type RecordId struct {
	page int
	slot int
}

/*
We must implement three Page classes that store the data of the B+ Tree.
1. BPlusNode represents the base page struct that contains common information between the inner and leaf nodes/pages.
2. Inner node represents an internal page which contains key, page pointer pairs.
3. Leaf node represents a leaf page which contains key, record id pairs.
*/
type BPlusTreeNode interface {

	// n.get(k) returns the leaf node on which k
	// may reside when queried from n
	get(key int) (leafNode, error)

	//
	put(key int, recordId RecordId) (int, RecordId)

	// helpers to get/set size (number of key/value pairs stored in that page)
	getSize() int
	setSize()

	// Helper method to get/set max size (capacity) of the page
	getMaxSize() int
	setMaxSize()
	getMinSize() int

	// n.toBytes() serializes n and writes bytes into buf
	toBytes(buf []byte) error
}

type bPlusTreeNode struct {
	bufferManager memory.BufferPoolManager
	pageType      int // page type (inner/leaf/invalid)
	size          int // number of key & value pairs in a page
	maxSize       int // max number of key and valye pairs in a page
}
