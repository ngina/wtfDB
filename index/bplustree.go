package index

import (
	"wtfDB/memory"
)

/*
A B+Tree is a perfectly balanced search tree in which the internal pages direct t
he search and leaf pages contain the actual data entries. (reference cmu 15-445).
The index provides efficient data lookups and retrieval without needing to search every row in
a database table. It enables rapid random lookups and efficient scans of ordered records.

Ideally, this implementation should suport thread-safe search, insertion, deletion
(including splitting and merging nodes), and an iteratory to support in-order leaf scans.

Implementation of simple B+ tree data structure where internal pages direct
the search and leaf pages contain actual data.
 (1) We only support unique key
 (2) support insert & remove
 (3) The structure should shrink and grow dynamically
 (4) Implement index iterator for range scan
*/

type BPlusTree interface {
	Insert(k int, v int) bool
	Get(k int) (int, bool)
	// Remove(k int) bool
}

type BPlusTreeMetadata struct {
	rootPageId int    // root page id, set to an in
	order      int    // minimum number of keys for any node
	indexName  string // name of the B+ tree index, default name is primary
}

type bPlusTree struct {
	root     BPlusTreeNode            // root of the B+ tree
	bpm      *memory.BufferPoolManager // buffer pool manager
	metadata *BPlusTreeMetadata
}

func NewBPlusTreeMetadata(indexName string) *BPlusTreeMetadata {
	return &BPlusTreeMetadata{
		order:      4,
		rootPageId: memory.InvalidPageId,
		indexName:  indexName,
	}
}

func NewBPlusTree(indexName string, bufferManager *memory.BufferPoolManager, treeMetadata *BPlusTreeMetadata) (*bPlusTree, error) {
	t := &bPlusTree{
		metadata: treeMetadata,
		bpm:      bufferManager,
	}
	// case 1. there exists a valid root page id
	if treeMetadata.rootPageId != memory.InvalidPageId {
		n, err := bPlusTreeNode{}.fromBytes(t.bpm, t.metadata.rootPageId)
		if err != nil {
			return nil, err
		}
		t.root = n
	} else {
		// case 2: we need to create the root page
		t.root = newLeafNode(t.bpm)
	}
	return t, nil
}

// Inserts a k,v pair into the B+tree
func (t *bPlusTree) Insert(k int, v int) bool {
	return t.root.insert(k, v)
}

// Return the value associated with a given key
func (t *bPlusTree) Get(k int) (int, bool) {
	return t.root.get(k)
}
