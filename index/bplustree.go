package index

import (
	"fmt"
	"strings"
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
	rootPageId int          // root page id, set to an in
	order      int          // minimum number of keys for any node
	indexName  string       // name of the B+ tree index, default name is primary
	seen       []*innerNode // maintains ancestral nodes seen during downward tree traversal from root to leaf
}

type bPlusTree struct {
	Root     BPlusTreeNode             // root of the B+ tree
	bpm      *memory.BufferPoolManager // buffer pool manager
	metadata *BPlusTreeMetadata
}

func NewBPlusTreeMetadata(indexName string) *BPlusTreeMetadata {
	return &BPlusTreeMetadata{
		order:      4,
		rootPageId: memory.InvalidPageId,
		indexName:  indexName,
		seen:       make([]*innerNode, 0),
	}
}

func NewBPlusTree(indexName string, b *memory.BufferPoolManager, m *BPlusTreeMetadata) (*bPlusTree, error) {
	bptree := &bPlusTree{
		metadata: m,
		bpm:      b,
	}
	// case 1. there exists a valid root page id
	if m.rootPageId != memory.InvalidPageId {
		n, err := fromBytes(b, m)
		if err != nil {
			return nil, err
		}
		bptree.Root = n
	} else {
		// case 2: we need to create the root page
		leaf := newLeafNode(b, m)
		bptree.updateRoot(leaf)
	}
	return bptree, nil
}

// Inserts a k,v pair into the B+tree
func (t *bPlusTree) Insert(k int, v int) bool {
	// how do we know there's an overflow ?
	// what happens when the tree height changes ?
	// how do we initiate the new root >
	// what type is the new root?
	// update root helper can be useful here
	fmt.Printf("inserting k,v pair: %+v,%+v\n", k, v)
	if t.Root.getMaxSize() <= t.Root.getSize() {
		// insertion into full root node will cause an overflow
		// case 1. root is a leaf, therefore we need to create a new inner node
		if t.Root.isLeaf() { // nit: type assertion with ok comma idiom ?
			fmt.Println("root is a leaf")
			newRoot := newInnerNode(t.bpm, t.metadata)
			t.metadata.seen = append(t.metadata.seen, newRoot) // append new root to ancestor stack maintained during downward tree traversal
			l, _ := t.Root.(*leafNode)
			// set first pointer in the new root to point to the subtree holding less than the first index entry
			newRoot.children = append(newRoot.children, uint64(l.frame.PageId))
			// set parent of root leaf L to newroot and update root page id
			t.updateRoot(newRoot)
			// perform insertion into current root node
			return l.insert(k, v)
		}

		// case 2: root node is an inner node, therefore we need to create a new inner node
		// 1. create a new root of type inner node and insert page pointer to current root, if the current root is full
		// 2. set parent of current root as the new root
		// 3. traverse root to find the correct leaf node L to insert k,v pair. use lookup algorithm to find correct leaf node
		// 4. insert k,v pair into leaf node
		// 5. if new root contains non-empty keys (index entries), update root pointer/root page id to point to new root

	}
	// case : root is leaf and root is not full (can insert k/v pair directly into leaf node)
	if t.Root.isLeaf() {
		return t.Root.insert(k, v)
	}

	// case : root is inner node and root is not full
	// 3. traverse root to find the correct leaf node L to insert k,v pair. use lookup algorithm to find correct leaf node
	// 4. insert k,v pair into leaf node
	fmt.Println("BPTree: current root is an inner node...")
	fmt.Printf("BPTree: inserting [%+v,%+v] into tree\n", k, v)
	leafNode, _ := t.Root.(*innerNode).search(k)
	return leafNode.insert(k, v)
}

// Return the value associated with a given key
func (t *bPlusTree) Get(k int) (int, bool) {
	return t.Root.get(k)
}

func (t *bPlusTree) updateRoot(newRoot BPlusTreeNode) {
	t.Root = newRoot
	t.metadata.rootPageId = newRoot.getPageId()
}

func (m *BPlusTreeMetadata) isRootPage(pageId int) bool {
	return m.rootPageId == pageId
}

func (m *BPlusTreeMetadata) getAncestor() *innerNode {
	if len(m.seen) > 0 {
		return m.seen[len(m.seen)-1]
	}
	return nil
}

// Returns the ancestor that was removed.
// Returns nil when there aren't any ancestors to remove.
func (m *BPlusTreeMetadata) removeAncestor() *innerNode {
	n := len(m.seen)
	if n > 0 {
		val := m.seen[n-1]
		m.seen = m.seen[:n]
		return val
	}
	return nil
}

// PrettyPrint recursively prints the B+ tree structure
func PrettyPrint(node BPlusTreeNode, level int, prefix string, isLast bool) {
	connector := "├── "   // Regular connector
	childPrefix := "│   " // Prefix for children at each level

	// If is last child
	if isLast {
		connector = "└── "   // Change to backtick for the last child
		childPrefix = "    " // No vertical line for the last child
	}

	var boxTop, boxBottom, boxContent strings.Builder
	// Define the box characters
	boxTop.WriteString("┌───────────────┐\n")
	boxBottom.WriteString("└───────────────┘\n")

	switch n := node.(type) {
	case *innerNode:
		boxContent.WriteString(fmt.Sprintf(" Inner Node:\n Keys: %v\n Page Pointers: %v\n", n.keys[1:], n.children))
		count := 15 - len(boxContent.String())
		if count < 0 {
			count = 0
		}

		// Print the inner node's keys and children
		fmt.Printf("%s%s", prefix, boxTop.String())
		fmt.Printf("%s%s%s", prefix, boxContent.String(), strings.Repeat(" ", count))
		fmt.Printf("%s%s", prefix, boxBottom.String())

		// Recursively print each child
		for i, childPageNum := range n.children {
			isLastChild := i == len(n.children)-1
			childNode, _ := fetchNodeByPage(n.bufferManager, n.treeMetadata, int(childPageNum))
			PrettyPrint(childNode, level+1, prefix+childPrefix, isLastChild)
		}
	case *leafNode:
		// fmt.Printf("%s%sLeaf Node: Keys: %v, RecordIds: %v, RightSibling: %d, PageId: %d\n",
		// 	indent, connector, n.keys, n.recordIds, n.rightSibling, n.frame.PageId)

		boxContent.WriteString(fmt.Sprintf(" Leaf Node:\n %sKeys: %v\n %sPageId: %v\n", connector, n.keys, connector, n.frame.PageId))
		count := 15 - len(boxContent.String())
		if count < 0 {
			count = 0
		}

		// Print the leaf node's keys and record IDs
		fmt.Printf("%s%s", prefix, boxTop.String())
		fmt.Printf("%s%s%s", prefix, boxContent.String(), strings.Repeat(" ", count))
		fmt.Printf("%s%s", prefix, boxBottom.String())
	default:
		fmt.Printf("%s%sUnknown Node Type\n", prefix, connector)
	}
}
