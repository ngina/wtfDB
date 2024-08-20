package index

import (
	"encoding/binary"
	"fmt"
)

// A leaf node is a node with no descendants that contains
// pairs of keys and Record IDs that point to the relevant records in the table,
// as well a pointer to its right sibling.
//
// The structure of a leaf node includes:
// 	1. the type of node (leaf or internal) (1 byte)
// 	2. the number of keys (2 bytes)
// 	3. the page id (8 bytes) of our right sibling (or -1 if we don't have a right sibling)
// 	4. A list of KV pairs
type LeafNode struct {
	keys []int
	recordIds []int // TODO: update to RecordId
	rightSibling uint64 // TODO: handle the zero value
}

func (n LeafNode) toBytes(buf []byte) (error) {
	if len(n.keys) != len(n.recordIds) {
		return fmt.Errorf("number of keys and record ids have to be equal")
	}

	buf[0] = byte(1)
	buf = binary.LittleEndian.AppendUint16(buf, uint16(len(n.keys)))
	buf = binary.LittleEndian.AppendUint64(buf, n.rightSibling)
	for i := range n.keys {
		buf = binary.LittleEndian.AppendUint64(buf, uint64(n.keys[i]))
		buf = binary.LittleEndian.AppendUint64(buf, uint64(n.recordIds[i]))
	}
	return nil
}

func fromBytes(data []byte) (LeafNode, error) {
	fmt.Printf("provided data %#v", data)
	return LeafNode{}, fmt.Errorf("not implemented")
}

func (n LeafNode) pageSizeInBytes() (int) {
	return (1 + 2 + 8 + (MaxKeySize + MaxPageSize) * len(n.keys))
}
