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
//  1. the type of node (leaf or internal) (1 byte)
//  2. the number of keys (2 bytes)
//  3. the page id (8 bytes) of the next leaf node (or -1 if there is no next node)
//  4. a list of KV pairs
const LeafFixedHeaderSizeInBytes = 11

type LeafNode struct {
	keys      []int
	recordIds []int  // TODO: update to RecordId
	next      uint64 // TODO: handle the zero value edge case
}

func (n LeafNode) get(key int) (LeafNode, error) {
	return LeafNode{}, nil
}

func (n LeafNode) put(key int, recordId int) error {
	return nil
}

func (n LeafNode) toBytes(buf []byte) error {
	if len(n.keys) != len(n.recordIds) {
		return fmt.Errorf("number of keys and record ids have to be equal")
	}

	buf[0] = byte(1)
	buf = binary.LittleEndian.AppendUint16(buf, uint16(len(n.keys)))
	buf = binary.LittleEndian.AppendUint64(buf, n.next)
	for i := range n.keys {
		buf = binary.LittleEndian.AppendUint64(buf, uint64(n.keys[i]))
		buf = binary.LittleEndian.AppendUint64(buf, uint64(n.recordIds[i]))
	}
	return nil
}

func fromBytes(data []byte) (LeafNode, error) {
	fmt.Printf("provided data %#v", data)
	if len(data) < LeafFixedHeaderSizeInBytes {
		return LeafNode{}, fmt.Errorf("page has less than the fixed size header")
	}

	keySize := binary.LittleEndian.Uint16(data[1:3])
	nextPointer := binary.LittleEndian.Uint64(data[3:12])

	keys, recordIds := []int{}, []int{}
	startIndex, endIndex := 12, 12+9
	for i := 0; i < int(keySize); i++ {
		k := binary.LittleEndian.Uint64(data[startIndex:endIndex])
		keys = append(keys, int(k))
		startIndex, endIndex = endIndex, endIndex+9

		v := binary.LittleEndian.Uint64(data[startIndex:endIndex])
		recordIds = append(recordIds, int(v))
		startIndex, endIndex = endIndex, endIndex+9
	}

	return LeafNode{
		next:      nextPointer,
		keys:      keys,
		recordIds: recordIds,
	}, nil
}

func (n LeafNode) pageSizeInBytes() int {
	return (1 + 2 + 8 + (MaxKeySize+MaxRecordIdSize)*len(n.keys))
}
