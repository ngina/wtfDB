package index

import (
	"encoding/binary"
	"fmt"
)

// A inner node of a B+ tree. An inner node with n keys stores n + 1 "pointers" to
// children nodes (where a pointer is just a page number).
//
// A inner node includes:
//  1. the type of node (leaf or internal) (1 byte)
//  2. the number of keys (2 bytes), which is one fewer than the number of children pointer
//  3. a list of n keys
//  4. a list of pointers to n+1 children.
const InnerFixedHeaderSizeInBytes = 3

var InnerNode innerNode

type innerNode struct {
	keys         []int
	pagePointers []uint64
}

// toBytes serializes an inner node to a []byte
func (n innerNode) toBytes(buf []byte) ([]byte, error) {
	if len(n.pagePointers) != len(n.keys)+1 {
		return nil, fmt.Errorf("number of children should be one greater than the number of keys")
	}

	buf[0] = byte(0)
	buf = binary.LittleEndian.AppendUint16(buf, uint16(len(n.keys)))
	for i := range n.keys {
		buf = binary.LittleEndian.AppendUint64(buf, uint64(n.keys[i]))
	}
	for i := range n.pagePointers {
		buf = binary.LittleEndian.AppendUint64(buf, uint64(n.pagePointers[i]))
	}
	return buf, nil
}

func (innerNode) fromBytes(data []byte) (innerNode, error) {
	if len(data) < InnerFixedHeaderSizeInBytes {
		return innerNode{}, fmt.Errorf("inner node page has less than the fixed size header")
	}

	keys := []int{}
	keyCount := int(binary.LittleEndian.Uint16(data[1:3]))
	start := InnerFixedHeaderSizeInBytes
	for i := 0; i < keyCount; i++ {
		keys = append(keys, int(binary.LittleEndian.Uint64(data[start:start+8])))
		start = start + 8
	}

	pagePointers := []uint64{}
	pointerCount := keyCount + 1
	start = InnerFixedHeaderSizeInBytes + keyCount*8
	for i := 0; i < pointerCount; i++ {
		pagePointers = append(pagePointers, binary.LittleEndian.Uint64(data[start:start+8]))
		start = start + 8
	}

	return innerNode{
		keys:         keys,
		pagePointers: pagePointers,
	}, nil
}
