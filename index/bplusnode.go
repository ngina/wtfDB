package index

const MaxPageSize = 256 * 1024
const MaxKeySize = 64 * 1024
const MaxRecordIdSize = 128 * 1024

type RecordId struct {
	page int
	slot int
}

type BPlusNode interface {

	// n.get(k) returns the leaf node on which k 
	// may reside when queried from n
	get(key int) (LeafNode, error)

	//
	put(key int, recordId RecordId) (int, RecordId)

	// n.toBytes() serializes n and writes bytes into buf
	toBytes(buf []byte) error
}

