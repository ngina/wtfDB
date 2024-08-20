package index

const BTREE_PAGE_SIZE = 256 * 1024
const BTREE_MAX_KEY_SIZE = 64 * 1024
const BTREE_MAX_RID_SIZE = 128 * 1024

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

