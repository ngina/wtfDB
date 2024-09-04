package index

type BPlusTree struct {
	root       innerNode
	order      int
	height     int
	noOfKeys   int
	noOfLeaves int
}

func (t *BPlusTree) insert(key int, record RecordId) {

}

func (t *BPlusTree) find(key int) {

}
