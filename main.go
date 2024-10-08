package main

import (
	"wtfDB/index"
	"wtfDB/io"
	"wtfDB/memory"
)

var bptree index.BPlusTree

func main() {
	indexName := "primary"
	filename := "db_files/dbtest_2"
	bufferSize := 4
	bpm := memory.NewBufferPoolManager(io.NewDiskManager(filename), bufferSize)
	treeMetadata := index.NewBPlusTreeMetadata(indexName)
	t, err := index.NewBPlusTree(indexName, bpm, treeMetadata)
	if err != nil {
		panic(err)
	}
	// Insert 5 keys to test leaf node split
	for i := range 5 {
		t.Insert(i, 100+i)
	}
	bptree = t
}
