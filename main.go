package main

import (
	"math/rand"
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

	// Test inserting and splitting of nodes
	for i := 1; i <= 8; i++ {
		t.Insert(100+i, rand.Intn(59))
	}
	bptree = t
}
