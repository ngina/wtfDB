package main

import (
	"fmt"
	"testing"
)

func Test_main(t *testing.T) {
	main()
	k := 4
	v, ok := bptree.Get(k)
	fmt.Printf("Get--> key: %d, value: %d, exists: %v", k, v, ok)
}
