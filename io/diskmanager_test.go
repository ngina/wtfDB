package io

import (
	"bytes"
	"crypto/rand"
	"fmt"
	"os"
	"testing"
)

func Test_write(t *testing.T) {
	d := setup()

	// create a random slice of bytes
	l := 1024
	data := make([]byte, l)
	_, err := rand.Read(data)
	if err != nil {
		fmt.Printf("error: %+v", err)
	}
	// The slice should now contain random bytes instead of only zeroes.
	fmt.Println(bytes.Equal(data, make([]byte, l)))

	err = d.WritePage(0, data)
	if err != nil {
		fmt.Printf("Error: %+v", err)
	}
}

func setup() DiskManager {
	baseDir := "/Users/ngina/Workspace/wtfDB/db_files/"

	err := os.MkdirAll(baseDir, 0750)
	if err != nil {
		fmt.Println("trouble creating database file directory")
	}

	dbFileName := "dbtest_1"
	return NewDiskManager(baseDir + dbFileName)
}
