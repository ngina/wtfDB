package io

import (
	"fmt"
	"io"
	"log"
	"os"
)

const (
	// MaxPageSize is the max data page size of the db is 4K bytes,
	// which is the typical OS page size.
	PageSize = 4 * 1024
)

var ErrorReadFromDisk = fmt.Errorf("error reading from disk")
var ErrorWriteToDisk = fmt.Errorf("error writing to disk")
var ErrorFlushToDisk = fmt.Errorf("page contents not flushed to disk")

/*
DiskManager is responsible for allocating and deallocating pages on disk.

It also reads and writes pages from and to disk, providing a logical file layer
within the context of a DBMS.
*/
type DiskManager interface {
	WritePage(pageId int, data []byte) error
	ReadPage(pageId int, buf []byte) error
}

type DefaultDiskManager struct {
	dbFile    *os.File
	writeCount int
}

/*
Creates a new disk manager that writes to the specified database file.
*/
func NewDiskManager(fileName string) DiskManager {
	f, err := os.OpenFile(fileName, os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		log.Fatal("cannot open db file: " + err.Error())
	}

	return &DefaultDiskManager{
		dbFile: f,
	}
}

func (d *DefaultDiskManager) Shutdown() {
	if err := d.dbFile.Close(); err != nil {
		log.Println("failed to close database file during shutdown")
	}
}

// WritePage writes the page data of the specified file to the disk file.
// It takes a page number and a slice of bytes to be written to the page.
// It returns an error if it cannot write to the page.
func (d *DefaultDiskManager) WritePage(pageId int, data []byte) error {
	d.writeCount++
	offset := pageId * PageSize
	_, err := d.dbFile.WriteAt(data, int64(offset))
	if err != nil {
		log.Printf("error writing to file at offset %d", offset)
		return ErrorWriteToDisk
	}

	// Explicitly flush file buffer content to disk.
	err = d.dbFile.Sync()
	if err != nil {
		return ErrorFlushToDisk
	}
	return nil
}

// Read the contents of the specified page from disk into the byte buffer
func (d *DefaultDiskManager) ReadPage(pageId int, buf []byte) error {
	offset := pageId * PageSize
	n, err := d.dbFile.ReadAt(buf, int64(offset))
	log.Printf("read bytes %d from page %d", n, pageId)
	if err != nil && err != io.EOF {
		log.Printf("error when writing to disk page %d", pageId)
		return ErrorReadFromDisk
	}
	if err == io.EOF && n < PageSize {
		log.Printf("i/o error: read hit end of file at offset %d, missing %d bytes", offset, PageSize-n)
	}
	return nil
}
