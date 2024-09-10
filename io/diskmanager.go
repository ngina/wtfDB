package io

const (
	// MaxPageSize is the max page size of the db is 4K bytes,
	// which is the typical OS page size.
	MaxPageSize = 4 * 1024
)

type DiskManager interface {
	WritePage(pageNumber int, contents []byte) error
	ReadPage(pageNumber int, p []byte) error
}

type DefaultDiskManager struct{}

func (d DefaultDiskManager) writePage(pageNumber int, contentsToWrite []byte) error {
	return nil
}

func (d DefaultDiskManager) readPage(pageNumber int, buf []byte) error {
	return nil
}

// toFile saves buffer data to disk.
// Creates file if it does not exist, or truncates the exisiting one before
// writing the content.
//
// This function is not atomic and concurrent readers may get half updated data.
// func (d DefaultDiskManager) toFile(path string, buf []byte) error {
// 	f, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0664)
// 	if err != nil {
// 		return err
// 	}
// 	defer f.Close()

// 	_, err = f.Write(buf)
// 	if err != nil {
// 		return err
// 	}

// 	// Explicitly flush file buffer content to disk.
// 	return f.Sync()
// }

// func (d DefaultDiskManager) fromFile(path string, buf []byte) error {
// 	return nil
// }
