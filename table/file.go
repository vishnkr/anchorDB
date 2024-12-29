package table

import (
	"fmt"
	"os"
	"path/filepath"
)

type FileWrapper struct{
	file *os.File
	size int64
}

func OpenFileWrapper(path string) (*FileWrapper,error){
	file, err := os.OpenFile(path, os.O_RDONLY, 0644)
	if err != nil {
		return nil, fmt.Errorf("failed to open file: %w", err)
	}
	stat, err := file.Stat()
	if err != nil {
		file.Close()
		return nil, fmt.Errorf("failed to get stats: %w", err)
	}
	return &FileWrapper{ file: file, size: stat.Size()},nil
}

func CreateFileWrapper(path string, data []byte) (*FileWrapper, error) {
	// Ensure the parent directory exists
	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create parent directory: %w", err)
	}

	// Open or create the file
	file, err := os.Create(path)
	if err != nil {
		return nil, fmt.Errorf("failed to open or create file: %w", err)
	}

	// Write data to the file
	written, err := file.Write(data)
	if err != nil {
		file.Close()
		return nil, fmt.Errorf("failed to write data to file: %w", err)
	}

	// Verify that all data has been written
	if written < len(data) {
		file.Close()
		return nil, fmt.Errorf("incomplete write: expected %d bytes, wrote %d bytes", len(data), written)
	}

	// Sync file contents to disk
	if err := file.Sync(); err != nil {
		file.Close()
		return nil, fmt.Errorf("failed to sync file: %w", err)
	}

	// Get file metadata (e.g., size)
	stat, err := file.Stat()
	if err != nil {
		file.Close()
		return nil, fmt.Errorf("failed to retrieve file stats: %w", err)
	}

	// Return the FileWrapper
	return &FileWrapper{size: stat.Size(), file: file}, nil
}


func (f *FileWrapper) ReadAt(offset int64, len int) []byte{
	buf := make([]byte, len)
	_,_ = f.file.ReadAt(buf,offset)
	return buf
}