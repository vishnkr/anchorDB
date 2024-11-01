package table

import (
	"fmt"
	"os"
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

func CreateFileWrapper(path string, data []byte) (*FileWrapper,error){
	if err := os.WriteFile(path, data, 0644); err != nil {
		return nil, fmt.Errorf("failed to write data to file: %w", err)
	}
	
	file, err:= os.OpenFile(path,os.O_RDWR,0644)
	if err != nil {
		return nil, fmt.Errorf("failed to open file after writing: %w", err)
	}
	if err := file.Sync(); err != nil {
		file.Close()
		return nil, fmt.Errorf("failed to sync file: %w", err)
	}
	stat, err := file.Stat()
	if err != nil {
		file.Close()
		return nil, fmt.Errorf("failed to get stats: %w", err)
	}
	return &FileWrapper{size: stat.Size(),file: file},nil
}

func (f *FileWrapper) ReadAt(offset int64, len int) []byte{
	buf := make([]byte, len)
	_,_ = f.file.ReadAt(buf,offset)
	return buf
}