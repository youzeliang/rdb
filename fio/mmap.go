package fio

import (
	"golang.org/x/exp/mmap"
	"os"
)

// MMap IO，内存文件映射
type MMap struct {
	readerAt *mmap.ReaderAt
}

func NewMMapIOManager(fileName string) (*MMap, error) {

	_, err := os.OpenFile(fileName, os.O_CREATE, DataFilePerm)
	if err != nil {
		return nil, err
	}

	readerAt, err := mmap.Open(fileName)
	if err != nil {
		return nil, err
	}

	return &MMap{
		readerAt: readerAt,
	}, nil
}
