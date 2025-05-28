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

func (m *MMap) Sync() error {
	// MMap does not support Sync operation
	return nil
}

func (m *MMap) Write(b []byte) (int, error) {
	// MMap does not support Write operation
	return 0, nil
}

func (m *MMap) Size() (int64, error) {
	return int64(m.readerAt.Len()), nil
}

func (m *MMap) Read(b []byte, offset int64) (int, error) {
	return m.readerAt.ReadAt(b, offset)
}

func (m *MMap) Close() error {
	return m.readerAt.Close()
}
