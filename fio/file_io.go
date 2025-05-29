package fio

import "os"

type FileIO struct {
	fd *os.File // System file descriptor
}

func NewFileIOManager(path string) (*FileIO, error) {
	fd, err := os.OpenFile(path,
		os.O_CREATE|os.O_RDWR|os.O_APPEND,
		DataFilePerm,
	)
	if err != nil {
		return nil, err
	}
	return &FileIO{fd: fd}, nil

}

// Read 从文件的给定位置读取对应的数据
func (fi *FileIO) Read(b []byte, offset int64) (int, error) {
	return fi.fd.ReadAt(b, offset)
}

func (fi *FileIO) Write(b []byte) (int, error) {
	return fi.fd.Write(b)
}

func (fi *FileIO) Sync() error {
	return fi.fd.Sync()
}

func (fi *FileIO) Close() error {

	return fi.fd.Close()
}

// Size 获取到文件大小
func (fi *FileIO) Size() (int64, error) {
	stat, err := fi.fd.Stat()
	if err != nil {
		return 0, err
	}
	return stat.Size(), nil
}
