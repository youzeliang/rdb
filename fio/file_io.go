package fio

import "os"

type FileIO struct {
	fd *os.File // system file descriptor
}

func NewFileIOManager(fileName string) (*FileIO, error) {
	fd, err := os.OpenFile(fileName,
		os.O_CREATE|os.O_RDWR|os.O_APPEND, DataFilePerm)
	if err != nil {
		return nil, err
	}

	return &FileIO{
		fd: fd,
	}, nil
}

func (fio *FileIO) Read(b []byte, offect int64) (int, error) {
	return fio.fd.ReadAt(b, offect)
}

func (fio *FileIO) Write(b []byte) (int, error) {
	return fio.fd.Write(b)
}

func (fio *FileIO) Sync() error {
	return fio.fd.Sync()
}

func (fio *FileIO) Close() error {
	return fio.fd.Close()
}
