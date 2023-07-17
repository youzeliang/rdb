package fio

const DataFilePerm = 0644

// IOManager Abstract IO management interface that can be integrated with different types of IO, currently supporting standard file IO.
type IOManager interface {
	// Read corresponding data from a given position in a file
	Read([]byte, int64) (int, error)

	// Write a byte array to a file
	Write([]byte) (int, error)

	// Sync persisting data
	Sync() error

	// Close
	Close() error

	// Size 获取到文件大小
	Size() (int64, error)
}

// 初始化IOManager，目前只支持标准 FileIO

func NewIOManager(fileName string) (IOManager, error) {
	return NewFileIOManager(fileName)
}
