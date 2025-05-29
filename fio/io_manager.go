package fio

const DataFilePerm = 0644

type FileIOType = byte

const (
	// StandardFIO standard file IO
	StandardFIO FileIOType = iota

	// MemoryMap memory file mapping
	MemoryMap
)

// IOManager Abstract IO management interface that can be integrated with different types of IO, currently supporting standard file IO.
type IOManager interface {
	// Read corresponding data from a given position in a file
	Read([]byte, int64) (int, error)

	// Write a byte array to a file
	Write([]byte) (int, error)

	// Sync persisting data
	Sync() error

	// Close the file
	Close() error

	// Size get the size of the file
	Size() (int64, error)
}

func NewIOManager(fileName string, ioType FileIOType) (IOManager, error) {
	switch ioType {
	case StandardFIO:
		return NewFileIOManager(fileName)
	case MemoryMap:
		return NewMMapIOManager(fileName)
	default:
		panic("unsupported io type")
	}
}
