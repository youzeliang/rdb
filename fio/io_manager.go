package fio

const DataFilePerm = 0644

type FileIOType = byte

const (
	// StandardFIO standard file IO
	StandardFIO FileIOType = iota

	// MemoryMap memory-mapped file IO
	MemoryMap
)

type IOManager interface {

	// Read reads data from the file at the specified offset.
	Read([]byte, int64) (int, error)

	// Write writes data to the file.
	Write([]byte) (int, error)

	// Sync syncs the data to disk.
	Sync() error

	// Close closes the file.
	Close() error

	// Size get the size of the file
	Size() (int64, error)
}

// NewIOManager Initializes an IOManager
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
