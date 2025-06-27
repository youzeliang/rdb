package data

import (
	"errors"
	"fmt"
	"github.com/youzeliang/rdb/fio"
	"hash/crc32"
	"io"
	"path/filepath"
)

var (
	ErrInvalidCRC    = errors.New("invalid crc value, log record maybe corrupted")
	ErrInvalidSize   = errors.New("invalid size value in log record")
	ErrReadLogRecord = errors.New("failed to read log record")
)

const (
	DataFileNameSuffix = ".data"
	HintFileName       = "hint-index"

	MergeFinishedFileName = "merge-finished"
	SeqNoFileName         = "seq-no"
)

// DataFile represents a data file
type DataFile struct {
	FileId    uint32        // File ID
	WriteOff  int64         // 文件写到哪个位置
	IoManager fio.IOManager // io 读写管理
}

func OpenDataFile(dirPath string, fileId uint32, ioType fio.FileIOType) (*DataFile, error) {
	fileName := GetDataFileName(dirPath, fileId)
	return newDataFile(fileName, fileId, ioType)
}

func newDataFile(fileName string, fileId uint32, ioType fio.FileIOType) (*DataFile, error) {
	// Initialize IOManager interface
	ioManager, err := fio.NewIOManager(fileName, ioType)
	if err != nil {
		return nil, err
	}
	return &DataFile{
		FileId:    fileId,
		WriteOff:  0,
		IoManager: ioManager,
	}, nil
}

// ReadLogRecord reads a LogRecord from the data file at the given offset
func (df *DataFile) ReadLogRecord(offset int64) (*LogRecord, int64, error) {

	fileSize, err := df.IoManager.Size()
	if err != nil {
		return nil, 0, fmt.Errorf("get file size error: %w", err)
	}

	if offset < 0 || offset >= fileSize {
		return nil, 0, fmt.Errorf("invalid offset %d, fileSize %d", offset, fileSize)
	}

	// read header
	var headerBytes int64 = maxLogRecordHeaderSize
	if offset+maxLogRecordHeaderSize > fileSize {
		headerBytes = fileSize - offset
	}

	headerBuf, err := df.readNBytes(headerBytes, offset)
	if err != nil {
		return nil, 0, fmt.Errorf("read header error: %w", err)
	}

	header, headerSize := decodeLogRecordHeader(headerBuf)
	if header == nil {
		return nil, 0, io.EOF
	}

	// check CRC and key/value size
	if header.crc == 0 && header.keySize == 0 && header.valueSize == 0 {
		return nil, 0, io.EOF
	}

	if header.keySize > uint32(fileSize) || header.valueSize > uint32(fileSize) {
		return nil, 0, ErrInvalidSize
	}

	keySize, valueSize := int64(header.keySize), int64(header.valueSize)
	var recordSize = headerSize + keySize + valueSize

	// Construct LogRecord object
	logRecord := &LogRecord{Type: header.recordType}

	// Read the actual key/value data
	if keySize > 0 || valueSize > 0 {
		kvBuf, err := df.readNBytes(keySize+valueSize, offset+headerSize)
		if err != nil {
			return nil, 0, fmt.Errorf("read kv error: %w", err)
		}

		// Parse key and value
		logRecord.Key = kvBuf[:keySize]
		logRecord.Value = kvBuf[keySize:]
	}

	// Verify data integrity
	crc := getLogRecordCRC(logRecord, headerBuf[crc32.Size:headerSize])
	if crc != header.crc {
		return nil, 0, ErrInvalidCRC
	}

	return logRecord, recordSize, nil
}

func (df *DataFile) Sync() error {
	return df.IoManager.Sync()
}

func (df *DataFile) Write(buf []byte) error {
	n, err := df.IoManager.Write(buf)
	if err != nil {
		return err
	}
	df.WriteOff += int64(n)
	return nil
}

func (df *DataFile) Close() error {
	return df.IoManager.Close()
}

// Read the specified number of bytes
func (df *DataFile) readNBytes(n int64, offset int64) (b []byte, err error) {
	b = make([]byte, n)
	_, err = df.IoManager.Read(b, offset)
	return
}

func OpenMergeFinishedFile(dirPath string) (*DataFile, error) {
	fileName := filepath.Join(dirPath, MergeFinishedFileName)
	return newDataFile(fileName, 0, fio.StandardFIO)
}

// OpenSeqNoFile opens the file that stores transaction sequence numbers
func OpenSeqNoFile(dirPath string) (*DataFile, error) {
	fileName := filepath.Join(dirPath, SeqNoFileName)
	return newDataFile(fileName, 0, fio.StandardFIO)
}

// WriteHintRecord writes the index to the hint file
func (df *DataFile) WriteHintRecord(key []byte, pos *Position) error {
	hintRecord := &LogRecord{
		Key:   key,
		Value: EncodeLogRecordPos(pos),
	}
	encRecord, _ := EncodeLogRecord(hintRecord)
	return df.Write(encRecord)
}

func GetDataFileName(dirPath string, fileId uint32) string {
	return filepath.Join(dirPath, fmt.Sprintf("%09d", fileId)+DataFileNameSuffix)
}

func OpenHintFile(dirPath string) (*DataFile, error) {
	fileName := filepath.Join(dirPath, HintFileName)
	return newDataFile(fileName, 0, fio.StandardFIO)
}

func (df *DataFile) SetIOManager(dirPath string, ioType fio.FileIOType) error {
	if err := df.IoManager.Close(); err != nil {
		return err
	}
	ioManager, err := fio.NewIOManager(GetDataFileName(dirPath, df.FileId), ioType)
	if err != nil {
		return err
	}
	df.IoManager = ioManager
	return nil
}
