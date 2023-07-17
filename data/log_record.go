package data

import (
	"encoding/binary"
	"hash/crc32"
)

type LogRecordPos struct {
	Fid    uint32 // file id
	Offset int64  // Offset refers to the position where data is stored in a data file.
}

type LogRecordType = byte

const (
	LogRecordNormal LogRecordType = iota
	LogRecordDeleted
)

// crc type keySize valueSize
// 4 +  1  +  5   +   5 = 15
const maxLogRecordHeaderSize = binary.MaxVarintLen32*2 + 5

// LogRecord 写入到数据文件的记录
// 之所以叫日志，是因为数据文件中的数据是追加写入的，类似日志的格式
type LogRecord struct {
	Key   []byte
	Value []byte
	Type  LogRecordType
}

// LogRecord 的头部信息
type logRecordHeader struct {
	crc        uint32        // crc 校验值
	recordType LogRecordType // 标识 LogRecord 的类型
	keySize    uint32        // key 的长度
	valueSize  uint32        // value 的长度

}

// EncodeLogRecord 对 LogRecord 进行编码，返回字节数组及长度
//
//	+-------------+-------------+-------------+--------------+-------------+--------------+
//	| crc 校验值  |  type 类型   |    key size |   value size |      key    |      value   |
//	+-------------+-------------+-------------+--------------+-------------+--------------+
//	    4字节          1字节        变长（最大5）   变长（最大5）     变长           变长

func EncodeLogRecord(logRecord *LogRecord) ([]byte, int64) {

	// 初始化一个 header 部分的字节数组
	header := make([]byte, maxLogRecordHeaderSize)

	// 第5个字节存Type
	header[4] = logRecord.Type

	var index = 5
	// 5字节之后，存储的是key和value的长度信息
	// 使用变长类型，节省空间

	index += binary.PutVarint(header[index:], int64(len(logRecord.Key)))
	index += binary.PutVarint(header[index:], int64(len(logRecord.Value)))

	var size = index + len(logRecord.Key) + len(logRecord.Value)

	encBytes := make([]byte, size)

	// 将 header 部分的内容拷贝过来

	copy(encBytes, header[:index])
	// 将 key 和 value 数据拷贝到字节数组中

	copy(encBytes[index:], logRecord.Key)
	copy(encBytes[index+len(logRecord.Key):], logRecord.Value)

	crc := crc32.ChecksumIEEE(encBytes[4:])
	binary.LittleEndian.PutUint32(encBytes[:4], crc)

	return encBytes, int64(size)

}

// 对字节数组中的Header信息进行解码
func decodeLogRecordHeader(buf []byte) (*logRecordHeader, int64) {

	// 如果没有4个字节，说明数据不完整
	if len(buf) <= 4 {
		return nil, 0
	}

	header := &logRecordHeader{
		crc:        binary.LittleEndian.Uint32(buf[:4]), // 最前面的字节
		recordType: buf[4],                              // 第5个字节
		keySize:    0,
		valueSize:  0,
	}

	var index = 5

	// 从第5个字节开始，读取key和value的长度信息

	keySize, keySizeLen := binary.Varint(buf[index:])
	header.keySize = uint32(keySize)
	index += keySizeLen

	valueSize, valueSizeLen := binary.Varint(buf[index:])
	header.valueSize = uint32(valueSize)
	index += valueSizeLen

	return header, int64(index)

}

func getLogRecordCRC(logRecord *LogRecord, header []byte) uint32 {
	if logRecord == nil {
		return 0
	}
	crc := crc32.ChecksumIEEE(header[:])
	crc = crc32.Update(crc, crc32.IEEETable, logRecord.Key)
	crc = crc32.Update(crc, crc32.IEEETable, logRecord.Value)
	return crc
}
