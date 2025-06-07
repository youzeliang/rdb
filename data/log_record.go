package data

import (
	"encoding/binary"
	"hash/crc32"
)

type LogRecordType = byte

const (
	LogRecordNormal  LogRecordType = iota
	LogRecordDeleted               // 在 loadIndexFromDataFiles 的时候遇到有删除标记的,也就会删除
	LogRecordTxnFinished
)

// 这里为什么是5个字节
// 因为我们使用了变长编码，所以 key 和 value 的长度是变长的
// 但是我们可以预估一个最大值，假设 key 和 value 的长度都不超过 2^32，那么 key 和 value 的长度最大就是 2^32
// 所以 key 和 value 的长度最大就是 2^32，所以 key 和 value 的长度最大就是 5 个字节

// crc          type   keySize valueSize
// 4(uint32)  +  1  +  5   +   5 = 15
const maxLogRecordHeaderSize = binary.MaxVarintLen32*2 + 5

// LogRecord 写入到数据文件的记录

// LogRecord 之所以叫日志，是因为数据文件中的数据是追加写入的，类似日志的格式
type LogRecord struct {
	Key   []byte
	Value []byte
	Type  LogRecordType
}

type TransactionRecord struct {
	Record *LogRecord
	Pos    *Position
}

// LogRecord 的头部信息
type logRecordHeader struct {
	crc        uint32        // crc 校验值
	recordType LogRecordType // 标识 LogRecord 的类型
	keySize    uint32        // key 的长度
	valueSize  uint32        // value 的长度
}

// Position 数据内存索引，主要是描述数据在磁盘上的位置
type Position struct {
	Fid    uint32 // 文件id, 将文件存储的到了哪个文件夹
	Offset int64  // 偏移，数据存储到了数据文件中的哪个位置
	Size   uint32 // 标识数据在磁盘上的大小

}

// EncodeLogRecord 对 LogRecord 进行编码，返回字节数组及长度
//
//	+-------------+-------------+-------------+--------------+-------------+--------------+
//	| crc 校验值  |  type 类型   |    key size |   value size |      key    |      value   |
//	+-------------+-------------+-------------+--------------+-------------+--------------+
//	    4字节          1字节        变长（最大5）   变长（最大5）     变长           变长
func EncodeLogRecord(logRecord *LogRecord) ([]byte, int64) {
	// initializing a header with a fixed size
	header := make([]byte, maxLogRecordHeaderSize)

	// 第五个字节存储 Type
	header[4] = logRecord.Type
	var index = 5
	// 5 字节之后，存储的是 key 和 value 的长度信息
	// 使用变长类型，节省空间
	// 这里递增，下一次就可以从新的位置开始存储了
	index += binary.PutVarint(header[index:], int64(len(logRecord.Key)))
	index += binary.PutVarint(header[index:], int64(len(logRecord.Value)))

	// the size of the entire LogRecord is the length of the header plus the length of the key and value
	var size = index + len(logRecord.Key) + len(logRecord.Value)
	encBytes := make([]byte, size)

	// copy the header part into the byte array
	copy(encBytes[:index], header[:index])

	// copy the key and value data into the byte array
	copy(encBytes[index:], logRecord.Key)
	copy(encBytes[index+len(logRecord.Key):], logRecord.Value)

	// check the CRC for the entire LogRecord data
	crc := crc32.ChecksumIEEE(encBytes[4:])
	binary.LittleEndian.PutUint32(encBytes[:4], crc)

	return encBytes, int64(size)
}

// 根据字节数组拿到拿到字节数组的头部信息,在get的时候 有从ReadLogRecord方法中调用
func decodeLogRecordHeader(buf []byte) (*logRecordHeader, int64) {
	// if the length is less than or equal to 4, the data is incomplete
	if len(buf) <= 4 {
		return nil, 0
	}

	header := &logRecordHeader{
		crc:        binary.LittleEndian.Uint32(buf[:4]),
		recordType: buf[4],
	}

	var index = 5

	// get the actual key size
	keySize, n := binary.Varint(buf[index:])
	header.keySize = uint32(keySize)
	index += n

	// get the actual value size
	valueSize, n := binary.Varint(buf[index:])
	header.valueSize = uint32(valueSize)
	index += n

	return header, int64(index)

}

func getLogRecordCRC(logRecord *LogRecord, header []byte) uint32 {
	if logRecord == nil {
		return 0
	}
	// 先计算header部分的CRC校验值
	crc := crc32.ChecksumIEEE(header[:])
	// 然后依次将key和value的数据更新到CRC校验值中
	// 这样做是为了确保整个LogRecord数据的完整性，包括header、key和value
	crc = crc32.Update(crc, crc32.IEEETable, logRecord.Key)
	crc = crc32.Update(crc, crc32.IEEETable, logRecord.Value)
	return crc
}

func EncodeLogRecordPos(pos *Position) []byte {
	buf := make([]byte, binary.MaxVarintLen32*2+binary.MaxVarintLen64)
	var index = 0
	index += binary.PutVarint(buf[index:], int64(pos.Fid))
	index += binary.PutVarint(buf[index:], pos.Offset)
	index += binary.PutVarint(buf[index:], int64(pos.Size))
	return buf[:index]
}

func DecodeLogRecordPos(buf []byte) *Position {
	var index = 0
	fileId, n := binary.Varint(buf[index:])
	index += n
	offset, n := binary.Varint(buf[index:])
	index += n
	size, _ := binary.Varint(buf[index:])
	return &Position{Fid: uint32(fileId), Offset: offset, Size: uint32(size)}
}
