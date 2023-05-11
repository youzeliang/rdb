package data

type LogRecordPos struct {
	Fid    uint32 // file id
	Offset int64  // Offset refers to the position where data is stored in a data file.
}

type LogRecordType = byte

const (
	LogRecordNormal LogRecordType = iota
	LogRecordDeleted
)

// LogRecord 写入到数据文件的记录
// 之所以叫日志，是因为数据文件中的数据是追加写入的，类似日志的格式
type LogRecord struct {
	Key   []byte
	Value []byte
	Type  LogRecordType
}

// LogRecordSize 进行编码, 返回字节数组及其长度
func EncodeLodRecord(logRecord LogRecord) ([]byte, int64) {
	return nil, 0
}
