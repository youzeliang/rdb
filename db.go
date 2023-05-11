package rdb

import (
	"rdb/data"
	"sync"
)

// DB bitcask db
type DB struct {
	mu         *sync.RWMutex
	activeFile *data.DataFile            // 当前活跃数据文件，可以用于写入
	OlderFiles map[uint32]*data.DataFile // 旧的数据文件。只能用于读
}

func (db *DB) Put(key []byte, value []byte) error {
	if len(key) == 0 {
		return ErrKeyIsEmpty
	}

	//return nil

	// 构造 LogRecord 结构体

	log_record := data.LogRecord{
		Key:   key,
		Value: value,
		Type:  data.LogRecordNormal,
	}

}

func (db *DB) appendLogRecord(logRecord data.LogRecord) (*data.LogRecordPos, error) {
	db.mu.Lock()
	defer db.mu.Unlock()

	// 判断当前活跃数据文件是否存在，因为数据库在没有写入的时候是没有文件生成的

	if db.activeFile == nil {

	}
}

// 设置当前活跃文件
func (db *DB) setActivityDataFile() error {

	var initialFaileId uint32 = 0

	if db.activeFile != nil {
		initialFaileId = db.activeFile.FileId
	}

}
