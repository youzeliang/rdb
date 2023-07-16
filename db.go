package main

import (
	"rdb/data"
	"rdb/index"
	"sync"
)

// DB bitcask db
type DB struct {
	options    Options
	mu         *sync.RWMutex
	activeFile *data.DataFile            // 当前活跃数据文件，可以用于写入
	OlderFiles map[uint32]*data.DataFile // 旧的数据文件。只能用于读
	indexer    index.Indexer             // 内存索引

}

// Put 写入Key/Value 数据, key 不能为空
func (db *DB) Put(key []byte, value []byte) error {
	if len(key) == 0 {
		return ErrKeyIsEmpty
	}

	// 构造 LogRecord 结构体

	logRecord := data.LogRecord{
		Key:   key,
		Value: value,
		Type:  data.LogRecordNormal,
	}

	// 追加写入到当前活跃数据文件中
	pos, err := db.appendLogRecord(logRecord)
	if err != nil {
		return err
	}

	// 更新内存索引

	if ok := db.indexer.Put(key, pos); !ok {
		return ErrIndexUpdate
	}

	return nil

}

func (db *DB) Get(key []byte) ([]byte, error) {
	db.mu.Lock()
	defer db.mu.Unlock()
	if len(key) == 0 {
		return nil, ErrKeyIsEmpty
	}

	// 从内存索引中获取数据文件的位置信息
	logRecordPos := db.indexer.Get(key)
	// 如果内存索引中没有，则说明key不存在
	if logRecordPos == nil {
		return nil, ErrKeyNotFound
	}

	// 根据文件id 找到对应的数据文件

	var dataFile *data.DataFile

	if db.activeFile.FileId == logRecordPos.Fid {
		dataFile = db.activeFile
	} else {
		dataFile = db.OlderFiles[logRecordPos.Fid]
	}

	// 数据文件为空
	if dataFile == nil {
		return nil, ErrDataFileNotFound
	}

	// 根据偏移读取对应的数据
	logRecord, err := dataFile.ReadLogRecord(logRecordPos.Offset)
	if err != nil {
		return nil, err
	}

	// 如果数据被删除了，则返回空
	if logRecord.Type == data.LogRecordDeleted {
		return nil, ErrKeyNotFound
	}

	return logRecord.Value, nil
}

// 追加写入到当前活跃数据文件中
func (db *DB) appendLogRecord(logRecord data.LogRecord) (*data.LogRecordPos, error) {
	db.mu.Lock()
	defer db.mu.Unlock()

	// 判断当前活跃数据文件是否存在，因为数据库在没有写入的时候是没有文件生成的

	if db.activeFile == nil {
		if err := db.setActivityDataFile(); err != nil {
			return nil, err
		}
	}

	// 写入数据编码
	enRecord, size := data.EncodeLodRecord(logRecord)

	// 如果写入的数据已经到达了活跃文件的最大值，则关闭活跃文件，并打开新的文件

	if db.activeFile.WriteOff+size > db.options.DataFileSize {

		// 先持久化数据文件，保证已有的数据持久到磁盘中去
		if err := db.activeFile.Sync(); err != nil {
			return nil, err
		}

		// 当前活跃文件转化为久的数据文件
		db.OlderFiles[db.activeFile.FileId] = db.activeFile

		// 打开新的数据文件
		if err := db.setActivityDataFile(); err != nil {
			return nil, err
		}
	}

	writeOff := db.activeFile.WriteOff
	if err := db.activeFile.Write(enRecord); err != nil {
		return nil, err
	}

	// 根据用户配置决定是否持久化
	if db.options.SyncWrite {
		if err := db.activeFile.Sync(); err != nil {
			return nil, err
		}
	}

	pos := &data.LogRecordPos{
		Fid:    db.activeFile.FileId,
		Offset: writeOff,
	}

	return pos, nil
}

// 设置当前活跃文件
// 在访问此方法前必须持有互斥锁
func (db *DB) setActivityDataFile() error {

	var initialFaileId uint32 = 0

	if db.activeFile != nil {
		initialFaileId = db.activeFile.FileId + 1
	}

	// 打开新的数据文件

	dataFile, err := data.OpenDataFile(db.options.DirPath, initialFaileId)
	if err != nil {
		return err
	}

	db.activeFile = dataFile

	return nil

}
