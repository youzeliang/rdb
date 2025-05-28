package rdb

import (
	"errors"
	"io"
	"io/ioutil"
	"os"
	"rdb/data"
	"rdb/index"
	"sort"
	"strconv"
	"strings"
	"sync"
)

// DB bitcask 存储引擎实例
type DB struct {
	options    Options
	mu         *sync.RWMutex
	fileIds    []int                     // 文件 id，只能在加载索引的时候使用，不能在其他的地方更新和使用
	activeFile *data.DataFile            // 当前活跃数据文件，可以用于写入
	OlderFiles map[uint32]*data.DataFile // 旧的数据文件。只能用于读
	index      index.Indexer             // 内存索引

}

func Open(options Options) (*DB, error) {
	// 对用户传入的配置项进行校验
	if err := checkOptions(options); err != nil {
		return nil, err
	}

	// 判断数据目录是否存在，如果不存在的话，则创建这个目录

	if _, err := os.Stat(options.DirPath); os.IsNotExist(err) {
		if err := os.MkdirAll(options.DirPath, os.ModePerm); err != nil {
			return nil, err
		}
	}

	// 初始化 DB 实例结构体

	db := &DB{
		options:    options,
		mu:         new(sync.RWMutex),
		OlderFiles: make(map[uint32]*data.DataFile),
		index:      index.NewIndexer(options.IndexType),
	}

	// 加载数据文件
	if err := db.loadDataFiles(); err != nil {
		return nil, err
	}

	if err := db.loadIndexFromDataFile(); err != nil {
		return nil, err
	}
	return db, nil
}

// Put 写入Key/Value 数据, key 不能为空
func (db *DB) Put(key []byte, value []byte) error {
	if len(key) == 0 {
		return ErrKeyIsEmpty
	}

	// 构造 LogRecord 结构体

	logRecord := &data.LogRecord{
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

	if ok := db.index.Put(key, pos); !ok {
		return ErrIndexUpdateFailed
	}

	return nil

}

func (db *DB) Delete(key []byte) error {

	// 判断 key 是否为空

	if len(key) == 0 {
		return ErrKeyIsEmpty
	}

	// 先检测 key 是否存在，如果不存在，则直接返回

	if pos := db.index.Get(key); pos == nil {
		return nil
	}

	// 构造 LogRecord，标识其是被删除的
	logRecord := &data.LogRecord{
		Key:  key,
		Type: data.LogRecordDeleted,
	}

	// 写入到数据文件中去

	_, err := db.appendLogRecord(logRecord)
	if err != nil {
		return err
	}

	ok := db.index.Delete(key)
	if !ok {
		return ErrIndexUpdateFailed
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
	logRecordPos := db.index.Get(key)
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
	logRecord, _, err := dataFile.ReadLogRecord(logRecordPos.Offset)
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
func (db *DB) appendLogRecord(logRecord *data.LogRecord) (*data.LogRecordPos, error) {
	db.mu.Lock()
	defer db.mu.Unlock()

	// 判断当前活跃数据文件是否存在，因为数据库在没有写入的时候是没有文件生成的

	if db.activeFile == nil {
		if err := db.setActivityDataFile(); err != nil {
			return nil, err
		}
	}

	// 写入数据编码
	enRecord, size := data.EncodeLogRecord(logRecord)

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

// 从磁盘中加载索引
func (db *DB) loadDataFiles() error {

	dirEntries, err := ioutil.ReadDir(db.options.DirPath)
	if err != nil {
		return err
	}

	var fileIds []int

	// 遍历传入的配置路径下的数据目录，找到所有以 .data 结尾的数据文件
	for _, dirEntry := range dirEntries {
		if strings.HasPrefix(dirEntry.Name(), data.DataFileNameSuffix) {
			splitNames := strings.Split(dirEntry.Name(), ".")
			fileId, err := strconv.Atoi(splitNames[0])
			// 防止数据目录有可能被损坏
			if err != nil {
				return ErrDataFileNotFound
			}
			fileIds = append(fileIds, fileId)
		}
	}

	// 这里对文件id进行排序, 从小到大依次加载

	sort.Ints(fileIds)

	db.fileIds = fileIds

	// 遍历每个文件id, 打开对应的数据文件

	for i, fid := range fileIds {
		dataFile, err := data.OpenDataFile(db.options.DirPath, uint32(fid))
		if err != nil {
			return err
		}

		// 最后一个，id是最大的，前面排过序了，说明是当前活跃文件
		if i == len(fileIds)-1 {
			db.activeFile = dataFile
		} else {
			db.OlderFiles[uint32(fid)] = dataFile
		}
	}

	return nil
}

// 从数据文件中加载索引，遍历文件中的所有记录，并更新到内存索引中

func (db *DB) loadIndexFromDataFile() error {

	// 没有文件，说明数据库是空的，直接返回
	if len(db.fileIds) == 0 {
		return nil
	}

	// 遍历所有文件id，处理文件中的记录

	for i, fid := range db.fileIds {

		var fileId = uint32(fid)
		var dataFile *data.DataFile

		if fileId == db.activeFile.FileId {
			dataFile = db.activeFile
		} else {
			dataFile = db.OlderFiles[fileId]
		}

		var offset int64 = 0

		for {

			logRecord, size, err := dataFile.ReadLogRecord(offset)
			if err != nil {
				if err == io.EOF {
					break
				}
				return err
			}

			// 构造内存索引并保存
			logRecordPos := &data.LogRecordPos{
				Fid:    fileId,
				Offset: offset,
			}

			if logRecord.Type == data.LogRecordDeleted {
				db.index.Delete(logRecord.Key)
			} else {
				db.index.Put(logRecord.Key, logRecordPos)
			}

			// 递增offset，下一次从新的位置开始读取

			offset += size

		}

		// 如果是当前活跃文件，更新这个文件的 WriteOff

		if i == len(db.fileIds)-1 {
			db.activeFile.WriteOff = offset
		}

	}

	return nil
}

func checkOptions(options Options) error {
	if options.DirPath == "" {
		return errors.New("database dir path is empty")
	}

	if options.DataFileSize <= 0 {
		return errors.New("database data file size must be greater than 0")
	}
	return nil
}
