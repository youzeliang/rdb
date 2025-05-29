package rdb

import (
	"errors"
	"fmt"
	"github.com/gofrs/flock"
	"github.com/youzeliang/rdb/data"
	"github.com/youzeliang/rdb/fio"
	"github.com/youzeliang/rdb/index"
	"github.com/youzeliang/rdb/utils"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
)

const (
	seqNoKey     = "seq.no"
	fileLockName = "flock"
)

// DB represents a key-value storage engine instance.
type DB struct {
	config                Options
	mutex                 *sync.RWMutex
	activeFile            *data.DataFile            // 当前活跃数据文件，可以用于写入
	dataFileIDs           []int                     // 文件 id，只能在加载索引的时候使用，不能在其他的地方更新和使用
	archivedFiles         map[uint32]*data.DataFile // 已归档的只读数据文件
	index                 index.Indexer             // 内存索引
	transactionID         uint64                    // 全局递增的事务ID
	isMerging             bool                      // 是否正在 merge
	seqNoFileExists       bool                      // 存储事务序列号的文件是否存在
	isInitial             bool                      // 是否是第一次初始化此数据目录
	fileLock              *flock.Flock              // 文件锁
	bytesWrittenSinceSync int                       // 当前累计写了多少个字节
	reclaimSize           int64                     // 表示有多少数据是无效的
}

// Stat 存储引擎统计信息
type Stat struct {
	KeyNum          uint  // key 的总数量
	DataFileNum     uint  // 数据文件的数量
	ReclaimableSize int64 // 可以进行 merge 回收的数据量，字节为单位
	DiskSize        int64 // 数据目录所占磁盘空间大小
}

// Open opens or creates a DB at the specified path with the given config.
// If the directory does not exist, it will be created.
func Open(options Options) (*DB, error) {
	if err := checkOptions(options); err != nil {
		return nil, fmt.Errorf("invalid config: %v", err)
	}

	var isInitial bool
	if err := os.MkdirAll(options.DirPath, os.ModePerm); err != nil {
		return nil, fmt.Errorf("failed to create directory: %v", err)
	}

	// Check if database is already in use
	fileLock := flock.New(filepath.Join(options.DirPath, fileLockName))
	hold, err := fileLock.TryLock()
	if err != nil {
		return nil, fmt.Errorf("failed to lock database: %v", err)
	}
	if !hold {
		return nil, ErrDatabaseIsUsing
	}

	entries, err := os.ReadDir(options.DirPath)
	if err != nil {
		return nil, fmt.Errorf("failed to read directory: %v", err)
	}
	isInitial = len(entries) == 0

	// 初始化 DB 实例结构体
	db := &DB{
		config:        options,
		mutex:         new(sync.RWMutex),
		archivedFiles: make(map[uint32]*data.DataFile),
		index:         index.NewIndexer(options.IndexType, options.DirPath, options.SyncWrites),
		isInitial:     isInitial,
		fileLock:      fileLock,
	}

	// Load existing data
	if err := db.loadMergeFiles(); err != nil {
		return nil, fmt.Errorf("failed to load merge files: %v", err)
	}

	if err := db.loadDataFiles(); err != nil {
		return nil, fmt.Errorf("failed to load data files: %v", err)
	}

	// Handle index loading based on index type
	if options.IndexType != BPlusTree {
		if err := db.loadIndexFromHintFile(); err != nil {
			return nil, fmt.Errorf("failed to load hint index: %v", err)
		}

		if err := db.loadIndexFromDataFiles(); err != nil {
			return nil, fmt.Errorf("failed to load data files index: %v", err)
		}

		if options.MMapAtStartup {
			if err := db.resetIoType(); err != nil {
				return nil, fmt.Errorf("failed to reset IO type: %v", err)
			}
		}
	}

	// 取出当前事务序列号
	if options.IndexType == BPlusTree {
		if err := db.loadSeqNo(); err != nil {
			return nil, fmt.Errorf("failed to load sequence number: %v", err)
		}
		if db.activeFile != nil {
			size, err := db.activeFile.IoManager.Size()
			if err != nil {
				return nil, fmt.Errorf("failed to get active file size: %v", err)
			}
			db.activeFile.WriteOff = size
		}
	}

	return db, nil
}

// Close 关闭数据库
func (db *DB) Close() error {
	defer func() {
		_ = db.fileLock.Unlock()
	}()
	if db.activeFile == nil {
		return nil
	}
	db.mutex.Lock()
	defer db.mutex.Unlock()

	// 关闭索引
	if err := db.index.Close(); err != nil {
		return err
	}

	// 保存当前事务序列号
	seqNoFile, err := data.OpenSeqNoFile(db.config.DirPath)
	if err != nil {
		return err
	}
	record := &data.LogRecord{
		Key:   []byte(seqNoKey),
		Value: []byte(strconv.FormatUint(db.transactionID, 10)),
	}
	encRecord, _ := data.EncodeLogRecord(record)
	if err := seqNoFile.Write(encRecord); err != nil {
		return fmt.Errorf("failed to write sequence number: %v", err)
	}
	if err := seqNoFile.Sync(); err != nil {
		return fmt.Errorf("failed to sync sequence number file: %v", err)
	}

	//	关闭当前活跃文件
	if err := db.activeFile.Close(); err != nil {
		return fmt.Errorf("failed to close active file: %v", err)
	}
	// 关闭旧的数据文件
	for _, file := range db.archivedFiles {
		if err := file.Close(); err != nil {
			return fmt.Errorf("failed to close data file: %v", err)
		}
	}
	return nil
}

// ListKeys 获取数据库中所有的 key
func (db *DB) ListKeys() [][]byte {
	iterator := db.index.Iterator(false)
	keys := make([][]byte, db.index.Size())
	var idx int
	for iterator.Rewind(); iterator.Valid(); iterator.Next() {
		keys[idx] = iterator.Key()
		idx++
	}
	return keys
}

// Put stores a key-value pair in the database.
// If the key already exists, its previous value will be overwritten.
func (db *DB) Put(key []byte, value []byte) error {
	if len(key) == 0 {
		return ErrKeyIsEmpty
	}

	logRecord := &data.LogRecord{
		Key:   logRecordKeyWithSeq(key, nonTransactionSeqNo),
		Value: value,
		Type:  data.LogRecordNormal,
	}

	pos, err := db.appendLogRecordWithLock(logRecord)
	if err != nil {
		return fmt.Errorf("failed to append log record: %v", err)
	}

	if oldPos := db.index.Put(key, pos); oldPos != nil {
		db.reclaimSize += int64(oldPos.Size)
	}
	return nil
}

// Backup 备份数据库，将数据文件拷贝到新的目录中
func (db *DB) Backup(dir string) error {
	db.mutex.RLock()
	defer db.mutex.RUnlock()
	return utils.CopyDir(db.config.DirPath, dir, []string{fileLockName})
}

// Delete removes the value for the given key.
// If the key does not exist, no error is returned.
func (db *DB) Delete(key []byte) error {
	if len(key) == 0 {
		return ErrKeyIsEmpty
	}

	// Check if key exists
	if pos := db.index.Get(key); pos == nil {
		return nil
	}

	// 构造 LogRecord 结构体,标识其是被删除的
	logRecord := &data.LogRecord{
		Key:  logRecordKeyWithSeq(key, nonTransactionSeqNo),
		Type: data.LogRecordDeleted,
	}

	pos, err := db.appendLogRecordWithLock(logRecord)
	if err != nil {
		return fmt.Errorf("failed to append delete record: %v", err)
	}

	db.reclaimSize += int64(pos.Size)

	oldPos, ok := db.index.Delete(key)
	if !ok {
		return ErrIndexUpdateFailed
	}
	if oldPos != nil {
		db.reclaimSize += int64(oldPos.Size)
	}
	return nil
}

// Get retrieves the value for the given key.
// Returns ErrKeyNotFound if the key does not exist.
func (db *DB) Get(key []byte) ([]byte, error) {
	if len(key) == 0 {
		return nil, ErrKeyIsEmpty
	}

	db.mutex.RLock()
	defer db.mutex.RUnlock()

	logRecordPos := db.index.Get(key)
	if logRecordPos == nil {
		return nil, ErrKeyNotFound
	}

	return db.getValueByPosition(logRecordPos)
}

func (db *DB) appendLogRecordWithLock(logRecord *data.LogRecord) (*data.Position, error) {
	db.mutex.Lock()
	defer db.mutex.Unlock()
	return db.appendLogRecord(logRecord)
}

// appendLogRecord appends a log record to the active data file.
// This method must be called with the write lock held.
func (db *DB) appendLogRecord(logRecord *data.LogRecord) (*data.Position, error) {
	if db.activeFile == nil {
		if err := db.setActiveDataFile(); err != nil {
			return nil, fmt.Errorf("failed to set active data file: %v", err)
		}
	}

	encRecord, size := data.EncodeLogRecord(logRecord)

	// Check if we need to rotate to a new data file
	if db.activeFile.WriteOff+size > db.config.DataFileSize {
		// Sync current file before rotation
		if err := db.activeFile.Sync(); err != nil {
			return nil, fmt.Errorf("failed to sync active file: %v", err)
		}

		// Move current file to older files
		db.archivedFiles[db.activeFile.FileId] = db.activeFile

		// Create new active file
		if err := db.setActiveDataFile(); err != nil {
			return nil, fmt.Errorf("failed to create new active file: %v", err)
		}
	}

	writeOff := db.activeFile.WriteOff
	if err := db.activeFile.Write(encRecord); err != nil {
		return nil, fmt.Errorf("failed to write log record: %v", err)
	}

	db.bytesWrittenSinceSync += int(size)

	// Handle sync based on configuration
	needSync := db.config.SyncWrites
	if !needSync && db.config.BytesPerSync > 0 && db.bytesWrittenSinceSync >= db.config.BytesPerSync {
		needSync = true
	}

	if needSync {
		if err := db.activeFile.Sync(); err != nil {
			return nil, fmt.Errorf("failed to sync file: %v", err)
		}
		db.bytesWrittenSinceSync = 0
	}

	return &data.Position{
		Fid:    db.activeFile.FileId,
		Offset: writeOff,
		Size:   uint32(size),
	}, nil
}

// 设置当前活跃文件
// 在访问此方法前必须持有互斥锁

func (db *DB) setActiveDataFile() error {
	var initialFileId uint32 = 0

	if db.activeFile != nil {
		initialFileId = db.activeFile.FileId + 1
	}

	dataFile, err := data.OpenDataFile(db.config.DirPath, initialFileId, fio.StandardFIO)
	if err != nil {
		return err
	}
	db.activeFile = dataFile

	return nil
}

// Fold 获取所有的数据，并执行用户指定的操作，函数返回 false 时终止遍历
func (db *DB) Fold(fn func(key []byte, value []byte) bool) error {
	db.mutex.RLock()
	defer db.mutex.RUnlock()

	iterator := db.index.Iterator(false)
	defer iterator.Close()
	for iterator.Rewind(); iterator.Valid(); iterator.Next() {
		value, err := db.getValueByPosition(iterator.Value())
		if err != nil {
			return err
		}
		if !fn(iterator.Key(), value) {
			break
		}
	}
	return nil
}

// Sync 持久化数据文件
func (db *DB) Sync() error {
	if db.activeFile == nil {
		return nil
	}
	db.mutex.Lock()
	defer db.mutex.Unlock()
	return db.activeFile.Sync()
}

// Stat 返回数据库的相关统计信息
func (db *DB) Stat() *Stat {
	db.mutex.RLock()
	defer db.mutex.RUnlock()

	dirSize, err := utils.DirSize(db.config.DirPath)
	if err != nil {
		return nil
	}

	dataFiles := uint(len(db.archivedFiles))
	if db.activeFile != nil {
		dataFiles++
	}

	return &Stat{
		KeyNum:          uint(db.index.Size()),
		DataFileNum:     dataFiles,
		ReclaimableSize: db.reclaimSize,
		DiskSize:        dirSize,
	}
}

// getValueByPosition retrieves a value from the data files using its position.
func (db *DB) getValueByPosition(pos *data.Position) ([]byte, error) {
	// Find the correct data file
	var dataFile *data.DataFile
	if db.activeFile.FileId == pos.Fid {
		dataFile = db.activeFile
	} else {
		dataFile = db.archivedFiles[pos.Fid]
	}

	if dataFile == nil {
		return nil, ErrDataFileNotFound
	}

	// 根据偏移读取对应的数据
	logRecord, _, err := dataFile.ReadLogRecord(pos.Offset)
	if err != nil {
		return nil, fmt.Errorf("failed to read log record: %v", err)
	}

	if logRecord.Type == data.LogRecordDeleted {
		return nil, ErrKeyNotFound
	}

	return logRecord.Value, nil
}

// loadDataFiles loads all data files from the database directory.
func (db *DB) loadDataFiles() error {
	files, err := os.ReadDir(db.config.DirPath)
	if err != nil {
		return fmt.Errorf("failed to read directory: %v", err)
	}

	var fileIds []int
	for _, file := range files {
		if strings.HasSuffix(file.Name(), data.DataFileNameSuffix) {
			splitNames := strings.Split(file.Name(), ".")
			fileId, err := strconv.Atoi(splitNames[0])
			if err != nil {
				return ErrDataDirectoryCorrupted
			}
			fileIds = append(fileIds, fileId)
		}
	}

	sort.Ints(fileIds)
	db.dataFileIDs = fileIds

	// Open all data files
	for i, fid := range fileIds {
		ioType := fio.StandardFIO
		if db.config.MMapAtStartup {
			ioType = fio.MemoryMap
		}

		dataFile, err := data.OpenDataFile(db.config.DirPath, uint32(fid), ioType)
		if err != nil {
			return fmt.Errorf("failed to open data file %d: %v", fid, err)
		}

		if i == len(fileIds)-1 {
			db.activeFile = dataFile
		} else {
			db.archivedFiles[uint32(fid)] = dataFile
		}
	}

	return nil
}

func (db *DB) loadIndexFromDataFiles() error {
	// 没有文件，说明数据库是空的，直接返回
	if len(db.dataFileIDs) == 0 {
		return nil
	}

	// 查看是否有过merge
	hasMerge, nonMergeFileId := false, uint32(0)
	mergeFileName := filepath.Join(db.config.DirPath, data.MergeFinishedFileName)
	if _, err := os.Stat(mergeFileName); err == nil {
		fid, err := db.getNonMergeFileId(db.config.DirPath)
		if err != nil {
			return err
		}
		hasMerge = true
		nonMergeFileId = fid
	}

	updateIndex := func(key []byte, typ data.LogRecordType, pos *data.Position) {
		var oldPos *data.Position
		if typ == data.LogRecordDeleted {
			oldPos, _ = db.index.Delete(key)
			db.reclaimSize += int64(pos.Size)
		} else {
			oldPos = db.index.Put(key, pos)
		}
		if oldPos != nil {
			db.reclaimSize += int64(oldPos.Size)
		}
	}

	// 暂存事务数据
	transactionRecords := make(map[uint64][]*data.TransactionRecord)
	var currentSeqNo = nonTransactionSeqNo

	// 遍历所有的文件id，处理文件中的记录
	for i, fid := range db.dataFileIDs {
		var fileId = uint32(fid)
		// 如果比最近未参与的merge文件id小，则说明已经从Hint文件中加载索引了
		if hasMerge && fileId < nonMergeFileId {
			continue
		}

		var dataFile *data.DataFile
		if fileId == db.activeFile.FileId {
			dataFile = db.activeFile
		} else {
			dataFile = db.archivedFiles[fileId]
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
			logRecordPos := &data.Position{Fid: fileId, Offset: offset, Size: uint32(size)}

			// 解析 key，拿到事务序列号
			realKey, seqNo := parseLogRecordKey(logRecord.Key)
			if seqNo == nonTransactionSeqNo {
				// 非事务操作，直接更新内存索引
				updateIndex(realKey, logRecord.Type, logRecordPos)
			} else {
				// 事务完成，对应的 seq no 的数据可以更新到内存索引中
				if logRecord.Type == data.LogRecordTxnFinished {
					for _, txnRecord := range transactionRecords[seqNo] {
						updateIndex(txnRecord.Record.Key, txnRecord.Record.Type, txnRecord.Pos)
					}
					delete(transactionRecords, seqNo)
				} else {
					logRecord.Key = realKey
					transactionRecords[seqNo] = append(transactionRecords[seqNo], &data.TransactionRecord{
						Record: logRecord,
						Pos:    logRecordPos,
					})
				}
			}

			// 更新事务序列号,防止在重启后拿到最新的序列号
			if seqNo > currentSeqNo {
				currentSeqNo = seqNo
			}

			// 递增 offset，下一次从新的位置开始读取
			offset += size
		}

		// 如果是当前活跃文件，更新这个文件的 WriteOff
		if i == len(db.dataFileIDs)-1 {
			db.activeFile.WriteOff = offset
		}
	}

	// 更新事务序列号
	db.transactionID = currentSeqNo
	return nil
}

func (db *DB) resetDataFileIoType() error {
	if db.activeFile == nil {
		return nil
	}
	if err := db.activeFile.IoManager.Close(); err != nil {
		return err
	}
	ioManager, err := fio.NewFileIOManager(data.GetDataFileName(db.config.DirPath, db.activeFile.FileId))
	if err != nil {
		return err
	}
	db.activeFile.IoManager = ioManager
	for _, file := range db.archivedFiles {
		if err := file.IoManager.Close(); err != nil {
			return err
		}
		ioManager, err := fio.NewFileIOManager(data.GetDataFileName(db.config.DirPath, file.FileId))
		if err != nil {
			return err
		}
		file.IoManager = ioManager
	}
	return nil
}

func (db *DB) loadSeqNo() error {
	fileName := filepath.Join(db.config.DirPath, data.SeqNoFileName)
	if _, err := os.Stat(fileName); os.IsNotExist(err) {
		return nil
	}

	seqNoFile, err := data.OpenSeqNoFile(db.config.DirPath)
	if err != nil {
		return err
	}
	record, _, err := seqNoFile.ReadLogRecord(0)
	seqNo, err := strconv.ParseUint(string(record.Value), 10, 64)
	if err != nil {
		return err
	}
	db.transactionID = seqNo
	db.seqNoFileExists = true

	return os.Remove(fileName)
}

// 将数据文件的 IO 类型设置为标准文件 IO
func (db *DB) resetIoType() error {
	if db.activeFile == nil {
		return nil
	}

	if err := db.activeFile.SetIOManager(db.config.DirPath, fio.StandardFIO); err != nil {
		return err
	}
	for _, dataFile := range db.archivedFiles {
		if err := dataFile.SetIOManager(db.config.DirPath, fio.StandardFIO); err != nil {
			return err
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
	if options.DataFileMergeRatio < 0 || options.DataFileMergeRatio > 1 {
		return errors.New("invalid merge ratio, must between 0 and 1")
	}
	return nil
}
