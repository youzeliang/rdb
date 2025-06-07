package rdb

import (
	"github.com/youzeliang/rdb/data"
	"github.com/youzeliang/rdb/utils"
	"io"
	"os"
	"path"
	"path/filepath"
	"sort"
	"strconv"
)

const (
	mergeDirName     = "-merge"
	mergeFinishedKey = "merge.finished"
)

func (db *DB) Merge() error {
	// 如果数据库为空，则直接返回
	if db.activeFile == nil {
		return nil
	}
	db.mutex.Lock()
	// 如果 merge 正在进行当中，则直接返回
	if db.isMerging {
		db.mutex.Unlock()
		return ErrMergeInProgress
	}

	// 查看可以 merge 的数据量是否达到了阈值
	totalSize, err := utils.DirSize(db.config.DirPath)
	if err != nil {
		db.mutex.Unlock()
		return err
	}
	if float32(db.reclaimSize)/float32(totalSize) < db.config.DataFileMergeRatio {
		db.mutex.Unlock()
		return ErrMergeRatioUnreached
	}

	// 查看剩余的空间容量是否可以容纳 merge 之后的数据量
	availableDiskSize, err := utils.AvailableDiskSize()
	if err != nil {
		db.mutex.Unlock()
		return err
	}
	if uint64(totalSize-db.reclaimSize) >= availableDiskSize {
		db.mutex.Unlock()
		return ErrNoEnoughSpaceForMerge
	}

	db.isMerging = true
	defer func() {
		db.isMerging = false
	}()

	// 持久化当前活跃文件
	if err := db.activeFile.Sync(); err != nil {
		db.mutex.Unlock()
		return err
	}
	// 将当前活跃文件转换为旧的数据文件
	db.archivedFiles[db.activeFile.FileId] = db.activeFile
	// 打开新的活跃文件
	if err := db.setActiveDataFile(); err != nil {
		db.mutex.Unlock()
		return nil
	}
	// 记录最近没有参与 merge 的文件 id
	nonMergeFileId := db.activeFile.FileId

	// 取出所有需要 merge 的文件
	var mergeFiles []*data.DataFile
	for _, file := range db.archivedFiles {
		mergeFiles = append(mergeFiles, file)
	}
	db.mutex.Unlock()

	//	待 merge 的文件从小到大进行排序，依次 merge
	sort.Slice(mergeFiles, func(i, j int) bool {
		return mergeFiles[i].FileId < mergeFiles[j].FileId
	})

	mergePath := db.getMergePath()
	// 如果目录存在，说明发生过 merge，将其删除掉
	if _, err := os.Stat(mergePath); err == nil {
		if err := os.RemoveAll(mergePath); err != nil {
			return err
		}
	}
	// 新建一个 merge path 的目录
	if err := os.MkdirAll(mergePath, os.ModePerm); err != nil {
		return err
	}
	// 打开一个新的临时 bitcask 实例
	mergeConfigs := db.config
	mergeConfigs.DirPath = mergePath
	mergeConfigs.SyncWrites = false
	mergeDB, err := Open(mergeConfigs)
	if err != nil {
		return err
	}

	// 打开 hint 文件存储索引
	hintFile, err := data.OpenHintFile(mergePath)
	if err != nil {
		return err
	}
	// 遍历处理每个数据文件
	for _, dataFile := range mergeFiles {
		var offset int64 = 0
		for {
			logRecord, size, err := dataFile.ReadLogRecord(offset)
			if err != nil {
				if err == io.EOF {
					break
				}
				return err
			}
			// 解析拿到实际的 key
			realKey, _ := parseLogRecordKey(logRecord.Key)
			logRecordPos := db.index.Get(realKey)
			// 和内存中的索引位置进行比较，如果有效则重写
			if logRecordPos != nil &&
				logRecordPos.Fid == dataFile.FileId &&
				logRecordPos.Offset == offset {
				// 清除事务标记
				logRecord.Key = logRecordKeyWithSeq(realKey, nonTransactionSeqNo)
				pos, err := mergeDB.appendLogRecord(logRecord)
				if err != nil {
					return err
				}
				// 将当前位置索引写到 Hint 文件当中
				if err := hintFile.WriteHintRecord(realKey, pos); err != nil {
					return err
				}
			}
			// 增加 offset
			offset += size
		}
	}

	// sync 保证持久化
	if err := hintFile.Sync(); err != nil {
		return err
	}
	if err := mergeDB.Sync(); err != nil {
		return err
	}

	// 写标识 merge 完成的文件
	mergeFinishedFile, err := data.OpenMergeFinishedFile(mergePath)
	if err != nil {
		return err
	}
	mergeFinRecord := &data.LogRecord{
		Key:   []byte(mergeFinishedKey),
		Value: []byte(strconv.Itoa(int(nonMergeFileId))),
	}
	encRecord, _ := data.EncodeLogRecord(mergeFinRecord)
	if err := mergeFinishedFile.Write(encRecord); err != nil {
		return err
	}
	if err := mergeFinishedFile.Sync(); err != nil {
		return err
	}

	return nil
}

func (db *DB) getMergePath() string {
	dir := path.Dir(path.Clean(db.config.DirPath))
	base := path.Base(db.config.DirPath)
	return filepath.Join(dir, base+mergeDirName)
}

func (db *DB) getNonMergeFileId(dirPath string) (uint32, error) {
	mergeFinishedFile, err := data.OpenMergeFinishedFile(dirPath)
	if err != nil {
		return 0, err
	}
	record, _, err := mergeFinishedFile.ReadLogRecord(0)
	if err != nil {
		return 0, err
	}
	nonMergeFileId, err := strconv.Atoi(string(record.Value))
	if err != nil {
		return 0, err
	}

	return uint32(nonMergeFileId), nil
}

// 加载merge数据目录
func (db *DB) loadMergeFiles() error {
	mergePath := db.getMergeDirPath()
	if _, err := os.Stat(mergePath); os.IsNotExist(err) {
		return nil
	}
	defer func() {
		_ = os.RemoveAll(mergePath)
	}()

	dirEntries, err := os.ReadDir(mergePath)
	if err != nil {
		return err
	}

	//	遍历查找是否完成了 merge
	var mergeFinished bool
	var fileNames []string
	for _, entry := range dirEntries {
		// 说明是全部完成了
		if entry.Name() == data.MergeFinishedFileName {
			mergeFinished = true
		}

		if entry.Name() == data.SeqNoFileName {
			continue
		}
		if entry.Name() == fileLockName {
			continue
		}

		fileNames = append(fileNames, entry.Name())
	}
	// 没有merge直接返回
	if !mergeFinished {
		return nil
	}

	nonMergeFileId, err := db.getNonMergeFileId(mergePath)
	if err != nil {
		return err
	}
	//	先删除旧的数据文件
	var fileId uint32 = 0
	for ; fileId < nonMergeFileId; fileId++ {
		fileName := data.GetDataFileName(db.config.DirPath, fileId)
		if _, err := os.Stat(fileName); err == nil {
			if err := os.Remove(fileName); err != nil {
				return err
			}
		}
	}

	// 移动文件
	for _, fileName := range fileNames {
		srcPath := filepath.Join(mergePath, fileName)
		destPath := filepath.Join(db.config.DirPath, fileName)
		if err := os.Rename(srcPath, destPath); err != nil {
			return err
		}
	}
	return nil
}

func (db *DB) getMergeDirPath() string {
	dir := path.Dir(path.Clean(db.config.DirPath))
	base := path.Base(db.config.DirPath)
	return filepath.Join(dir, base+mergeDirName)
}

func (db *DB) loadIndexFromHintFile() error {
	hintFileName := filepath.Join(db.config.DirPath, data.HintFileName)
	if _, err := os.Stat(hintFileName); os.IsNotExist(err) {
		return nil
	}

	hintFile, err := data.OpenHintFile(db.config.DirPath)
	if err != nil {
		return err
	}

	var offset int64 = 0
	for {
		logRecord, size, err := hintFile.ReadLogRecord(offset)
		if err != nil {
			if err == io.EOF {
				break
			}
			return err
		}
		logRecordPos := data.DecodeLogRecordPos(logRecord.Value)
		db.index.Put(logRecord.Key, logRecordPos)
		offset += size
	}
	return nil
}
