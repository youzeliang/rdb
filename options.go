package rdb

import "os"

// Configs 配置项的结构体、用户可传递过来的配置项
type Configs struct {
	// 数据库数据目录
	DirPath string

	// 数据文件的大小
	FileSize int64

	// 每次写数据是否持久化
	SyncWrites bool

	// 索引类型
	IndexType IndexerType

	// 积累多少字节写入后进行持久化
	BytesPerSync int

	// 启动时是否使用 MMap 加载数据
	MMapAtStartup bool

	// 数据文件合并的阈值
	DataFileMergeRatio float32
}

// IteratorConfigs 索引迭代器配置项
type IteratorConfigs struct {
	// 遍历前缀为指定值的 Key，默认为空
	Prefix []byte
	// 是否反向遍历，默认 false 是正向
	Reverse bool
}

// WriteBatchConfigs 批量写配置项
type WriteBatchConfigs struct {
	// 一个批次当中最大的数据量
	MaxBatchNum uint

	// 提交时是否 sync 持久化
	SyncWrites bool
}

type IndexerType = int8

const (
	// BTree 索引
	BTree IndexerType = iota + 1

	// ART Adpative Radix Tree 自适应基数树索引
	ART

	// BPlusTree B+ 树索引，将索引存储到磁盘上
	BPlusTree
)

var DefaultOptions = Configs{
	DirPath:            os.TempDir(),
	FileSize:           256 * 1024 * 1024, // 256MB
	SyncWrites:         false,
	IndexType:          BTree,
	BytesPerSync:       0,
	MMapAtStartup:      true,
	DataFileMergeRatio: 0.5,
}

var DefaultIteratorConfigs = IteratorConfigs{
	Prefix:  nil,
	Reverse: false,
}

var DefaultWriteBatchConfigs = WriteBatchConfigs{
	MaxBatchNum: 10000,
	SyncWrites:  true,
}
