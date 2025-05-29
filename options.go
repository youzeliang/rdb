package rdb

type Options struct {

	// 数据库数据目录
	DirPath string

	// 数据文件大小
	DataFileSize int64

	// 每次写数据是否持久化

	SyncWrite bool

	// 索引类型
	IndexType IndexerType

	// 启动时是否使用 MMap 加载数据
	MMapAtStartup bool

	// 累计写到多少字节后进行持久化
	BytesPerSync uint

	//	数据文件合并的阈值
	FileMergeRatio float32
}

type IndexerType = int8

const (
	// BTree 索引
	BTree IndexerType = iota + 1

	// ART Adpative Radix Tree 自适应基数树索引
	ART
)
