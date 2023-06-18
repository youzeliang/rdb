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
}

type IndexerType = int8

const (
	// BTree 索引
	BTree IndexerType = iota + 1

	// ART Adpative Radix Tree 自适应基数树索引
	ART
)
