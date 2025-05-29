package index

import (
	"bytes"
	"github.com/google/btree"
	"github.com/youzeliang/rdb/data"
)

type Indexer interface {
	// Put 向索引中存储 key 对应的数据位置信息
	Put(key []byte, pos *data.Position) *data.Position

	// Get 根据 key 取出对应的索引位置信息
	Get(key []byte) *data.Position

	// Delete 根据 key 删除对应的索引位置信息
	Delete(key []byte) (*data.Position, bool)

	// Iterator 索引迭代器
	Iterator(reverse bool) Iterator

	// Size 索引中的数据量
	Size() int

	Close() error
}

type IndexType = int8

const (
	// Btree 索引
	Btree IndexType = iota + 1

	// ART 自适应基数树索引
	ART

	// BPTree B+树索引
	BPTree
)

func NewIndexer(typ IndexType, dirPath string, sync bool) Indexer {
	switch typ {
	case Btree:
		return NewBTree()
	case ART:
		return NewART()
	case BPTree:
		return NewBPlusTree(dirPath, sync)
	default:
		panic("unknown index type")
	}
}

type Item struct {
	key []byte
	pos *data.Position
}

// Less 因为在BTree里数据排列都是有序的，所以这里的less方法是比较key的大小的规则
func (i *Item) Less(bi btree.Item) bool {
	return bytes.Compare(i.key, bi.(*Item).key) == -1
}

// Iterator 通用索引迭代器
type Iterator interface {
	// Rewind 重新回到迭代器的起点，即第一个数据
	Rewind()

	// Seek 根据传入的 key 查找到第一个大于（或小于）等于的目标 key，根据从这个 key 开始遍历
	Seek(key []byte)

	// Next 跳转到下一个 key
	Next()

	// Valid 是否有效，即是否已经遍历完了所有的 key，用于退出遍历
	Valid() bool

	// Key 当前遍历位置的 Key 数据
	Key() []byte

	// Value 当前遍历位置的 Value 数据
	Value() *data.Position

	// Close 关闭迭代器，释放相应资源
	Close()
}
