package index

import (
	"bytes"
	"github.com/google/btree"
	"github.com/youzeliang/rdb/data"
	"sort"
	"sync"
)

type BTree struct {
	btree *btree.BTree
	lock  *sync.RWMutex
}

func (bt *BTree) Iterator(reverse bool) Iterator {
	if bt.btree == nil {
		return nil
	}
	bt.lock.RLock()
	defer bt.lock.RUnlock()
	return newBTreeIterator(bt.btree, reverse)
}

// NewBTree 新建 BTree 索引结构

func NewBTree() *BTree {
	return &BTree{
		// 控制叶子节点的数量
		btree: btree.New(32),
		// 因为并发不安全, 读操作是并发安全的
		lock: new(sync.RWMutex),
	}
}

// BTree 索引迭代器
type btreeIterator struct {
	index   int     // 当前遍历的下标位置
	reverse bool    // 是否是反向遍历
	values  []*Item // key+位置索引信息
}

func newBTreeIterator(tree *btree.BTree, reverse bool) *btreeIterator {
	var idx int
	values := make([]*Item, tree.Len())

	saveValues := func(it btree.Item) bool {
		values[idx] = it.(*Item)
		idx++
		// 这里返回 true，表示继续遍历
		return true
	}
	if reverse {
		tree.Descend(saveValues)
	} else {
		tree.Ascend(saveValues)
	}

	return &btreeIterator{
		index:   0,
		reverse: reverse,
		values:  values,
	}
}

// Rewind 重新回到迭代器的起点，即第一个数据
func (b *btreeIterator) Rewind() {
	b.index = 0
}

func (bt *BTree) Close() error {
	return nil
}

// Seek 根据传入的 key 查找到第一个大于（或小于）等于的目标 key，根据从这个 key 开始遍历
func (b *btreeIterator) Seek(key []byte) {
	if b.reverse {
		b.index = sort.Search(len(b.values), func(i int) bool {
			return bytes.Compare(b.values[i].key, key) <= 0
		})
	} else {
		b.index = sort.Search(len(b.values), func(i int) bool {
			return bytes.Compare(b.values[i].key, key) >= 0
		})
	}
}

func (b *btreeIterator) Next() {
	b.index += 1
}

func (b *btreeIterator) Valid() bool {
	return b.index < len(b.values)
}

func (b *btreeIterator) Key() []byte {
	return b.values[b.index].key
}

func (b *btreeIterator) Value() *data.Position {
	return b.values[b.index].pos
}

func (b *btreeIterator) Close() {
	b.values = nil
}

func (bt *BTree) Size() int {
	return bt.btree.Len()
}

func (bt *BTree) Put(key []byte, pos *data.Position) *data.Position {
	it := &Item{key: key, pos: pos}
	bt.lock.Lock()
	oldItem := bt.btree.ReplaceOrInsert(it)
	bt.lock.Unlock()
	if oldItem == nil {
		return nil
	}
	return oldItem.(*Item).pos
}

func (bt *BTree) Get(key []byte) *data.Position {
	it := &Item{
		key: key,
	}
	// item 里的less方法是比较key的大小的规则
	btreeItem := bt.btree.Get(it)
	if btreeItem == nil {
		return nil
	}

	return btreeItem.(*Item).pos
}

func (bt *BTree) Delete(key []byte) (*data.Position, bool) {
	it := &Item{key: key}
	bt.lock.Lock()
	oldItem := bt.btree.Delete(it)
	bt.lock.Unlock()
	if oldItem == nil {
		return nil, false
	}
	return oldItem.(*Item).pos, true
}
