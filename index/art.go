package index

// 自适应基数索引
// 主要封装了 https://github.com/plar/go-adaptive-radix-tree 库

import (
	"bytes"
	goart "github.com/plar/go-adaptive-radix-tree"
	"github.com/youzeliang/rdb/data"
	"sort"
	"sync"
)

type AdaptiveRadixTree struct {
	tree goart.Tree
	lock *sync.RWMutex
}

// NewART 新建 ART 索引
func NewART() *AdaptiveRadixTree {
	return &AdaptiveRadixTree{
		tree: goart.New(),
		lock: new(sync.RWMutex),
	}
}

func (art *AdaptiveRadixTree) Put(key []byte, pos *data.Position) *data.Position {
	art.lock.Lock()
	oldValue, _ := art.tree.Insert(key, pos)
	art.lock.Unlock()
	if oldValue == nil {
		return nil
	}
	return oldValue.(*data.Position)
}

func (art *AdaptiveRadixTree) Get(key []byte) *data.Position {
	art.lock.RLock()
	defer art.lock.RUnlock()
	value, found := art.tree.Search(key)
	if !found {
		return nil
	}
	return value.(*data.Position)
}

func (art *AdaptiveRadixTree) Delete(key []byte) (*data.Position, bool) {
	art.lock.Lock()
	oldValue, deleted := art.tree.Delete(key)
	art.lock.Unlock()
	if oldValue == nil {
		return nil, false
	}
	return oldValue.(*data.Position), deleted
}
func (art *AdaptiveRadixTree) Size() int {
	art.lock.RLock()
	defer art.lock.RUnlock()
	size := art.tree.Size()
	return size
}

// Iterator 索引迭代器
func (art *AdaptiveRadixTree) Iterator(reverse bool) Iterator {
	art.lock.RLock()
	iterator := newArtIterator(art.tree, reverse)
	art.lock.RUnlock()
	return iterator
}

func (art *AdaptiveRadixTree) Close() error {
	return nil
}

// ART 索引迭代器
type artIterator struct {
	currIndex int     // 当前遍历的下标位置
	reverse   bool    // 是否是反向遍历
	values    []*Item // key+位置索引信息
}

func (a artIterator) Rewind() {
	a.currIndex = 0
}

func (a artIterator) Seek(key []byte) {
	if a.reverse {
		a.currIndex = sort.Search(len(a.values), func(i int) bool {
			return bytes.Compare(a.values[i].key, key) <= 0
		})
	} else {
		a.currIndex = sort.Search(len(a.values), func(i int) bool {
			return bytes.Compare(a.values[i].key, key) >= 0
		})
	}
}

func (a artIterator) Next() {
	a.currIndex += 1
}

func (a artIterator) Valid() bool {
	return a.currIndex < len(a.values)
}

func (a artIterator) Key() []byte {
	return a.values[a.currIndex].key
}

func (a artIterator) Value() *data.Position {
	return a.values[a.currIndex].pos
}

func (a artIterator) Close() {
	a.values = nil
}

func newArtIterator(tree goart.Tree, reverse bool) *artIterator {
	var idx int
	if reverse {
		idx = tree.Size() - 1
	}
	values := make([]*Item, tree.Size())
	// node 就是索引树中存在的节点，包含key value 数据
	saveValues := func(node goart.Node) bool {
		item := &Item{
			key: node.Key(),
			pos: node.Value().(*data.Position),
		}
		values[idx] = item
		if reverse {
			idx--
		} else {
			idx++
		}
		return true
	}

	tree.ForEach(saveValues)
	return &artIterator{
		currIndex: 0,
		reverse:   reverse,
		values:    values,
	}
}
