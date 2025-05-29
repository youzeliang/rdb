package index

import (
	"github.com/youzeliang/rdb/data"
	"go.etcd.io/bbolt"
	"path/filepath"
)

const indexFileName = "bptree-index"

var indexBucketName = []byte("bitcask-index")

// BPlusTree B+树索引，将索引存储到磁盘上
type BPlusTree struct {
	// 这里不需要加锁了，因为内部有对并发的处理
	tree *bbolt.DB
}

func NewBPlusTree(dirPath string, sync bool) *BPlusTree {
	// 打开 bbolt 实例
	opts := bbolt.DefaultOptions
	opts.NoSync = !sync
	bptree, err := bbolt.Open(filepath.Join(dirPath, indexFileName), 0644, opts)
	if err != nil {
		panic("failed to open bptree at startup")
	}

	// 创建一个对应的 bucket
	if err := bptree.Update(func(tx *bbolt.Tx) error {
		// 这里是初始化阶段，其实是不需要bucket的返回的
		_, err := tx.CreateBucketIfNotExists(indexBucketName)
		return err
	}); err != nil {
		panic("failed to create bptree bucket at startup")
	}
	return &BPlusTree{tree: bptree}
}

func (bpt *BPlusTree) Put(key []byte, pos *data.Position) *data.Position {
	// 这里的update方法是一个事务，所有的操作都是原子性的
	var oldVal []byte
	if err := bpt.tree.Update(func(tx *bbolt.Tx) error {
		bucket := tx.Bucket(indexBucketName)
		oldVal = bucket.Get(key)
		return bucket.Put(key, data.EncodeLogRecordPos(pos))
	}); err != nil {
		panic("failed to put value in bptree")
	}
	if len(oldVal) == 0 {
		return nil
	}
	return data.DecodeLogRecordPos(oldVal)
}

func (bpt *BPlusTree) Get(key []byte) *data.Position {
	var pos *data.Position
	// view是一个只读的方法
	if err := bpt.tree.View(func(tx *bbolt.Tx) error {
		bucket := tx.Bucket(indexBucketName)
		value := bucket.Get(key)
		if len(value) != 0 {
			pos = data.DecodeLogRecordPos(value)
		}
		return nil
	}); err != nil {
		panic("failed to get key-value pair from bptree")
	}
	return pos
}

func (bpt *BPlusTree) Delete(key []byte) (*data.Position, bool) {
	var oldVal []byte
	if err := bpt.tree.Update(func(tx *bbolt.Tx) error {
		bucket := tx.Bucket(indexBucketName)
		if oldVal = bucket.Get(key); len(oldVal) != 0 {
			return bucket.Delete(key)
		}
		return nil
	}); err != nil {
		panic("failed to delete value in bptree")
	}
	if len(oldVal) == 0 {
		return nil, false
	}
	return data.DecodeLogRecordPos(oldVal), true
}

func (bpt *BPlusTree) Iterator(reverse bool) Iterator {
	return newBptreeIterator(bpt.tree, reverse)
}

func (bpt *BPlusTree) Close() error {
	return bpt.tree.Close()
}

func (bpt *BPlusTree) Size() int {
	var size int
	if err := bpt.tree.View(func(tx *bbolt.Tx) error {
		bucket := tx.Bucket(indexBucketName)
		size = bucket.Stats().KeyN
		return nil
	}); err != nil {
		panic("failed to get size from bptree")
	}

	return size
}

// B+树迭代器
type bptreeIterator struct {
	tx        *bbolt.Tx
	cursor    *bbolt.Cursor
	reverse   bool
	currKey   []byte
	currValue []byte
}

func (bi *bptreeIterator) Seek(key []byte) {
	bi.currKey, bi.currValue = bi.cursor.Seek(key)
}

func (bi *bptreeIterator) Next() {
	if bi.reverse {
		bi.currKey, bi.currValue = bi.cursor.Prev()
	} else {
		bi.currKey, bi.currValue = bi.cursor.Next()
	}
}

func (bi *bptreeIterator) Valid() bool {
	return len(bi.currKey) != 0
}

func (bi *bptreeIterator) Key() []byte {
	return bi.currValue
}

func (bi *bptreeIterator) Value() *data.Position {
	return data.DecodeLogRecordPos(bi.currValue)
}

func (bi *bptreeIterator) Close() {
	_ = bi.tx.Rollback()
}

func newBptreeIterator(db *bbolt.DB, reverse bool) *bptreeIterator {

	// 相当于手动开启
	tx, err := db.Begin(false)
	if err != nil {
		panic("failed to begin transaction for bptree iterator")
	}

	bi := &bptreeIterator{
		tx:      tx,
		cursor:  tx.Bucket(indexBucketName).Cursor(),
		reverse: reverse,
	}

	bi.Rewind()
	return bi
}

func (bi *bptreeIterator) Rewind() {
	if bi.reverse {
		bi.currKey, bi.currValue = bi.cursor.Last()
	} else {
		bi.currKey, bi.currValue = bi.cursor.First()
	}
}
