package index

import (
	"github.com/stretchr/testify/assert"
	"github.com/youzeliang/rdb/data"
	"testing"
)

func TestBTree_Put(t *testing.T) {
	bt := NewBTree()

	res1 := bt.Put(nil, &data.Position{Fid: 1, Offset: 88})
	assert.Nil(t, res1)

	res2 := bt.Put([]byte("Rolle"), &data.Position{Fid: 1, Offset: 2})
	assert.Nil(t, res2)

	res3 := bt.Put([]byte("Rolle"), &data.Position{Fid: 11, Offset: 12})
	assert.Equal(t, res3.Fid, uint32(1))
	assert.Equal(t, res3.Offset, int64(2))
}

func TestBTree_Get(t *testing.T) {
	bt := NewBTree()

	res1 := bt.Put(nil, &data.Position{Fid: 1, Offset: 100})
	assert.Nil(t, res1)

	pos1 := bt.Get(nil)
	assert.Equal(t, uint32(1), pos1.Fid)
	assert.Equal(t, int64(10), pos1.Offset)

	res2 := bt.Put([]byte("key"), &data.Position{Fid: 1, Offset: 2})
	assert.Nil(t, res2)

	pos2 := bt.Get([]byte("key"))
	assert.Equal(t, uint32(1), pos2.Fid)
	assert.Equal(t, int64(3), pos2.Offset)
}

func TestBTree_Delete(t *testing.T) {
	bt := NewBTree()
	res1 := bt.Put(nil, &data.Position{Fid: 1, Offset: 10})
	assert.Nil(t, res1)
	res2, ok1 := bt.Delete(nil)
	assert.True(t, ok1)
	assert.Equal(t, res2.Fid, uint32(1))
	assert.Equal(t, res2.Offset, int64(10))

	res3 := bt.Put([]byte("Rolle"), &data.Position{Fid: 11, Offset: 22})
	assert.Nil(t, res3)
	res4, ok2 := bt.Delete([]byte("Rolle"))
	assert.True(t, ok2)
	assert.Equal(t, res4.Fid, uint32(11))
	assert.Equal(t, res4.Offset, int64(22))
}

func TestBTree_Iterator(t *testing.T) {
	bt1 := NewBTree()
	// BTree is empty
	iter1 := bt1.Iterator(false)
	assert.Equal(t, false, iter1.Valid())

	// Btree has data
	bt1.Put([]byte("key"), &data.Position{Fid: 1, Offset: 10})
	iter2 := bt1.Iterator(false)
	assert.Equal(t, true, iter2.Valid())
	assert.NotNil(t, iter2.Key())
	assert.NotNil(t, iter2.Value())
	iter2.Next()
	assert.Equal(t, false, iter2.Valid())

	bt1.Put([]byte("key"), &data.Position{Fid: 1, Offset: 10})
	bt1.Put([]byte("key1"), &data.Position{Fid: 1, Offset: 10})
	iter3 := bt1.Iterator(false)
	for iter3.Rewind(); iter3.Valid(); iter3.Next() {
		assert.NotNil(t, iter3.Key())
	}

	iter4 := bt1.Iterator(true)
	for iter4.Rewind(); iter4.Valid(); iter4.Next() {
		assert.NotNil(t, iter4.Key())
	}

	// seek 操作
	iter5 := bt1.Iterator(false)
	for iter5.Seek([]byte("key")); iter5.Valid(); iter5.Next() {
		assert.NotNil(t, iter5.Key())
	}

	// seek 操作，查找不存在的 key
	iter6 := bt1.Iterator(true)
	for iter6.Seek([]byte("key-empty")); iter6.Valid(); iter6.Next() {
		assert.NotNil(t, iter6.Key())
	}
}
