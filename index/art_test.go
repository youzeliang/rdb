package index

import (
	"github.com/stretchr/testify/assert"
	"github.com/youzeliang/rdb/data"
	"testing"
)

func TestAdaptiveRadixTree_Put(t *testing.T) {
	art := NewART()
	art.Put([]byte("a"), &data.Position{Fid: 11, Offset: 123})
	art.Put([]byte("a"), &data.Position{Fid: 11, Offset: 123})
	art.Put([]byte("a"), &data.Position{Fid: 11, Offset: 123})

	art.Put(nil, nil)
	t.Log(art.Size())
}

func TestAdaptiveRadixTree_Get(t *testing.T) {
	art := NewART()
	art.Put([]byte("caas"), &data.Position{Fid: 11, Offset: 123})
	art.Put([]byte("eeda"), &data.Position{Fid: 11, Offset: 123})
	art.Put([]byte("bbue"), &data.Position{Fid: 11, Offset: 123})

	val := art.Get([]byte("caas"))
	t.Log(val)
}

func TestAdaptiveRadixTree_Delete(t *testing.T) {
	art := NewART()

	res1, ok1 := art.Delete([]byte("not exist"))
	assert.Nil(t, res1)
	assert.False(t, ok1)

	art.Put([]byte("key-1"), &data.Position{Fid: 1, Offset: 12})
	res2, ok2 := art.Delete([]byte("key-1"))
	assert.True(t, ok2)
	assert.Equal(t, uint32(1), res2.Fid)
	assert.Equal(t, int64(12), res2.Offset)

	pos := art.Get([]byte("key-1"))
	assert.Nil(t, pos)
}

func TestAdaptiveRadixTree_Iterator(t *testing.T) {
	art := NewART()
	art.Put([]byte("annde"), &data.Position{Fid: 11, Offset: 123})
	art.Put([]byte("cnedc"), &data.Position{Fid: 11, Offset: 123})
	art.Put([]byte("aeeue"), &data.Position{Fid: 11, Offset: 123})
	art.Put([]byte("esnue"), &data.Position{Fid: 11, Offset: 123})
	art.Put([]byte("bnede"), &data.Position{Fid: 11, Offset: 123})

	art.Iterator(true)
}
