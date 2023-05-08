package index

import (
	"bytes"
	"github.com/google/btree"
	"rdb/data"
)

type Item struct {
	key []byte
	pos *data.LogRecordPos
}

func (i *Item) Less(than btree.Item) bool {
	return bytes.Compare(i.key, than.(*Item).key) == -1
}

type Indexer interface {
	// Put store the position information of data corresponding to the key in the index.
	Put(key []byte, pos *data.LogRecordPos) bool

	// Get the position information of data corresponding to the key in the index.
	Get(key []byte) *data.LogRecordPos

	// Delete the position information of data corresponding to the key in the index.
	Delete(key []byte) bool
}
