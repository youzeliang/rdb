package index

import (
	"github.com/google/btree"
	"rdb/data"
)

type Item struct {
	key []byte
	pos *data.LogRecordPos
}

func (i Item) Less(than btree.Item) bool {
	//TODO implement me
	panic("implement me")
}
