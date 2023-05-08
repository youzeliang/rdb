package index

import (
	"github.com/google/btree"
	"github.com/stretchr/testify/assert"
	"rdb/data"
	"reflect"
	"testing"
)

func TestItem_Less(t *testing.T) {
	type fields struct {
		key []byte
		pos *data.LogRecordPos
	}
	type args struct {
		than btree.Item
	}
	tests := []struct {
		name   string
		fields fields
		args   args
		want   bool
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			i := &Item{
				key: tt.fields.key,
				pos: tt.fields.pos,
			}
			if got := i.Less(tt.args.than); got != tt.want {
				t.Errorf("Less() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestBTree_Delete(t *testing.T) {
	bt := NewBTree()
	res1 := bt.Put(nil, &data.LogRecordPos{Fid: 1, Offset: 100})
	assert.True(t, res1)

	res2 := bt.Delete(nil)
	assert.True(t, res2)

	res3 := bt.Put([]byte("aa"), &data.LogRecordPos{Fid: 10, Offset: 20})
	assert.True(t, res3)

	res4 := bt.Delete([]byte("aa"))
	assert.True(t, res4)

}

func TestBTree_Get(t *testing.T) {
	bt := NewBTree()

	res1 := bt.Put(nil, &data.LogRecordPos{Fid: 1, Offset: 100})
	assert.True(t, res1)

	pos1 := bt.Get(nil)
	assert.Equal(t, uint32(1), pos1.Fid)
	assert.Equal(t, int64(100), pos1.Offset)

	res2 := bt.Put([]byte("a"), &data.LogRecordPos{Fid: 1, Offset: 2})
	assert.True(t, res2)
	res3 := bt.Put([]byte("a"), &data.LogRecordPos{Fid: 1, Offset: 3})
	assert.True(t, res3)

	pos2 := bt.Get([]byte("a"))
	t.Log(pos2)
}

func TestBTree_Put(t *testing.T) {
	bt := NewBTree()

	res1 := bt.Put(nil, &data.LogRecordPos{Fid: 1, Offset: 100})
	assert.True(t, res1)

	res2 := bt.Put([]byte("a"), &data.LogRecordPos{Fid: 1, Offset: 2})
	assert.True(t, res2)

}

func TestItem_Less1(t *testing.T) {
	type fields struct {
		key []byte
		pos *data.LogRecordPos
	}
	type args struct {
		than btree.Item
	}
	tests := []struct {
		name   string
		fields fields
		args   args
		want   bool
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			i := &Item{
				key: tt.fields.key,
				pos: tt.fields.pos,
			}
			if got := i.Less(tt.args.than); got != tt.want {
				t.Errorf("Less() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestNewBTree(t *testing.T) {
	tests := []struct {
		name string
		want *BTree
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := NewBTree(); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("NewBTree() = %v, want %v", got, tt.want)
			}
		})
	}
}
