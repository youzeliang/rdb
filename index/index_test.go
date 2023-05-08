package index

import (
	"github.com/google/btree"
	"rdb/data"
	"reflect"
	"sync"
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
	type fields struct {
		tree *btree.BTree
		lock *sync.RWMutex
	}
	type args struct {
		key []byte
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
			bt := &BTree{
				tree: tt.fields.tree,
				lock: tt.fields.lock,
			}
			if got := bt.Delete(tt.args.key); got != tt.want {
				t.Errorf("Delete() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestBTree_Get(t *testing.T) {
	type fields struct {
		tree *btree.BTree
		lock *sync.RWMutex
	}
	type args struct {
		key []byte
	}
	tests := []struct {
		name   string
		fields fields
		args   args
		want   *data.LogRecordPos
	}{
		{
			name: "Test Get with non-existing key",
			fields: fields{
				tree: btree.New(2),
				lock: &sync.RWMutex{},
			},
			args: args{
				key: []byte("test-key"),
			},
			want: nil,
		},
		{
			name: "Test Get with existing key",
			fields: fields{
				tree: btree.New(2),
				lock: &sync.RWMutex{},
			},
			args: args{
				key: []byte("test-key1"),
			},
			want: &data.LogRecordPos{Fid: 1, Offset: 0},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			bt := &BTree{
				tree: tt.fields.tree,
				lock: tt.fields.lock,
			}
			if got := bt.Get(tt.args.key); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("Get() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestBTree_Put(t *testing.T) {
	type fields struct {
		tree *btree.BTree
		lock *sync.RWMutex
	}
	type args struct {
		key []byte
		pos *data.LogRecordPos
	}
	tests := []struct {
		name   string
		fields fields
		args   args
		want   bool
	}{
		{
			name: "Test Put with non-existing key",
			fields: fields{
				tree: btree.New(2),
				lock: &sync.RWMutex{},
			},
			args: args{
				key: []byte("test-key"),
				pos: &data.LogRecordPos{Fid: 1, Offset: 0},
			},
			want: true,
		},
		{
			name: "Test Put with existing key",
			fields: fields{
				tree: btree.New(2),
				lock: &sync.RWMutex{},
			},
			args: args{
				key: []byte("test-key"),
				pos: &data.LogRecordPos{Fid: 1, Offset: 0},
			},
			want: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			bt := &BTree{
				tree: tt.fields.tree,
				lock: tt.fields.lock,
			}
			if got := bt.Put(tt.args.key, tt.args.pos); got != tt.want {
				t.Errorf("Put() = %v, want %v", got, tt.want)
			}
		})
	}
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
