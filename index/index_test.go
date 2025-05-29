package index

import (
	"github.com/google/btree"
	"github.com/youzeliang/rdb/data"
	"reflect"
	"testing"
)

func TestItem_Less(t *testing.T) {
	type fields struct {
		key []byte
		pos *data.Position
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

func TestItem_Less1(t *testing.T) {
	type fields struct {
		key []byte
		pos *data.Position
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
