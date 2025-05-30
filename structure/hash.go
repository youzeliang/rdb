package redis

import (
	"errors"
	"github.com/youzeliang/rdb"
)

// HSet 设置哈希表中字段的值
func (rds *RedisDataStructure) HSet(key, field, value []byte) (bool, error) {
	// 先查询元数据
	meta, err := rds.findMetadata(key, Hash)
	if err != nil {
		return false, err
	}

	// 构造数据部分的 key
	hk := &hashInternalKey{
		key:     key,
		version: meta.version,
		field:   field,
	}
	encKey := hk.encode()

	// field 不存在，才会返回true
	var exist = true
	// 先查找是否存在
	if _, err = rds.db.Get(encKey); errors.Is(err, rdb.ErrKeyNotFound) {
		exist = false
	}

	// 更新数据和元数据
	wb := rds.db.NewWriteBatch(rdb.DefaultWriteBatchOptions)
	if !exist {
		meta.size++
		_ = wb.Put(key, meta.encode())
	}
	// value 可能不一样，所以要更新
	_ = wb.Put(encKey, value)
	if err = wb.Commit(); err != nil {
		return false, err
	}

	return !exist, nil
}

// HGet 获取哈希表中字段的值
func (rds *RedisDataStructure) HGet(key, field []byte) ([]byte, error) {
	meta, err := rds.findMetadata(key, Hash)
	if err != nil {
		return nil, err
	}
	if meta.size == 0 {
		return nil, nil
	}

	hk := &hashInternalKey{
		key:     key,
		version: meta.version,
		field:   field,
	}

	return rds.db.Get(hk.encode())
}

// HDel 删除哈希表中的字段
func (rds *RedisDataStructure) HDel(key, field []byte) (bool, error) {
	meta, err := rds.findMetadata(key, Hash)
	if err != nil {
		return false, err
	}
	if meta.size == 0 {
		return false, nil
	}

	// 构造数据部分的 key
	hk := &hashInternalKey{
		key:     key,
		version: meta.version,
		field:   field,
	}
	encKey := hk.encode()

	// 先查看是否存在
	var exist = true
	if _, err = rds.db.Get(encKey); errors.Is(err, rdb.ErrKeyNotFound) {
		exist = false
	}

	// 更新数据和元数据
	if exist {
		wb := rds.db.NewWriteBatch(rdb.DefaultWriteBatchOptions)
		meta.size--
		_ = wb.Put(key, meta.encode())
		_ = wb.Delete(encKey)
		if err = wb.Commit(); err != nil {
			return false, err
		}
	}
	return exist, nil
}
