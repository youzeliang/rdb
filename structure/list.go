package redis

import (
	"github.com/youzeliang/rdb"
)

// LPush 将元素插入到列表头部
func (rds *RedisDataStructure) LPush(key, element []byte) (uint32, error) {
	return rds.pushInner(key, element, true)
}

// RPush 将元素插入到列表尾部
func (rds *RedisDataStructure) RPush(key, element []byte) (uint32, error) {
	return rds.pushInner(key, element, false)
}

// LPop 从列表头部弹出元素
func (rds *RedisDataStructure) LPop(key []byte) ([]byte, error) {
	return rds.popInner(key, true)
}

// RPop 从列表尾部弹出元素
func (rds *RedisDataStructure) RPop(key []byte) ([]byte, error) {
	return rds.popInner(key, false)
}

// pushInner 列表内部插入实现
func (rds *RedisDataStructure) pushInner(key, element []byte, isLeft bool) (uint32, error) {
	// 查找元数据
	meta, err := rds.findMetadata(key, List)
	if err != nil {
		return 0, err
	}

	// 构造 listInternalKey
	lk := &listInternalKey{
		key:     key,
		version: meta.version,
	}
	if isLeft {
		lk.index = meta.head - 1
	} else {
		lk.index = meta.tail
	}

	// 更新数据和元数据
	wb := rds.db.NewWriteBatch(rdb.DefaultWriteBatchConfigs)
	meta.size++
	if isLeft {
		meta.head--
	} else {
		meta.tail++
	}
	_ = wb.Put(key, meta.encode())
	_ = wb.Put(lk.encode(), element)
	if err = wb.Commit(); err != nil {
		return 0, err
	}
	return meta.size, nil
}

// popInner 列表内部弹出实现
func (rds *RedisDataStructure) popInner(key []byte, isLeft bool) ([]byte, error) {
	// 查找元数据
	meta, err := rds.findMetadata(key, List)
	if err != nil {
		return nil, err
	}
	if meta.size == 0 {
		return nil, nil
	}

	// 构造 listInternalKey
	lk := &listInternalKey{
		key:     key,
		version: meta.version,
	}
	if isLeft {
		lk.index = meta.head
	} else {
		lk.index = meta.tail - 1
	}

	element, err := rds.db.Get(lk.encode())
	if err != nil {
		return nil, err
	}

	// 更新数据和元数据
	wb := rds.db.NewWriteBatch(rdb.DefaultWriteBatchConfigs)
	meta.size--
	if isLeft {
		meta.head++
	} else {
		meta.tail--
	}
	_ = wb.Put(key, meta.encode())
	_ = wb.Delete(lk.encode())
	if err = wb.Commit(); err != nil {
		return nil, err
	}
	return element, nil
}
