package redis

import (
	"errors"
	"time"

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

// LLen 获取列表长度
func (rds *RedisDataStructure) LLen(key []byte) (uint32, error) {
	meta, err := rds.findMetadata(key, List)
	if err != nil {
		return 0, err
	}
	return meta.size, nil
}

// LRange 获取列表指定范围内的元素
func (rds *RedisDataStructure) LRange(key []byte, start, stop int64) ([][]byte, error) {
	meta, err := rds.findMetadata(key, List)
	if err != nil {
		return nil, err
	}
	if meta.size == 0 {
		return [][]byte{}, nil
	}

	// Convert negative indices
	size := int64(meta.size)
	if start < 0 {
		start = size + start
	}
	if stop < 0 {
		stop = size + stop
	}

	// Handle out of range
	if start >= size || stop < 0 || start > stop {
		return [][]byte{}, nil
	}
	if start < 0 {
		start = 0
	}
	if stop >= size {
		stop = size - 1
	}

	// Collect elements in range
	result := make([][]byte, 0, stop-start+1)
	for i := start; i <= stop; i++ {
		lk := &listInternalKey{
			key:     key,
			version: meta.version,
			index:   meta.head + uint64(i),
		}
		element, err := rds.db.Get(lk.encode())
		if err != nil {
			if err == rdb.ErrKeyNotFound {
				continue
			}
			return nil, err
		}
		result = append(result, element)
	}
	return result, nil
}

// LIndex 获取列表指定索引的元素
func (rds *RedisDataStructure) LIndex(key []byte, index int64) ([]byte, error) {
	meta, err := rds.findMetadata(key, List)
	if err != nil {
		return nil, err
	}
	if meta.size == 0 {
		return nil, nil
	}

	size := int64(meta.size)
	if index < 0 {
		index = size + index
	}

	if index < 0 || index >= size {
		return nil, nil
	}

	lk := &listInternalKey{
		key:     key,
		version: meta.version,
		index:   meta.head + uint64(index),
	}
	return rds.db.Get(lk.encode())
}

// LInsert 在列表中插入元素
func (rds *RedisDataStructure) LInsert(key []byte, isBefore bool, pivot, element []byte) (int64, error) {
	meta, err := rds.findMetadata(key, List)
	if err != nil {
		return 0, err
	}
	if meta.size == 0 {
		return -1, nil
	}

	// Find the pivot index
	var pivotIdx int64 = -1
	for i := int64(0); i < int64(meta.size); i++ {
		lk := &listInternalKey{
			key:     key,
			version: meta.version,
			index:   meta.head + uint64(i),
		}
		val, err := rds.db.Get(lk.encode())
		if err != nil {
			continue
		}
		if string(val) == string(pivot) {
			pivotIdx = i
			break
		}
	}

	if pivotIdx == -1 {
		return -1, nil
	}

	// Insert at the position
	insertIdx := meta.head + uint64(pivotIdx)
	if !isBefore {
		insertIdx++
	}

	// We need to shift elements and insert
	// For simplicity, we'll read all elements, rebuild the list
	// This is a simplified approach - in production, a more efficient method would be used
	wb := rds.db.NewWriteBatch(rdb.DefaultWriteBatchConfigs)

	// Read all elements
	elements := make([][]byte, meta.size)
	for i := int64(0); i < int64(meta.size); i++ {
		lk := &listInternalKey{
			key:     key,
			version: meta.version,
			index:   meta.head + uint64(i),
		}
		val, _ := rds.db.Get(lk.encode())
		elements[i] = val
		_ = wb.Delete(lk.encode())
	}

	// Increment version for new write
	newVersion := time.Now().UnixNano()

	// Rebuild list with new element
	newSize := uint32(0)
	var newHead uint64 = initialListMark
	var newTail uint64 = initialListMark

	for i, el := range elements {
		insertPos := int64(i)
		if !isBefore {
			insertPos = int64(i) + 1
		}

		if insertPos == pivotIdx {
			// Insert the new element
			newIdx := newHead + uint64(newSize)
			if newSize == 0 {
				newHead = newIdx
			}
			lk := &listInternalKey{
				key:     key,
				version: newVersion,
				index:   newIdx,
			}
			_ = wb.Put(lk.encode(), element)
			newSize++
			newTail = newIdx + 1
		}

		// Insert existing element
		newIdx := newHead + uint64(newSize)
		if newSize == 0 {
			newHead = newIdx
		}
		lk := &listInternalKey{
			key:     key,
			version: newVersion,
			index:   newIdx,
		}
		_ = wb.Put(lk.encode(), el)
		newSize++
		newTail = newIdx + 1
	}

	// Handle case where we insert after the last element (at the end)
	if pivotIdx == int64(meta.size)-1 && !isBefore {
		newIdx := newHead + uint64(newSize)
		lk := &listInternalKey{
			key:     key,
			version: newVersion,
			index:   newIdx,
		}
		_ = wb.Put(lk.encode(), element)
		newSize++
		newTail = newIdx + 1
	}

	// Update metadata
	meta.version = newVersion
	meta.size = newSize
	meta.head = newHead
	meta.tail = newTail
	_ = wb.Put(key, meta.encode())

	if err = wb.Commit(); err != nil {
		return 0, err
	}
	return int64(newSize), nil
}

// LSet 设置列表指定索引的元素
func (rds *RedisDataStructure) LSet(key []byte, index int64, element []byte) error {
	meta, err := rds.findMetadata(key, List)
	if err != nil {
		return err
	}
	if meta.size == 0 {
		return errors.New("ERR no such key")
	}

	size := int64(meta.size)
	if index < 0 {
		index = size + index
	}

	if index < 0 || index >= size {
		return errors.New("ERR index out of range")
	}

	lk := &listInternalKey{
		key:     key,
		version: meta.version,
		index:   meta.head + uint64(index),
	}

	wb := rds.db.NewWriteBatch(rdb.DefaultWriteBatchConfigs)
	_ = wb.Put(lk.encode(), element)
	return wb.Commit()
}

// LTrim 修剪列表保留指定范围
func (rds *RedisDataStructure) LTrim(key []byte, start, stop int64) error {
	meta, err := rds.findMetadata(key, List)
	if err != nil {
		return err
	}
	if meta.size == 0 {
		return nil
	}

	size := int64(meta.size)
	if start < 0 {
		start = size + start
	}
	if stop < 0 {
		stop = size + stop
	}

	// Handle out of range
	if start >= size || stop < 0 || start > stop {
		// Delete the key
		wb := rds.db.NewWriteBatch(rdb.DefaultWriteBatchConfigs)
		_ = wb.Delete(key)
		return wb.Commit()
	}

	if start < 0 {
		start = 0
	}
	if stop >= size {
		stop = size - 1
	}

	// Read elements to keep
	keepCount := stop - start + 1
	keepElements := make([][]byte, keepCount)
	for i := start; i <= stop; i++ {
		lk := &listInternalKey{
			key:     key,
			version: meta.version,
			index:   meta.head + uint64(i),
		}
		val, _ := rds.db.Get(lk.encode())
		keepElements[i-start] = val
	}

	// Delete all old elements and write new ones
	wb := rds.db.NewWriteBatch(rdb.DefaultWriteBatchConfigs)

	// Delete old elements
	for i := int64(0); i < int64(meta.size); i++ {
		lk := &listInternalKey{
			key:     key,
			version: meta.version,
			index:   meta.head + uint64(i),
		}
		_ = wb.Delete(lk.encode())
	}

	// Write new elements
	newVersion := time.Now().UnixNano()
	var newHead uint64 = initialListMark
	newSize := uint32(0)

	for i, el := range keepElements {
		newIdx := newHead + uint64(i)
		if i == 0 {
			newHead = newIdx
		}
		lk := &listInternalKey{
			key:     key,
			version: newVersion,
			index:   newIdx,
		}
		_ = wb.Put(lk.encode(), el)
		newSize++
	}

	// Update metadata
	meta.version = newVersion
	meta.size = newSize
	meta.head = newHead
	meta.tail = newHead + uint64(newSize)
	_ = wb.Put(key, meta.encode())

	return wb.Commit()
}

// RPopLPush 弹出源列表尾部并推入目标列表头部
func (rds *RedisDataStructure) RPopLPush(source, destination []byte) ([]byte, error) {
	// Pop from source
	element, err := rds.RPop(source)
	if err != nil {
		return nil, err
	}
	if element == nil {
		return nil, nil
	}

	// Push to destination
	_, err = rds.LPush(destination, element)
	if err != nil {
		// Try to rollback - push back to source
		_, _ = rds.RPush(source, element)
		return nil, err
	}

	return element, nil
}
