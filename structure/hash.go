package redis

import (
	"encoding/binary"
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
	wb := rds.db.NewWriteBatch(rdb.DefaultWriteBatchConfigs)
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
		wb := rds.db.NewWriteBatch(rdb.DefaultWriteBatchConfigs)
		meta.size--
		_ = wb.Put(key, meta.encode())
		_ = wb.Delete(encKey)
		if err = wb.Commit(); err != nil {
			return false, err
		}
	}
	return exist, nil
}

// HGetAll 获取哈希表所有字段和值
func (rds *RedisDataStructure) HGetAll(key []byte) ([][]byte, error) {
	meta, err := rds.findMetadata(key, Hash)
	if err != nil {
		return nil, err
	}
	if meta.size == 0 {
		return [][]byte{}, nil
	}

	// Use iterator to find all fields for this key
	iterator := rds.db.NewIterator(rdb.DefaultIteratorConfigs)
	prefix := rds.encodeHashKeyPrefix(key, meta.version)

	result := make([][]byte, 0, meta.size*2)
	for iterator.Rewind(); iterator.Valid(); iterator.Next() {
		dbKey := iterator.Key()
		if !rds.hasKeyPrefix(dbKey, prefix) {
			break
		}
		field := rds.extractHashField(dbKey, prefix)
		value, err := iterator.Value()
		if err != nil {
			break
		}
		result = append(result, field, value)
	}
	return result, nil
}

// HLen 获取哈希表字段数量
func (rds *RedisDataStructure) HLen(key []byte) (uint32, error) {
	meta, err := rds.findMetadata(key, Hash)
	if err != nil {
		return 0, err
	}
	return meta.size, nil
}

// HExists 判断字段是否存在
func (rds *RedisDataStructure) HExists(key, field []byte) (int, error) {
	meta, err := rds.findMetadata(key, Hash)
	if err != nil {
		return 0, err
	}
	if meta.size == 0 {
		return 0, nil
	}

	hk := &hashInternalKey{
		key:     key,
		version: meta.version,
		field:   field,
	}

	_, err = rds.db.Get(hk.encode())
	if err != nil {
		if errors.Is(err, rdb.ErrKeyNotFound) {
			return 0, nil
		}
		return 0, err
	}
	return 1, nil
}

// HKeys 获取所有字段名
func (rds *RedisDataStructure) HKeys(key []byte) ([][]byte, error) {
	meta, err := rds.findMetadata(key, Hash)
	if err != nil {
		return nil, err
	}
	if meta.size == 0 {
		return [][]byte{}, nil
	}

	// Use iterator to find all fields for this key
	iterator := rds.db.NewIterator(rdb.DefaultIteratorConfigs)
	prefix := rds.encodeHashKeyPrefix(key, meta.version)

	result := make([][]byte, 0, meta.size)
	for iterator.Rewind(); iterator.Valid(); iterator.Next() {
		dbKey := iterator.Key()
		if !rds.hasKeyPrefix(dbKey, prefix) {
			break
		}
		field := rds.extractHashField(dbKey, prefix)
		result = append(result, field)
	}
	return result, nil
}

// HVals 获取所有值
func (rds *RedisDataStructure) HVals(key []byte) ([][]byte, error) {
	meta, err := rds.findMetadata(key, Hash)
	if err != nil {
		return nil, err
	}
	if meta.size == 0 {
		return [][]byte{}, nil
	}

	// Use iterator to find all fields for this key
	iterator := rds.db.NewIterator(rdb.DefaultIteratorConfigs)
	prefix := rds.encodeHashKeyPrefix(key, meta.version)

	result := make([][]byte, 0, meta.size)
	for iterator.Rewind(); iterator.Valid(); iterator.Next() {
		dbKey := iterator.Key()
		if !rds.hasKeyPrefix(dbKey, prefix) {
			break
		}
		value, err := iterator.Value()
		if err != nil {
			break
		}
		result = append(result, value)
	}
	return result, nil
}

// HIncrBy 字段值增加指定数值
func (rds *RedisDataStructure) HIncrBy(key, field []byte, num int64) (int64, error) {
	meta, err := rds.findMetadata(key, Hash)
	if err != nil {
		return 0, err
	}

	// Get current value
	hk := &hashInternalKey{
		key:     key,
		version: meta.version,
		field:   field,
	}

	var oldVal int64 = 0
	var exist = true
	encKey := hk.encode()
	existingValue, err := rds.db.Get(encKey)
	if err != nil {
		if errors.Is(err, rdb.ErrKeyNotFound) {
			exist = false
		} else {
			return 0, err
		}
	} else {
		oldVal, err = parseInt64(existingValue)
		if err != nil {
			return 0, errors.New("ERR value is not an integer")
		}
	}

	newVal := oldVal + num

	wb := rds.db.NewWriteBatch(rdb.DefaultWriteBatchConfigs)
	if !exist {
		meta.size++
		_ = wb.Put(key, meta.encode())
	}
	_ = wb.Put(encKey, formatInt64(newVal))
	if err = wb.Commit(); err != nil {
		return 0, err
	}
	return newVal, nil
}

// HMGet 批量获取字段值
func (rds *RedisDataStructure) HMGet(key []byte, fields ...[]byte) ([][]byte, error) {
	meta, err := rds.findMetadata(key, Hash)
	if err != nil {
		return nil, err
	}

	result := make([][]byte, len(fields))
	for i, field := range fields {
		hk := &hashInternalKey{
			key:     key,
			version: meta.version,
			field:   field,
		}
		value, err := rds.db.Get(hk.encode())
		if err != nil {
			if errors.Is(err, rdb.ErrKeyNotFound) {
				result[i] = nil
				continue
			}
			return nil, err
		}
		result[i] = value
	}
	return result, nil
}

// HMSET 批量设置字段值
func (rds *RedisDataStructure) HMSet(key []byte, fieldValues ...[]byte) error {
	if len(fieldValues)%2 != 0 {
		return errors.New("HMSET requires an even number of arguments")
	}

	meta, err := rds.findMetadata(key, Hash)
	if err != nil {
		return err
	}

	wb := rds.db.NewWriteBatch(rdb.DefaultWriteBatchConfigs)
	for i := 0; i < len(fieldValues); i += 2 {
		field := fieldValues[i]
		value := fieldValues[i+1]

		hk := &hashInternalKey{
			key:     key,
			version: meta.version,
			field:   field,
		}

		// Check if field exists
		var exist = true
		if _, err = rds.db.Get(hk.encode()); errors.Is(err, rdb.ErrKeyNotFound) {
			exist = false
		}

		if !exist {
			meta.size++
		}
		_ = wb.Put(hk.encode(), value)
	}
	_ = wb.Put(key, meta.encode())
	return wb.Commit()
}

// encodeHashKeyPrefix encodes the key and version prefix for hash iteration
func (rds *RedisDataStructure) encodeHashKeyPrefix(key []byte, version int64) []byte {
	buf := make([]byte, len(key)+8)
	copy(buf, key)
	binary.LittleEndian.PutUint64(buf[len(key):], uint64(version))
	return buf
}

// hasKeyPrefix checks if the key has the given prefix
func (rds *RedisDataStructure) hasKeyPrefix(key, prefix []byte) bool {
	if len(key) < len(prefix) {
		return false
	}
	for i := 0; i < len(prefix); i++ {
		if key[i] != prefix[i] {
			return false
		}
	}
	return true
}

// extractHashField extracts the field from a hash internal key
func (rds *RedisDataStructure) extractHashField(dbKey, prefix []byte) []byte {
	// The internal key format is: key + version(8) + field
	// The prefix is: key + version(8)
	// So the field starts after the prefix
	fieldLen := len(dbKey) - len(prefix)
	field := make([]byte, fieldLen)
	copy(field, dbKey[len(prefix):])
	return field
}
