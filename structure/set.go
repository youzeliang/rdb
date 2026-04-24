package redis

import (
	"encoding/binary"
	"errors"
	"github.com/youzeliang/rdb"
)

// SAdd 将元素添加到集合中
func (rds *RedisDataStructure) SAdd(key, member []byte) (bool, error) {
	meta, err := rds.findMetadata(key, Set)
	if err != nil {
		return false, err
	}

	// 构造 key
	sk := &setInternalKey{
		key:     key,
		version: meta.version,
		member:  member,
	}

	var exist = true
	// 查看是否已经存在了
	if _, err = rds.db.Get(sk.encode()); errors.Is(err, rdb.ErrKeyNotFound) {
		exist = false
	}

	if !exist {
		// 不存在的话就更新
		wb := rds.db.NewWriteBatch(rdb.DefaultWriteBatchConfigs)
		meta.size++
		_ = wb.Put(key, meta.encode())
		_ = wb.Put(sk.encode(), nil)
		if err = wb.Commit(); err != nil {
			return false, err
		}
	}
	return !exist, nil
}

// SIsMember 判断元素是否在集合中
func (rds *RedisDataStructure) SIsMember(key, member []byte) (bool, error) {
	meta, err := rds.findMetadata(key, Set)
	if err != nil {
		return false, err
	}
	if meta.size == 0 {
		return false, nil
	}

	// 构造 key
	sk := &setInternalKey{
		key:     key,
		version: meta.version,
		member:  member,
	}

	_, err = rds.db.Get(sk.encode())
	if err != nil {
		if errors.Is(err, rdb.ErrKeyNotFound) {
			return false, nil
		}
		return false, err
	}
	return true, nil
}

// SRem 从集合中删除元素
func (rds *RedisDataStructure) SRem(key, member []byte) (bool, error) {
	meta, err := rds.findMetadata(key, Set)
	if err != nil {
		return false, err
	}
	if meta.size == 0 {
		return false, nil
	}

	// 构造 key
	sk := &setInternalKey{
		key:     key,
		version: meta.version,
		member:  member,
	}

	if _, err = rds.db.Get(sk.encode()); err != nil {
		if errors.Is(err, rdb.ErrKeyNotFound) {
			return false, nil
		}
		return false, err
	}

	// 更新数据和元数据
	wb := rds.db.NewWriteBatch(rdb.DefaultWriteBatchConfigs)
	meta.size--
	_ = wb.Put(key, meta.encode())
	_ = wb.Delete(sk.encode())
	if err = wb.Commit(); err != nil {
		return false, err
	}
	return true, nil
}

// SCard 获取集合基数
func (rds *RedisDataStructure) SCard(key []byte) (uint32, error) {
	meta, err := rds.findMetadata(key, Set)
	if err != nil {
		return 0, err
	}
	return meta.size, nil
}

// SMembers 获取集合所有成员
func (rds *RedisDataStructure) SMembers(key []byte) ([][]byte, error) {
	meta, err := rds.findMetadata(key, Set)
	if err != nil {
		return nil, err
	}
	if meta.size == 0 {
		return [][]byte{}, nil
	}

	// Use iterator to find all members
	iterator := rds.db.NewIterator(rdb.DefaultIteratorConfigs)
	prefix := rds.encodeSetKeyPrefix(key, meta.version)

	result := make([][]byte, 0, meta.size)
	for iterator.Rewind(); iterator.Valid(); iterator.Next() {
		dbKey := iterator.Key()
		if !rds.hasKeyPrefix(dbKey, prefix) {
			break
		}
		member := rds.extractSetMember(dbKey, prefix)
		result = append(result, member)
	}
	return result, nil
}

// SRandMember 随机返回成员
func (rds *RedisDataStructure) SRandMember(key []byte, count int) ([][]byte, error) {
	members, err := rds.SMembers(key)
	if err != nil {
		return nil, err
	}
	if len(members) == 0 {
		return [][]byte{}, nil
	}

	if count >= 0 {
		// Return up to count members (with possible duplicates in real Redis, but we avoid that)
		result := make([][]byte, 0, count)
		for i := 0; i < count && i < len(members); i++ {
			result = append(result, members[i])
		}
		return result, nil
	} else {
		// Negative count means allow duplicates, return |count| members
		result := make([][]byte, 0, -count)
		for i := 0; i < -count; i++ {
			idx := i % len(members)
			result = append(result, members[idx])
		}
		return result, nil
	}
}

// SPop 随机弹出成员
func (rds *RedisDataStructure) SPop(key []byte, count int) ([][]byte, error) {
	members, err := rds.SRandMember(key, count)
	if err != nil {
		return nil, err
	}
	if len(members) == 0 {
		return nil, nil
	}

	wb := rds.db.NewWriteBatch(rdb.DefaultWriteBatchConfigs)
	for _, member := range members {
		sk := &setInternalKey{
			key:     key,
			version: 0, // Will be updated below
			member:  member,
		}
		// Find the actual version
		meta, err := rds.findMetadata(key, Set)
		if err != nil {
			continue
		}
		sk.version = meta.version
		_ = wb.Delete(sk.encode())
		meta.size--
		_ = wb.Put(key, meta.encode())
	}
	if err = wb.Commit(); err != nil {
		return nil, err
	}
	return members, nil
}

// SInter 求交集
func (rds *RedisDataStructure) SInter(keys ...[]byte) ([][]byte, error) {
	if len(keys) == 0 {
		return [][]byte{}, nil
	}

	// Get members of first set
	result, err := rds.SMembers(keys[0])
	if err != nil {
		return nil, err
	}
	if len(result) == 0 {
		return result, nil
	}

	resultSet := make(map[string]struct{})
	for _, m := range result {
		resultSet[string(m)] = struct{}{}
	}

	// Intersect with remaining sets
	for i := 1; i < len(keys); i++ {
		members, err := rds.SMembers(keys[i])
		if err != nil {
			return nil, err
		}
		tempSet := make(map[string]struct{})
		for _, m := range members {
			tempSet[string(m)] = struct{}{}
		}

		// Keep only members in both sets
		for k := range resultSet {
			if _, exists := tempSet[k]; !exists {
				delete(resultSet, k)
			}
		}
	}

	// Convert back to slice
	finalResult := make([][]byte, 0, len(resultSet))
	for k := range resultSet {
		finalResult = append(finalResult, []byte(k))
	}
	return finalResult, nil
}

// SUnion 求并集
func (rds *RedisDataStructure) SUnion(keys ...[]byte) ([][]byte, error) {
	if len(keys) == 0 {
		return [][]byte{}, nil
	}

	resultSet := make(map[string]struct{})

	for _, key := range keys {
		members, err := rds.SMembers(key)
		if err != nil {
			return nil, err
		}
		for _, m := range members {
			resultSet[string(m)] = struct{}{}
		}
	}

	// Convert back to slice
	result := make([][]byte, 0, len(resultSet))
	for k := range resultSet {
		result = append(result, []byte(k))
	}
	return result, nil
}

// SDiff 求差集
func (rds *RedisDataStructure) SDiff(keys ...[]byte) ([][]byte, error) {
	if len(keys) == 0 {
		return [][]byte{}, nil
	}

	// Get members of first set
	result, err := rds.SMembers(keys[0])
	if err != nil {
		return nil, err
	}

	resultSet := make(map[string]struct{})
	for _, m := range result {
		resultSet[string(m)] = struct{}{}
	}

	// Remove members from remaining sets
	for i := 1; i < len(keys); i++ {
		members, err := rds.SMembers(keys[i])
		if err != nil {
			return nil, err
		}
		for _, m := range members {
			delete(resultSet, string(m))
		}
	}

	// Convert back to slice
	finalResult := make([][]byte, 0, len(resultSet))
	for k := range resultSet {
		finalResult = append(finalResult, []byte(k))
	}
	return finalResult, nil
}

// encodeSetKeyPrefix encodes the key and version prefix for set iteration
func (rds *RedisDataStructure) encodeSetKeyPrefix(key []byte, version int64) []byte {
	buf := make([]byte, len(key)+8)
	copy(buf, key)
	binary.LittleEndian.PutUint64(buf[len(key):], uint64(version))
	return buf
}

// extractSetMember extracts the member from a set internal key
func (rds *RedisDataStructure) extractSetMember(dbKey, prefix []byte) []byte {
	fieldLen := len(dbKey) - len(prefix)
	member := make([]byte, fieldLen)
	copy(member, dbKey[len(prefix):])
	return member
}
