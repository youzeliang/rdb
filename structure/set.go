package redis

import (
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
