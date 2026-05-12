package redis

import (
	"encoding/binary"
	"errors"
	"github.com/youzeliang/rdb"
	"time"
)

func encodeStringValue(value []byte, ttl time.Duration) ([]byte, error) {
	if value == nil {
		return nil, nil
	}
	buf := make([]byte, binary.MaxVarintLen64+1)
	buf[0] = String
	idx := 1
	if ttl != 0 {
		idx += binary.PutVarint(buf[idx:], time.Now().Add(ttl).UnixNano())
	}
	encValue := make([]byte, idx+len(value))
	copy(encValue[:idx], buf[:idx])
	copy(encValue[idx:], value)
	return encValue, nil
}

// Set 设置字符串的值，可以设置过期时间
func (rds *RedisDataStructure) Set(key []byte, value []byte, ttl time.Duration) error {
	encValue, err := encodeStringValue(value, ttl)
	if err != nil {
		return err
	}
	return rds.db.Put(key, encValue)
}

// Get 获取字符串的值
func (rds *RedisDataStructure) Get(key []byte) ([]byte, error) {
	value, err := rds.db.Get(key)
	if err != nil {
		return nil, err
	}

	// 解码
	dataType := value[0]
	if dataType != String {
		return nil, ErrWrongTypeOperation
	}

	var index = 1
	expire, n := binary.Varint(value[index:])
	index += n
	// 判断是否过期
	if expire > 0 && expire <= time.Now().UnixNano() {
		return nil, nil
	}

	return value[index:], nil
}

// StrLen 获取字符串长度
func (rds *RedisDataStructure) StrLen(key []byte) (int, error) {
	value, err := rds.Get(key)
	if err != nil {
		return 0, err
	}
	return len(value), nil
}

// SetNX if the key does not exist, set the string value and return 1, otherwise return 0
func (rds *RedisDataStructure) SetNX(key []byte, value []byte, ttl time.Duration) (int, error) {
	if value == nil {
		return 0, nil
	}

	_, err := rds.db.Get(key)
	if err == nil {
		return 0, nil
	}
	if errors.Is(err, rdb.ErrKeyNotFound) {
		return 0, err
	}

	encValue, err := encodeStringValue(value, ttl)
	if err != nil {
		return 0, err
	}
	err = rds.db.Put(key, encValue)
	if err != nil {
		return 0, err
	}
	return 1, nil
}

// Expire sets the expiration time for a string key, returning 1 on success, or 0 if the key does not exist or has already expired.
func (rds *RedisDataStructure) Expire(key []byte, ttl time.Duration) (int, error) {
	if ttl <= 0 {
		return 0, nil
	}
	value, err := rds.db.Get(key)
	if err != nil {
		if errors.Is(err, rdb.ErrKeyNotFound) {
			return 0, nil
		}
		return 0, err
	}
	if len(value) < 2 {
		return 0, nil
	}
	dataType := value[0]
	if dataType != String {
		return 0, ErrWrongTypeOperation
	}
	var index = 1
	_, n := binary.Varint(value[index:])
	index += n
	payload := value[index:]

	encValue, err := encodeStringValue(payload, ttl)
	if err != nil {
		return 0, err
	}
	err = rds.db.Put(key, encValue)
	if err != nil {
		return 0, err
	}
	return 1, nil
}
