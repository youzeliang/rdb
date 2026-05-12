package redis

import (
	"encoding/binary"
	"errors"
	"fmt"
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

// MGet batch gets values for multiple keys
func (rds *RedisDataStructure) MGet(keys ...[]byte) ([][]byte, error) {
	values := make([][]byte, len(keys))
	for i, key := range keys {
		value, err := rds.Get(key)
		if err != nil {
			return nil, err
		}
		values[i] = value
	}
	return values, nil
}

// MSet batch sets multiple key-value pairs
func (rds *RedisDataStructure) MSet(keyValues ...[]byte) error {
	if len(keyValues)%2 != 0 {
		return errors.New("MSet requires an even number of arguments")
	}
	for i := 0; i < len(keyValues); i += 2 {
		key := keyValues[i]
		value := keyValues[i+1]
		if err := rds.Set(key, value, 0); err != nil {
			return err
		}
	}
	return nil
}

// Append appends the value to the existing string or creates a new one
func (rds *RedisDataStructure) Append(key []byte, value []byte) (int, error) {
	if value == nil {
		return 0, nil
	}

	// Try to get existing value
	oldValue, err := rds.Get(key)
	if err != nil {
		if errors.Is(err, rdb.ErrKeyNotFound) {
			// Key doesn't exist, just set it
			if err := rds.Set(key, value, 0); err != nil {
				return 0, err
			}
			return len(value), nil
		}
		return 0, err
	}

	// Append the value
	newValue := append(oldValue, value...)
	if err := rds.Set(key, newValue, 0); err != nil {
		return 0, err
	}
	return len(newValue), nil
}

// Incr increments the value by 1
func (rds *RedisDataStructure) Incr(key []byte) (int64, error) {
	return rds.incrBy(key, 1)
}

// Decr decrements the value by 1
func (rds *RedisDataStructure) Decr(key []byte) (int64, error) {
	return rds.incrBy(key, -1)
}

// IncrBy increments the value by the specified amount
func (rds *RedisDataStructure) IncrBy(key []byte, num int64) (int64, error) {
	return rds.incrBy(key, num)
}

// DecrBy decrements the value by the specified amount
func (rds *RedisDataStructure) DecrBy(key []byte, num int64) (int64, error) {
	return rds.incrBy(key, -num)
}

// incrBy internal method for increment/decrement
func (rds *RedisDataStructure) incrBy(key []byte, num int64) (int64, error) {
	value, err := rds.db.Get(key)
	if err != nil && !errors.Is(err, rdb.ErrKeyNotFound) {
		return 0, err
	}

	var oldVal int64 = 0
	if errors.Is(err, rdb.ErrKeyNotFound) {
		// Key doesn't exist, start from 0
		oldVal = 0
	} else {
		// Decode and parse existing value
		dataType := value[0]
		if dataType != String {
			return 0, ErrWrongTypeOperation
		}
		var index = 1
		expire, n := binary.Varint(value[index:])
		index += n
		if expire > 0 && expire <= time.Now().UnixNano() {
			// Key has expired, treat as non-existing
			oldVal = 0
		} else {
			payload := value[index:]
			if len(payload) == 0 {
				oldVal = 0
			} else {
				oldVal, err = parseInt64(payload)
				if err != nil {
					return 0, errors.New("ERR value is not an integer")
				}
			}
		}
	}

	newVal := oldVal + num
	newPayload := formatInt64(newVal)

	if err := rds.Set(key, newPayload, 0); err != nil {
		return 0, err
	}
	return newVal, nil
}

// GetSet gets the old value and sets the new value
func (rds *RedisDataStructure) GetSet(key []byte, value []byte) ([]byte, error) {
	oldValue, err := rds.Get(key)
	if err != nil {
		return nil, err
	}
	if err := rds.Set(key, value, 0); err != nil {
		return nil, err
	}
	return oldValue, nil
}

// parseInt64 parses a byte slice as int64
func parseInt64(data []byte) (int64, error) {
	var neg bool
	if len(data) > 0 && data[0] == '-' {
		neg = true
		data = data[1:]
	}
	var val int64 = 0
	for _, b := range data {
		if b < '0' || b > '9' {
			return 0, errors.New("invalid integer")
		}
		val = val*10 + int64(b-'0')
	}
	if neg {
		val = -val
	}
	return val, nil
}

// formatInt64 converts int64 to byte representation
func formatInt64(val int64) []byte {
	return []byte(fmt.Sprintf("%d", val))
}
