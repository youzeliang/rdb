package redis

import (
	"encoding/binary"
	"time"
)

// Set 设置字符串的值，可以设置过期时间
func (rds *RedisDataStructure) Set(key []byte, value []byte, ttl time.Duration) error {
	if value == nil {
		return nil
	}

	// 编码 value : type + expire + payload
	buf := make([]byte, binary.MaxVarintLen64+1)
	buf[0] = String
	var index = 1
	var expire int64 = 0
	if ttl != 0 {
		expire = time.Now().Add(ttl).UnixNano()
	}
	index += binary.PutVarint(buf[index:], expire)

	encValue := make([]byte, index+len(value))
	copy(encValue[:index], buf[:index])
	copy(encValue[index:], value)

	// 调用存储接口写入数据
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