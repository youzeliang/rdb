package redis

import (
	"encoding/binary"
	"errors"
	"github.com/youzeliang/rdb"
	"math"
)

// ZAdd 将元素及其分数添加到有序集合中
func (rds *RedisDataStructure) ZAdd(key []byte, score float64, member []byte) (bool, error) {
	meta, err := rds.findMetadata(key, ZSet)
	if err != nil {
		return false, err
	}

	// 构造数据部分的key
	zk := &zsetInternalKey{
		key:     key,
		version: meta.version,
		member:  member,
		score:   score,
	}

	var exist = true
	// 查看是否已经存在了
	if _, err = rds.db.Get(zk.encodeWithMember()); errors.Is(err, rdb.ErrKeyNotFound) {
		exist = false
	}

	wb := rds.db.NewWriteBatch(rdb.DefaultWriteBatchConfigs)
	if !exist {
		meta.size++
		_ = wb.Put(key, meta.encode())
	}
	// 不管是否存在，都需要更新
	_ = wb.Put(zk.encodeWithMember(), encodeFloat64(score))
	_ = wb.Put(zk.encodeWithScore(), nil)
	if err = wb.Commit(); err != nil {
		return false, err
	}
	return !exist, nil
}

// ZScore 获取有序集合中成员的分数
func (rds *RedisDataStructure) ZScore(key []byte, member []byte) (float64, error) {
	meta, err := rds.findMetadata(key, ZSet)
	if err != nil {
		return math.NaN(), err
	}
	if meta.size == 0 {
		return math.NaN(), nil
	}

	// 构造数据部分的key
	zk := &zsetInternalKey{
		key:     key,
		version: meta.version,
		member:  member,
	}

	value, err := rds.db.Get(zk.encodeWithMember())
	if err != nil {
		if errors.Is(err, rdb.ErrKeyNotFound) {
			return math.NaN(), nil
		}
		return math.NaN(), err
	}
	return decodeFloat64(value), nil
}

// encodeFloat64 编码float64类型的分数
func encodeFloat64(val float64) []byte {
	buf := make([]byte, 8)
	bits := math.Float64bits(val)
	binary.BigEndian.PutUint64(buf, bits)
	return buf
}

// decodeFloat64 解码float64类型的分数
func decodeFloat64(buf []byte) float64 {
	bits := binary.BigEndian.Uint64(buf)
	return math.Float64frombits(bits)
}
