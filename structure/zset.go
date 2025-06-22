package redis

import (
	"encoding/binary"
	"errors"
	"github.com/youzeliang/rdb"
	"math"
)

// ZAdd adds a member with a score to a sorted set.
func (rds *RedisDataStructure) ZAdd(key []byte, score float64, member []byte) (bool, error) {
	meta, err := rds.findMetadata(key, ZSet)
	if err != nil {
		return false, err
	}

	// Construct the key for the data part
	zk := &zsetInternalKey{
		key:     key,
		version: meta.version,
		member:  member,
		score:   score,
	}

	var exist = true
	// check if the member already exists
	if _, err = rds.db.Get(zk.encodeWithMember()); errors.Is(err, rdb.ErrKeyNotFound) {
		exist = false
	}

	wb := rds.db.NewWriteBatch(rdb.DefaultWriteBatchConfigs)
	if !exist {
		meta.size++
		_ = wb.Put(key, meta.encode())
	}
	// whether it exists or not, we need to update
	_ = wb.Put(zk.encodeWithMember(), encodeFloat64(score))
	_ = wb.Put(zk.encodeWithScore(), nil)
	if err = wb.Commit(); err != nil {
		return false, err
	}
	return !exist, nil
}

// ZScore get the score of a member in a sorted set.
func (rds *RedisDataStructure) ZScore(key []byte, member []byte) (float64, error) {
	meta, err := rds.findMetadata(key, ZSet)
	if err != nil {
		return math.NaN(), err
	}
	if meta.size == 0 {
		return math.NaN(), nil
	}

	// Construct the key for the data part
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

// encodeFloat64 encodes a float64 value into a byte slice.
func encodeFloat64(val float64) []byte {
	buf := make([]byte, 8)
	bits := math.Float64bits(val)
	binary.BigEndian.PutUint64(buf, bits)
	return buf
}

// decodeFloat64 decodes a byte slice into a float64 value.
func decodeFloat64(buf []byte) float64 {
	bits := binary.BigEndian.Uint64(buf)
	return math.Float64frombits(bits)
}
