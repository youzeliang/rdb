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

// zsetMember represents a member with its score for sorting
type zsetMember struct {
	member []byte
	score  float64
}

// ZRange returns members in the specified index range
func (rds *RedisDataStructure) ZRange(key []byte, start, stop int64, withScores bool) ([][]byte, error) {
	meta, err := rds.findMetadata(key, ZSet)
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

	// Get all members and sort by score
	members, err := rds.getAllZSetMembers(key, meta)
	if err != nil {
		return nil, err
	}

	// Sort by score ascending
	sortZSetMembers(members)

	// Extract the requested range
	result := make([][]byte, 0)
	for i := start; i <= stop && i < int64(len(members)); i++ {
		result = append(result, members[i].member)
		if withScores {
			result = append(result, encodeFloat64(members[i].score))
		}
	}
	return result, nil
}

// ZRevRange returns members in reverse order by index
func (rds *RedisDataStructure) ZRevRange(key []byte, start, stop int64, withScores bool) ([][]byte, error) {
	meta, err := rds.findMetadata(key, ZSet)
	if err != nil {
		return nil, err
	}
	if meta.size == 0 {
		return [][]byte{}, nil
	}

	// Convert negative indices (for reverse, -1 is the highest score)
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

	// Get all members and sort by score
	members, err := rds.getAllZSetMembers(key, meta)
	if err != nil {
		return nil, err
	}

	// Sort by score descending
	sortZSetMembersDesc(members)

	// Extract the requested range
	result := make([][]byte, 0)
	for i := start; i <= stop && i < int64(len(members)); i++ {
		result = append(result, members[i].member)
		if withScores {
			result = append(result, encodeFloat64(members[i].score))
		}
	}
	return result, nil
}

// ZRank returns the rank of a member (0-based, ascending by score)
func (rds *RedisDataStructure) ZRank(key []byte, member []byte) (int64, error) {
	members, err := rds.ZRange(key, 0, -1, false)
	if err != nil {
		return 0, err
	}

	for i, m := range members {
		if string(m) == string(member) {
			return int64(i), nil
		}
	}
	return 0, errors.New("ERR no such member")
}

// ZRevRank returns the reverse rank of a member (0-based, descending by score)
func (rds *RedisDataStructure) ZRevRank(key []byte, member []byte) (int64, error) {
	members, err := rds.ZRevRange(key, 0, -1, false)
	if err != nil {
		return 0, err
	}

	for i, m := range members {
		if string(m) == string(member) {
			return int64(i), nil
		}
	}
	return 0, errors.New("ERR no such member")
}

// ZCard returns the number of members in a sorted set
func (rds *RedisDataStructure) ZCard(key []byte) (uint32, error) {
	meta, err := rds.findMetadata(key, ZSet)
	if err != nil {
		return 0, err
	}
	return meta.size, nil
}

// ZCount returns the number of members with scores in the range
func (rds *RedisDataStructure) ZCount(key []byte, min, max float64) (uint32, error) {
	members, err := rds.getAllZSetMembersWithScores(key)
	if err != nil {
		return 0, err
	}

	count := uint32(0)
	for _, m := range members {
		if m.score >= min && m.score <= max {
			count++
		}
	}
	return count, nil
}

// ZIncrBy increments the score of a member
func (rds *RedisDataStructure) ZIncrBy(key []byte, num float64, member []byte) (float64, error) {
	// Get current score
	currentScore, err := rds.ZScore(key, member)
	if err != nil {
		return 0, err
	}

	// If member doesn't exist, currentScore will be NaN, treat as 0
	newScore := currentScore + num
	if math.IsNaN(currentScore) {
		newScore = num
	}

	// Get meta for version
	meta, err := rds.findMetadata(key, ZSet)
	if err != nil {
		return 0, err
	}

	// Update the score
	zk := &zsetInternalKey{
		key:     key,
		version: meta.version,
		member:  member,
		score:   newScore,
	}

	wb := rds.db.NewWriteBatch(rdb.DefaultWriteBatchConfigs)
	exist := !math.IsNaN(currentScore)
	if !exist {
		meta.size++
		_ = wb.Put(key, meta.encode())
	}
	_ = wb.Put(zk.encodeWithMember(), encodeFloat64(newScore))
	_ = wb.Put(zk.encodeWithScore(), nil)
	if err = wb.Commit(); err != nil {
		return 0, err
	}

	return newScore, nil
}

// getAllZSetMembers retrieves all members of a zset
func (rds *RedisDataStructure) getAllZSetMembers(key []byte, meta *metadata) ([]zsetMember, error) {
	// Use iterator to find all members
	iterator := rds.db.NewIterator(rdb.DefaultIteratorConfigs)
	prefix := rds.encodeZSetMemberPrefix(key, meta.version)

	members := make([]zsetMember, 0, meta.size)
	for iterator.Rewind(); iterator.Valid(); iterator.Next() {
		dbKey := iterator.Key()
		if !rds.hasKeyPrefix(dbKey, prefix) {
			break
		}
		member := rds.extractZSetMember(dbKey, prefix)
		scoreVal, _ := iterator.Value()

		var score float64
		if len(scoreVal) > 0 {
			score = decodeFloat64(scoreVal)
		}

		members = append(members, zsetMember{member: member, score: score})
	}
	return members, nil
}

// getAllZSetMembersWithScores is a convenience method
func (rds *RedisDataStructure) getAllZSetMembersWithScores(key []byte) ([]zsetMember, error) {
	meta, err := rds.findMetadata(key, ZSet)
	if err != nil {
		return nil, err
	}
	if meta.size == 0 {
		return []zsetMember{}, nil
	}
	return rds.getAllZSetMembers(key, meta)
}

// encodeZSetMemberPrefix encodes the key and version prefix for zset iteration
func (rds *RedisDataStructure) encodeZSetMemberPrefix(key []byte, version int64) []byte {
	buf := make([]byte, len(key)+8)
	copy(buf, key)
	binary.LittleEndian.PutUint64(buf[len(key):], uint64(version))
	return buf
}

// extractZSetMember extracts the member from a zset internal key
func (rds *RedisDataStructure) extractZSetMember(dbKey, prefix []byte) []byte {
	fieldLen := len(dbKey) - len(prefix)
	member := make([]byte, fieldLen)
	copy(member, dbKey[len(prefix):])
	return member
}

// sortZSetMembers sorts zset members by score ascending
func sortZSetMembers(members []zsetMember) {
	for i := 0; i < len(members); i++ {
		for j := i + 1; j < len(members); j++ {
			if members[j].score < members[i].score {
				members[i], members[j] = members[j], members[i]
			}
		}
	}
}

// sortZSetMembersDesc sorts zset members by score descending
func sortZSetMembersDesc(members []zsetMember) {
	for i := 0; i < len(members); i++ {
		for j := i + 1; j < len(members); j++ {
			if members[j].score > members[i].score {
				members[i], members[j] = members[j], members[i]
			}
		}
	}
}
