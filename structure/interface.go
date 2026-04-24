package redis

import "time"

type StringCmd interface {
	Set(key []byte, value []byte, ttl time.Duration) error
	Get(key []byte) ([]byte, error)
	StrLen(key []byte) (int, error)
	SetNX(key []byte, value []byte, ttl time.Duration) (int, error)
	Expire(key []byte, ttl time.Duration) (int, error)
	MGet(keys ...[]byte) ([][]byte, error)
	MSet(keyValues ...[]byte) error
	Append(key []byte, value []byte) (int, error)
	Incr(key []byte) (int64, error)
	Decr(key []byte) (int64, error)
	IncrBy(key []byte, num int64) (int64, error)
	DecrBy(key []byte, num int64) (int64, error)
	GetSet(key []byte, value []byte) ([]byte, error)
}

type HashCmd interface {
	HSet(key, field, value []byte) (bool, error)
	HGet(key, field []byte) ([]byte, error)
	HDel(key, field []byte) (bool, error)
	HGetAll(key []byte) ([][]byte, error)
	HLen(key []byte) (uint32, error)
	HExists(key, field []byte) (int, error)
	HKeys(key []byte) ([][]byte, error)
	HVals(key []byte) ([][]byte, error)
	HIncrBy(key, field []byte, num int64) (int64, error)
	HMGet(key []byte, fields ...[]byte) ([][]byte, error)
	HMSet(key []byte, fieldValues ...[]byte) error
}

type ListCmd interface {
	LPush(key, element []byte) (uint32, error)
	RPush(key, element []byte) (uint32, error)
	LPop(key []byte) ([]byte, error)
	RPop(key []byte) ([]byte, error)
	LLen(key []byte) (uint32, error)
	LRange(key []byte, start, stop int64) ([][]byte, error)
	LIndex(key []byte, index int64) ([]byte, error)
	LInsert(key []byte, isBefore bool, pivot, element []byte) (int64, error)
	LSet(key []byte, index int64, element []byte) error
	LTrim(key []byte, start, stop int64) error
	RPopLPush(source, destination []byte) ([]byte, error)
}

type SetCmd interface {
	SAdd(key, member []byte) (bool, error)
	SRem(key, member []byte) (bool, error)
	SIsMember(key, member []byte) (bool, error)
	SCard(key []byte) (uint32, error)
	SMembers(key []byte) ([][]byte, error)
	SRandMember(key []byte, count int) ([][]byte, error)
	SPop(key []byte, count int) ([][]byte, error)
	SInter(keys ...[]byte) ([][]byte, error)
	SUnion(keys ...[]byte) ([][]byte, error)
	SDiff(keys ...[]byte) ([][]byte, error)
}

type ZSetCmd interface {
	ZAdd(key []byte, score float64, member []byte) (bool, error)
	ZScore(key []byte, member []byte) (float64, error)
	ZRange(key []byte, start, stop int64, withScores bool) ([][]byte, error)
	ZRevRange(key []byte, start, stop int64, withScores bool) ([][]byte, error)
	ZRank(key []byte, member []byte) (int64, error)
	ZRevRank(key []byte, member []byte) (int64, error)
	ZCard(key []byte) (uint32, error)
	ZCount(key []byte, min, max float64) (uint32, error)
	ZIncrBy(key []byte, num float64, member []byte) (float64, error)
}
