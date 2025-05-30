package redis

import "time"

type StringCmd interface {
	Set(key []byte, value []byte, ttl time.Duration) error
	Get(key []byte) ([]byte, error)
	StrLen(key []byte) (int, error)
}

type HashCmd interface {
	HSet(key, field, value []byte) (bool, error)
	HGet(key, field []byte) ([]byte, error)
	HDel(key, field []byte) (bool, error)
}

type ListCmd interface {
	LPush(key, element []byte) (uint32, error)
	RPush(key, element []byte) (uint32, error)
	LPop(key []byte) ([]byte, error)
	RPop(key []byte) ([]byte, error)
}

type SetCmd interface {
	SAdd(key, member []byte) (bool, error)
	SRem(key, member []byte) (bool, error)
	SIsMember(key, member []byte) (bool, error)
}

type ZSetCmd interface {
	ZAdd(key []byte, score float64, member []byte) (bool, error)
	ZScore(key []byte, member []byte) (float64, error)
} 