package redis

import (
	"errors"
	"github.com/youzeliang/rdb"
	"time"
)

var (
	ErrWrongTypeOperation = errors.New("WRONGTYPE Operation against a key holding the wrong kind of value")
)

type RedisDataType = byte

const (
	String RedisDataType = iota
	Hash
	Set
	List
	ZSet
)

type RedisDataStructure struct {
	db *rdb.DB
}

// NewRedisDataStructure 初始化 Redis 数据结构服务
func NewRedisDataStructure(options rdb.Options) (*RedisDataStructure, error) {
	db, err := rdb.Open(options)
	if err != nil {
		return nil, err
	}
	return &RedisDataStructure{db: db}, nil
}

func (rds *RedisDataStructure) findMetadata(key []byte, dataType RedisDataType) (*metadata, error) {
	metaBuf, err := rds.db.Get(key)
	if err != nil && !errors.Is(err, rdb.ErrKeyNotFound) {
		return nil, err
	}

	var meta *metadata
	var exist = true
	if errors.Is(err, rdb.ErrKeyNotFound) {
		exist = false
	} else {
		meta = decodeMetadata(metaBuf)
		// 判断数据类型
		if meta.dataType != dataType {
			return nil, ErrWrongTypeOperation
		}
		// 判断过期时间
		if meta.expire != 0 && meta.expire <= time.Now().UnixNano() {
			exist = false
		}
	}

	if !exist {
		meta = &metadata{
			dataType: dataType,
			expire:   0,
			version:  time.Now().UnixNano(),
			size:     0,
		}
		if dataType == List {
			meta.head = initialListMark
			meta.tail = initialListMark
		}
	}
	return meta, nil
}

func (rds *RedisDataStructure) Close() error {
	return rds.db.Close()
}
