package main

import (
	"encoding/binary"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/tidwall/redcon"
	"github.com/youzeliang/rdb"
	st "github.com/youzeliang/rdb/structure"
	"github.com/youzeliang/rdb/utils"
)

func newWrongNumberOfArgsError(cmd string) error {
	return fmt.Errorf("ERR wrong number of arguments for '%s' command", cmd)
}

type cmdHandler func(cli *BitcaskClient, args [][]byte) (interface{}, error)

// 先支持这些常用的 Redis 命令
var supportedCommands = map[string]cmdHandler{
	// Original commands
	"set":   set,
	"get":   get,
	"hset":  hset,
	"sadd":  sadd,
	"lpush": lpush,
	"zadd":  zadd,
	// Key commands
	"del":    del,
	"exists": exists,
	"type":   typ,
	"expire": expire,
	"ttl":    ttl,
	"persist": persist,
	"rename": rename,
	"renamenx": renamenx,
	"keys":   keys,
	// String commands
	"mget":   mget,
	"mset":   mset,
	"append": appendCmd,
	"incr":   incr,
	"decr":   decr,
	"incrby": incrby,
	"decrby": decrby,
	"getset": getset,
	// Hash commands
	"hgetall": hgetall,
	"hlen":    hlen,
	"hexists": hexists,
	"hkeys":   hkeys,
	"hvals":   hvals,
	"hincrby": hincrby,
	"hmget":   hmget,
	"hmset":   hmset,
	// List commands
	"llen":      llen,
	"lrange":    lrange,
	"lindex":    lindex,
	"linsert":   linsert,
	"lset":      lset,
	"ltrim":     ltrim,
	"rpoplpush": rpoplpush,
	// Set commands
	"scard":     scard,
	"smembers":  smembers,
	"srandmember": srandmember,
	"spop":     spop,
	"sinter":   sinter,
	"sunion":   sunion,
	"sdiff":    sdiff,
	// ZSet commands
	"zrange":   zrange,
	"zrevrange": zrevrange,
	"zrank":    zrank,
	"zrevrank": zrevrank,
	"zcard":    zcard,
	"zcount":   zcount,
	"zincrby":  zincrby,
}

type BitcaskClient struct {
	server *BitcaskServer
	db     *st.RedisDataStructure
}

func execClientCommand(conn redcon.Conn, cmd redcon.Command) {
	command := strings.ToLower(string(cmd.Args[0]))

	client, _ := conn.Context().(*BitcaskClient)
	switch command {
	case "quit":
		_ = conn.Close()
	case "ping":
		conn.WriteString("PONG")
	default:
		cmdFunc, ok := supportedCommands[command]
		if !ok {
			conn.WriteError("Err unsupported command: '" + command + "'")
			return
		}
		res, err := cmdFunc(client, cmd.Args[1:])
		if err != nil {
			if errors.Is(err, rdb.ErrKeyNotFound) {
				conn.WriteNull()
			} else {
				conn.WriteError(err.Error())
			}
			return
		}
		conn.WriteAny(res)
	}
}

// ============ Original Commands ============

func set(cli *BitcaskClient, args [][]byte) (interface{}, error) {
	if len(args) != 2 {
		return nil, newWrongNumberOfArgsError("set")
	}

	key, value := args[0], args[1]
	if err := cli.db.Set(key, value, 0); err != nil {
		return nil, err
	}
	return redcon.SimpleString("OK"), nil
}

func get(cli *BitcaskClient, args [][]byte) (interface{}, error) {
	if len(args) != 1 {
		return nil, newWrongNumberOfArgsError("get")
	}

	value, err := cli.db.Get(args[0])
	if err != nil {
		return nil, err
	}
	return value, nil
}

func hset(cli *BitcaskClient, args [][]byte) (interface{}, error) {
	if len(args) != 3 {
		return nil, newWrongNumberOfArgsError("hset")
	}

	var ok = 0
	key, field, value := args[0], args[1], args[2]
	res, err := cli.db.HSet(key, field, value)
	if err != nil {
		return nil, err
	}
	if res {
		ok = 1
	}
	return redcon.SimpleInt(ok), nil
}

func sadd(cli *BitcaskClient, args [][]byte) (interface{}, error) {
	if len(args) != 2 {
		return nil, newWrongNumberOfArgsError("sadd")
	}

	var ok = 0
	key, member := args[0], args[1]
	res, err := cli.db.SAdd(key, member)
	if err != nil {
		return nil, err
	}
	if res {
		ok = 1
	}
	return redcon.SimpleInt(ok), nil
}

func lpush(cli *BitcaskClient, args [][]byte) (interface{}, error) {
	if len(args) != 2 {
		return nil, newWrongNumberOfArgsError("lpush")
	}

	key, value := args[0], args[1]
	res, err := cli.db.LPush(key, value)
	if err != nil {
		return nil, err
	}
	return redcon.SimpleInt(res), nil
}

func zadd(cli *BitcaskClient, args [][]byte) (interface{}, error) {
	if len(args) != 3 {
		return nil, newWrongNumberOfArgsError("zadd")
	}

	var ok = 0
	key, score, member := args[0], args[1], args[2]
	res, err := cli.db.ZAdd(key, utils.FloatFromBytes(score), member)
	if err != nil {
		return nil, err
	}
	if res {
		ok = 1
	}
	return redcon.SimpleInt(ok), nil
}

// ============ Key Commands ============

func del(cli *BitcaskClient, args [][]byte) (interface{}, error) {
	if len(args) < 1 {
		return nil, newWrongNumberOfArgsError("del")
	}

	deleted := 0
	for _, key := range args {
		err := cli.db.Delete(key)
		if err == nil {
			deleted++
		}
	}
	return redcon.SimpleInt(deleted), nil
}

func exists(cli *BitcaskClient, args [][]byte) (interface{}, error) {
	if len(args) < 1 {
		return nil, newWrongNumberOfArgsError("exists")
	}

	exists := 0
	for _, key := range args {
		_, err := cli.db.Get(key)
		if err == nil {
			exists++
		}
	}
	return redcon.SimpleInt(exists), nil
}

func typ(cli *BitcaskClient, args [][]byte) (interface{}, error) {
	if len(args) != 1 {
		return nil, newWrongNumberOfArgsError("type")
	}

	// We need to check the data type
	// Since the DB interface doesn't expose Type directly, we try to get the key
	// and check error type, or we need to add a Type method
	value, err := cli.db.Get(args[0])
	if err != nil {
		if errors.Is(err, rdb.ErrKeyNotFound) {
			return redcon.SimpleString("none"), nil
		}
		return nil, err
	}

	// Decode type from value
	dataType := value[0]
	switch dataType {
	case st.String:
		return redcon.SimpleString("string"), nil
	case st.Hash:
		return redcon.SimpleString("hash"), nil
	case st.List:
		return redcon.SimpleString("list"), nil
	case st.Set:
		return redcon.SimpleString("set"), nil
	case st.ZSet:
		return redcon.SimpleString("zset"), nil
	default:
		return redcon.SimpleString("unknown"), nil
	}
}

func expire(cli *BitcaskClient, args [][]byte) (interface{}, error) {
	if len(args) != 2 {
		return nil, newWrongNumberOfArgsError("expire")
	}

	key := args[0]
	seconds, err := strconv.ParseInt(string(args[1]), 10, 64)
	if err != nil {
		return nil, errors.New("ERR value is not an integer")
	}
	if seconds <= 0 {
		return redcon.SimpleInt(0), nil
	}

	ttl := time.Duration(seconds) * time.Second
	res, err := cli.db.Expire(key, ttl)
	return redcon.SimpleInt(res), err
}

func ttl(cli *BitcaskClient, args [][]byte) (interface{}, error) {
	if len(args) != 1 {
		return nil, newWrongNumberOfArgsError("ttl")
	}

	// Get the value and decode expire time
	value, err := cli.db.Get(args[0])
	if err != nil {
		if errors.Is(err, rdb.ErrKeyNotFound) {
			return redcon.SimpleInt(-2), nil
		}
		return nil, err
	}

	// Decode expire time
	dataType := value[0]
	if dataType != st.String {
		return redcon.SimpleInt(-1), nil
	}

	var index = 1
	expire, n := binary.Varint(value[index:])
	index += n

	if expire == 0 {
		return redcon.SimpleInt(-1), nil
	}

	if expire > 0 && expire <= time.Now().UnixNano() {
		return redcon.SimpleInt(-2), nil
	}

	// Calculate remaining TTL in seconds
	remaining := (expire - time.Now().UnixNano()) / int64(time.Second)
	if remaining < 0 {
		remaining = 0
	}
	return redcon.SimpleInt(remaining), nil
}

func persist(cli *BitcaskClient, args [][]byte) (interface{}, error) {
	if len(args) != 1 {
		return nil, newWrongNumberOfArgsError("persist")
	}

	// Get value, decode, and re-encode without expire
	value, err := cli.db.Get(args[0])
	if err != nil {
		if errors.Is(err, rdb.ErrKeyNotFound) {
			return redcon.SimpleInt(0), nil
		}
		return nil, err
	}

	dataType := value[0]
	if dataType != st.String {
		return redcon.SimpleInt(0), nil
	}

	// Decode and remove expire
	var index = 1
	_, n := binary.Varint(value[index:])
	index += n
	payload := value[index:]

	// Re-encode without expire
	buf := make([]byte, binary.MaxVarintLen64+1)
	buf[0] = st.String
	idx := 1
	idx += binary.PutVarint(buf[idx:], 0) // expire = 0 means no expire

	encValue := make([]byte, idx+len(payload))
	copy(encValue[:idx], buf[:idx])
	copy(encValue[idx:], payload)

	err = cli.db.Put(args[0], encValue)
	if err != nil {
		return redcon.SimpleInt(0), nil
	}
	return redcon.SimpleInt(1), nil
}

func rename(cli *BitcaskClient, args [][]byte) (interface{}, error) {
	if len(args) != 2 {
		return nil, newWrongNumberOfArgsError("rename")
	}

	key, newKey := args[0], args[1]

	// Get value of old key
	value, err := cli.db.Get(key)
	if err != nil {
		return nil, err
	}

	// Delete new key if exists and put value
	wb := cli.db.NewWriteBatch(rdb.DefaultWriteBatchConfigs)
	_ = wb.Delete(newKey)
	_ = wb.Put(newKey, value)
	err = wb.Commit()
	if err != nil {
		return nil, err
	}

	// Delete old key
	err = cli.db.Delete(key)
	if err != nil {
		return nil, err
	}

	return redcon.SimpleString("OK"), nil
}

func renamenx(cli *BitcaskClient, args [][]byte) (interface{}, error) {
	if len(args) != 2 {
		return nil, newWrongNumberOfArgsError("renamenx")
	}

	key, newKey := args[0], args[1]

	// Check if newKey exists
	_, err := cli.db.Get(newKey)
	if err == nil {
		return redcon.SimpleInt(0), nil
	}
	if !errors.Is(err, rdb.ErrKeyNotFound) {
		return nil, err
	}

	// Get value of old key
	value, err := cli.db.Get(key)
	if err != nil {
		return nil, err
	}

	// Put value at new key
	err = cli.db.Put(newKey, value)
	if err != nil {
		return nil, err
	}

	// Delete old key
	err = cli.db.Delete(key)
	if err != nil {
		return nil, err
	}

	return redcon.SimpleInt(1), nil
}

func keys(cli *BitcaskClient, args [][]byte) (interface{}, error) {
	if len(args) != 1 {
		return nil, newWrongNumberOfArgsError("keys")
	}

	pattern := string(args[0])

	// Use iterator to find all keys
	iterator := cli.db.NewIterator(rdb.DefaultIteratorConfigs)

	result := make([][]byte, 0)
	for iterator.Rewind(); iterator.Valid(); iterator.Next() {
		key := iterator.Key()
		keyStr := string(key)

		// Simple pattern matching - convert Redis pattern to our matching
		if matchPattern(pattern, keyStr) {
			result = append(result, key)
		}
	}

	// Return as array
	return result, nil
}

// matchPattern does simple pattern matching for KEYS command
func matchPattern(pattern, key string) bool {
	// Handle simple patterns like "user:*" or "*"
	if pattern == "*" {
		return true
	}

	// Handle patterns with *
	if strings.Contains(pattern, "*") {
		parts := strings.Split(pattern, "*")
		idx := 0
		for _, part := range parts {
			if part == "" {
				continue
			}
			pos := strings.Index(key[idx:], part)
			if pos == -1 {
				return false
			}
			idx += pos + len(part)
		}
		return true
	}

	// Exact match
	return key == pattern
}

// ============ String Commands ============

func mget(cli *BitcaskClient, args [][]byte) (interface{}, error) {
	if len(args) < 1 {
		return nil, newWrongNumberOfArgsError("mget")
	}

	values, err := cli.db.MGet(args...)
	if err != nil {
		return nil, err
	}

	result := make([]interface{}, len(values))
	for i, v := range values {
		if v == nil {
			result[i] = nil
		} else {
			result[i] = v
		}
	}
	return result, nil
}

func mset(cli *BitcaskClient, args [][]byte) (interface{}, error) {
	if len(args) < 2 || len(args)%2 != 0 {
		return nil, newWrongNumberOfArgsError("mset")
	}

	err := cli.db.MSet(args...)
	if err != nil {
		return nil, err
	}
	return redcon.SimpleString("OK"), nil
}

func appendCmd(cli *BitcaskClient, args [][]byte) (interface{}, error) {
	if len(args) != 2 {
		return nil, newWrongNumberOfArgsError("append")
	}

	res, err := cli.db.Append(args[0], args[1])
	return redcon.SimpleInt(res), err
}

func incr(cli *BitcaskClient, args [][]byte) (interface{}, error) {
	if len(args) != 1 {
		return nil, newWrongNumberOfArgsError("incr")
	}

	res, err := cli.db.Incr(args[0])
	return redcon.SimpleInt(res), err
}

func decr(cli *BitcaskClient, args [][]byte) (interface{}, error) {
	if len(args) != 1 {
		return nil, newWrongNumberOfArgsError("decr")
	}

	res, err := cli.db.Decr(args[0])
	return redcon.SimpleInt(res), err
}

func incrby(cli *BitcaskClient, args [][]byte) (interface{}, error) {
	if len(args) != 2 {
		return nil, newWrongNumberOfArgsError("incrby")
	}

	num, err := strconv.ParseInt(string(args[1]), 10, 64)
	if err != nil {
		return nil, errors.New("ERR value is not an integer")
	}

	res, err := cli.db.IncrBy(args[0], num)
	return redcon.SimpleInt(res), err
}

func decrby(cli *BitcaskClient, args [][]byte) (interface{}, error) {
	if len(args) != 2 {
		return nil, newWrongNumberOfArgsError("decrby")
	}

	num, err := strconv.ParseInt(string(args[1]), 10, 64)
	if err != nil {
		return nil, errors.New("ERR value is not an integer")
	}

	res, err := cli.db.DecrBy(args[0], num)
	return redcon.SimpleInt(res), err
}

func getset(cli *BitcaskClient, args [][]byte) (interface{}, error) {
	if len(args) != 2 {
		return nil, newWrongNumberOfArgsError("getset")
	}

	res, err := cli.db.GetSet(args[0], args[1])
	return res, err
}

// ============ Hash Commands ============

func hgetall(cli *BitcaskClient, args [][]byte) (interface{}, error) {
	if len(args) != 1 {
		return nil, newWrongNumberOfArgsError("hgetall")
	}

	res, err := cli.db.HGetAll(args[0])
	if err != nil {
		return nil, err
	}

	result := make([]interface{}, len(res))
	for i, v := range res {
		result[i] = v
	}
	return result, nil
}

func hlen(cli *BitcaskClient, args [][]byte) (interface{}, error) {
	if len(args) != 1 {
		return nil, newWrongNumberOfArgsError("hlen")
	}

	res, err := cli.db.HLen(args[0])
	return redcon.SimpleInt(res), err
}

func hexists(cli *BitcaskClient, args [][]byte) (interface{}, error) {
	if len(args) != 2 {
		return nil, newWrongNumberOfArgsError("hexists")
	}

	res, err := cli.db.HExists(args[0], args[1])
	return redcon.SimpleInt(res), err
}

func hkeys(cli *BitcaskClient, args [][]byte) (interface{}, error) {
	if len(args) != 1 {
		return nil, newWrongNumberOfArgsError("hkeys")
	}

	res, err := cli.db.HKeys(args[0])
	if err != nil {
		return nil, err
	}

	result := make([]interface{}, len(res))
	for i, v := range res {
		result[i] = v
	}
	return result, nil
}

func hvals(cli *BitcaskClient, args [][]byte) (interface{}, error) {
	if len(args) != 1 {
		return nil, newWrongNumberOfArgsError("hvals")
	}

	res, err := cli.db.HVals(args[0])
	if err != nil {
		return nil, err
	}

	result := make([]interface{}, len(res))
	for i, v := range res {
		result[i] = v
	}
	return result, nil
}

func hincrby(cli *BitcaskClient, args [][]byte) (interface{}, error) {
	if len(args) != 3 {
		return nil, newWrongNumberOfArgsError("hincrby")
	}

	num, err := strconv.ParseInt(string(args[2]), 10, 64)
	if err != nil {
		return nil, errors.New("ERR value is not an integer")
	}

	res, err := cli.db.HIncrBy(args[0], args[1], num)
	return redcon.SimpleInt(res), err
}

func hmget(cli *BitcaskClient, args [][]byte) (interface{}, error) {
	if len(args) < 2 {
		return nil, newWrongNumberOfArgsError("hmget")
	}

	res, err := cli.db.HMGet(args[0], args[1:]...)
	if err != nil {
		return nil, err
	}

	result := make([]interface{}, len(res))
	for i, v := range res {
		if v == nil {
			result[i] = nil
		} else {
			result[i] = v
		}
	}
	return result, nil
}

func hmset(cli *BitcaskClient, args [][]byte) (interface{}, error) {
	if len(args) < 3 || len(args)%2 != 1 {
		return nil, newWrongNumberOfArgsError("hmset")
	}

	err := cli.db.HMSet(args[0], args[1:]...)
	if err != nil {
		return nil, err
	}
	return redcon.SimpleString("OK"), nil
}

// ============ List Commands ============

func llen(cli *BitcaskClient, args [][]byte) (interface{}, error) {
	if len(args) != 1 {
		return nil, newWrongNumberOfArgsError("llen")
	}

	res, err := cli.db.LLen(args[0])
	return redcon.SimpleInt(res), err
}

func lrange(cli *BitcaskClient, args [][]byte) (interface{}, error) {
	if len(args) != 3 {
		return nil, newWrongNumberOfArgsError("lrange")
	}

	start, err := strconv.ParseInt(string(args[1]), 10, 64)
	if err != nil {
		return nil, errors.New("ERR value is not an integer")
	}
	stop, err := strconv.ParseInt(string(args[2]), 10, 64)
	if err != nil {
		return nil, errors.New("ERR value is not an integer")
	}

	res, err := cli.db.LRange(args[0], start, stop)
	if err != nil {
		return nil, err
	}

	result := make([]interface{}, len(res))
	for i, v := range res {
		result[i] = v
	}
	return result, nil
}

func lindex(cli *BitcaskClient, args [][]byte) (interface{}, error) {
	if len(args) != 2 {
		return nil, newWrongNumberOfArgsError("lindex")
	}

	index, err := strconv.ParseInt(string(args[1]), 10, 64)
	if err != nil {
		return nil, errors.New("ERR value is not an integer")
	}

	res, err := cli.db.LIndex(args[0], index)
	return res, err
}

func linsert(cli *BitcaskClient, args [][]byte) (interface{}, error) {
	if len(args) != 4 {
		return nil, newWrongNumberOfArgsError("linsert")
	}

	isBefore := strings.ToLower(string(args[1])) == "before"
	res, err := cli.db.LInsert(args[0], isBefore, args[2], args[3])
	return redcon.SimpleInt(res), err
}

func lset(cli *BitcaskClient, args [][]byte) (interface{}, error) {
	if len(args) != 3 {
		return nil, newWrongNumberOfArgsError("lset")
	}

	index, err := strconv.ParseInt(string(args[1]), 10, 64)
	if err != nil {
		return nil, errors.New("ERR value is not an integer")
	}

	err = cli.db.LSet(args[0], index, args[2])
	if err != nil {
		return nil, err
	}
	return redcon.SimpleString("OK"), nil
}

func ltrim(cli *BitcaskClient, args [][]byte) (interface{}, error) {
	if len(args) != 3 {
		return nil, newWrongNumberOfArgsError("ltrim")
	}

	start, err := strconv.ParseInt(string(args[1]), 10, 64)
	if err != nil {
		return nil, errors.New("ERR value is not an integer")
	}
	stop, err := strconv.ParseInt(string(args[2]), 10, 64)
	if err != nil {
		return nil, errors.New("ERR value is not an integer")
	}

	err = cli.db.LTrim(args[0], start, stop)
	if err != nil {
		return nil, err
	}
	return redcon.SimpleString("OK"), nil
}

func rpoplpush(cli *BitcaskClient, args [][]byte) (interface{}, error) {
	if len(args) != 2 {
		return nil, newWrongNumberOfArgsError("rpoplpush")
	}

	res, err := cli.db.RPopLPush(args[0], args[1])
	return res, err
}

// ============ Set Commands ============

func scard(cli *BitcaskClient, args [][]byte) (interface{}, error) {
	if len(args) != 1 {
		return nil, newWrongNumberOfArgsError("scard")
	}

	res, err := cli.db.SCard(args[0])
	return redcon.SimpleInt(res), err
}

func smembers(cli *BitcaskClient, args [][]byte) (interface{}, error) {
	if len(args) != 1 {
		return nil, newWrongNumberOfArgsError("smembers")
	}

	res, err := cli.db.SMembers(args[0])
	if err != nil {
		return nil, err
	}

	result := make([]interface{}, len(res))
	for i, v := range res {
		result[i] = v
	}
	return result, nil
}

func srandmember(cli *BitcaskClient, args [][]byte) (interface{}, error) {
	if len(args) < 1 {
		return nil, newWrongNumberOfArgsError("srandmember")
	}

	count := 1
	if len(args) >= 2 {
		var err error
		count, err = strconv.Atoi(string(args[1]))
		if err != nil {
			count = 1
		}
	}

	res, err := cli.db.SRandMember(args[0], count)
	if err != nil {
		return nil, err
	}

	if len(res) == 0 {
		return nil, nil
	}

	if count == 1 && len(res) == 1 {
		return res[0], nil
	}

	result := make([]interface{}, len(res))
	for i, v := range res {
		result[i] = v
	}
	return result, nil
}

func spop(cli *BitcaskClient, args [][]byte) (interface{}, error) {
	if len(args) < 1 {
		return nil, newWrongNumberOfArgsError("spop")
	}

	count := 1
	if len(args) >= 2 {
		var err error
		count, err = strconv.Atoi(string(args[1]))
		if err != nil {
			count = 1
		}
	}

	res, err := cli.db.SPop(args[0], count)
	if err != nil {
		return nil, err
	}

	if len(res) == 0 {
		return nil, nil
	}

	if count == 1 && len(res) == 1 {
		return res[0], nil
	}

	result := make([]interface{}, len(res))
	for i, v := range res {
		result[i] = v
	}
	return result, nil
}

func sinter(cli *BitcaskClient, args [][]byte) (interface{}, error) {
	if len(args) < 1 {
		return nil, newWrongNumberOfArgsError("sinter")
	}

	res, err := cli.db.SInter(args...)
	if err != nil {
		return nil, err
	}

	result := make([]interface{}, len(res))
	for i, v := range res {
		result[i] = v
	}
	return result, nil
}

func sunion(cli *BitcaskClient, args [][]byte) (interface{}, error) {
	if len(args) < 1 {
		return nil, newWrongNumberOfArgsError("sunion")
	}

	res, err := cli.db.SUnion(args...)
	if err != nil {
		return nil, err
	}

	result := make([]interface{}, len(res))
	for i, v := range res {
		result[i] = v
	}
	return result, nil
}

func sdiff(cli *BitcaskClient, args [][]byte) (interface{}, error) {
	if len(args) < 1 {
		return nil, newWrongNumberOfArgsError("sdiff")
	}

	res, err := cli.db.SDiff(args...)
	if err != nil {
		return nil, err
	}

	result := make([]interface{}, len(res))
	for i, v := range res {
		result[i] = v
	}
	return result, nil
}

// ============ ZSet Commands ============

func zrange(cli *BitcaskClient, args [][]byte) (interface{}, error) {
	if len(args) < 3 {
		return nil, newWrongNumberOfArgsError("zrange")
	}

	start, err := strconv.ParseInt(string(args[1]), 10, 64)
	if err != nil {
		return nil, errors.New("ERR value is not an integer")
	}
	stop, err := strconv.ParseInt(string(args[2]), 10, 64)
	if err != nil {
		return nil, errors.New("ERR value is not an integer")
	}

	withScores := false
	if len(args) >= 4 && strings.ToLower(string(args[3])) == "withscores" {
		withScores = true
	}

	res, err := cli.db.ZRange(args[0], start, stop, withScores)
	if err != nil {
		return nil, err
	}

	result := make([]interface{}, len(res))
	for i, v := range res {
		result[i] = v
	}
	return result, nil
}

func zrevrange(cli *BitcaskClient, args [][]byte) (interface{}, error) {
	if len(args) < 3 {
		return nil, newWrongNumberOfArgsError("zrevrange")
	}

	start, err := strconv.ParseInt(string(args[1]), 10, 64)
	if err != nil {
		return nil, errors.New("ERR value is not an integer")
	}
	stop, err := strconv.ParseInt(string(args[2]), 10, 64)
	if err != nil {
		return nil, errors.New("ERR value is not an integer")
	}

	withScores := false
	if len(args) >= 4 && strings.ToLower(string(args[3])) == "withscores" {
		withScores = true
	}

	res, err := cli.db.ZRevRange(args[0], start, stop, withScores)
	if err != nil {
		return nil, err
	}

	result := make([]interface{}, len(res))
	for i, v := range res {
		result[i] = v
	}
	return result, nil
}

func zrank(cli *BitcaskClient, args [][]byte) (interface{}, error) {
	if len(args) != 2 {
		return nil, newWrongNumberOfArgsError("zrank")
	}

	res, err := cli.db.ZRank(args[0], args[1])
	if err != nil {
		return nil, err
	}
	return redcon.SimpleInt(res), nil
}

func zrevrank(cli *BitcaskClient, args [][]byte) (interface{}, error) {
	if len(args) != 2 {
		return nil, newWrongNumberOfArgsError("zrevrank")
	}

	res, err := cli.db.ZRevRank(args[0], args[1])
	if err != nil {
		return nil, err
	}
	return redcon.SimpleInt(res), nil
}

func zcard(cli *BitcaskClient, args [][]byte) (interface{}, error) {
	if len(args) != 1 {
		return nil, newWrongNumberOfArgsError("zcard")
	}

	res, err := cli.db.ZCard(args[0])
	return redcon.SimpleInt(res), err
}

func zcount(cli *BitcaskClient, args [][]byte) (interface{}, error) {
	if len(args) != 3 {
		return nil, newWrongNumberOfArgsError("zcount")
	}

	min := utils.FloatFromBytes(args[1])
	max := utils.FloatFromBytes(args[2])

	res, err := cli.db.ZCount(args[0], min, max)
	return redcon.SimpleInt(res), err
}

func zincrby(cli *BitcaskClient, args [][]byte) (interface{}, error) {
	if len(args) != 3 {
		return nil, newWrongNumberOfArgsError("zincrby")
	}

	num := utils.FloatFromBytes(args[1])

	res, err := cli.db.ZIncrBy(args[0], num, args[2])
	return res, err
}
