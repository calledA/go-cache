package database

import (
	"github.com/shopspring/decimal"
	"gmr/go-cache/datastruct/bitmap"
	"gmr/go-cache/interface/database"
	"gmr/go-cache/interface/redis"
	"gmr/go-cache/lib/utils"
	"gmr/go-cache/redis/protocol"
	"math/bits"
	"strconv"
	"strings"
	"time"
)

/**
 * @Author: wanglei
 * @File: string
 * @Version: 1.0.0
 * @Description:
 * @Date: 2023/08/07 16:20
 */

func (db *DB) getAsString(key string) ([]byte, protocol.ErrorReply) {
	entity, ok := db.GetEntity(key)
	if !ok {
		return nil, nil
	}
	bytes, ok := entity.Data.([]byte)
	if !ok {
		return nil, &protocol.WrongTypeErrorReply{}
	}
	return bytes, nil
}

func execGet(db *DB, args [][]byte) redis.Reply {
	key := string(args[0])
	bytes, err := db.getAsString(key)
	if err != nil {
		return err
	}
	if bytes == nil {
		return &protocol.NullBulkReply{}
	}
	return protocol.MakeBulkReply(bytes)
}

const (
	upsertPolicy = iota
	insertPolicy
	updatePolicy
)

const unlimitedTTL int64 = 0

func execGetEX(db *DB, args [][]byte) redis.Reply {
	key := string(args[0])
	bytes, err := db.getAsString(key)
	ttl := unlimitedTTL

	if err != nil {
		return err
	}
	if bytes == nil {
		return &protocol.NullBulkReply{}
	}

	for i := 1; i < len(args); i++ {
		arg := strings.ToUpper(string(args[i]))
		if arg == "EX" {
			if ttl != unlimitedTTL {
				return &protocol.SyntaxErrorReply{}
			}
			if i+1 >= len(args) {
				return &protocol.SyntaxErrorReply{}
			}
			ttlArg, err := strconv.ParseInt(string(args[i+1]), 10, 64)
			if err != nil {
				return &protocol.SyntaxErrorReply{}
			}
			if ttlArg <= 0 {
				return protocol.MakeErrorReply("ERR invalid expire time in getex")
			}
			ttl = ttlArg * 1000
			i++
		} else if arg == "PX" {
			if ttl != unlimitedTTL {
				return &protocol.SyntaxErrorReply{}
			}
			if i+1 >= len(args) {
				return &protocol.SyntaxErrorReply{}
			}
			ttlArg, err := strconv.ParseInt(string(args[i+1]), 10, 64)
			if err != nil {
				return &protocol.SyntaxErrorReply{}
			}
			if ttlArg <= 0 {
				return protocol.MakeErrorReply("ERR invalid expire time in getex")
			}
			ttl = ttlArg
			i++
		} else if arg == "PERSIST" {
			if ttl != unlimitedTTL {
				return &protocol.SyntaxErrorReply{}
			}
			if i+1 > len(args) {
				return &protocol.SyntaxErrorReply{}
			}
			db.Persist(key)
		}
	}

	if len(args) > 1 {
		if ttl != unlimitedTTL {
			expireTime := time.Now().Add(time.Duration(ttl) * time.Millisecond)
			db.Expire(key, expireTime)
			db.addAof(aof.MakeExpireCmd(key, expireTime).Args)
		} else {
			db.Persist(key)
			db.addAof(utils.ToCmdLineByByte("persist", args[0]))
		}
	}
	return protocol.MakeBulkReply(bytes)
}

func execSet(db *DB, args [][]byte) redis.Reply {
	key := string(args[0])
	value := args[1]
	policy := upsertPolicy
	ttl := unlimitedTTL

	if len(args) > 2 {
		for i := 2; i < len(args); i++ {
			arg := strings.ToUpper(string(args[i]))
			if arg == "NX" {
				if policy == updatePolicy {
					return &protocol.SyntaxErrorReply{}
				}
				policy = insertPolicy
			} else if arg == "XX" {
				if policy == insertPolicy {
					return &protocol.SyntaxErrorReply{}
				}
				policy = updatePolicy
			} else if arg == "EX" {
				if ttl != unlimitedTTL {
					return &protocol.SyntaxErrorReply{}
				}
				if i+1 >= len(args) {
					return &protocol.SyntaxErrorReply{}
				}
				ttlArg, err := strconv.ParseInt(string(args[i+1]), 10, 64)
				if err != nil {
					return &protocol.SyntaxErrorReply{}
				}
				if ttlArg <= 0 {
					return protocol.MakeErrorReply("ERR invalid expire time in set")
				}
				ttl = ttlArg * 1000
				i++
			} else if arg == "PX" {
				if ttl != unlimitedTTL {
					return &protocol.SyntaxErrorReply{}
				}
				if i+1 >= len(args) {
					return &protocol.SyntaxErrorReply{}
				}
				ttlArg, err := strconv.ParseInt(string(args[i+1]), 10, 64)
				if err != nil {
					return &protocol.SyntaxErrorReply{}
				}
				if ttlArg <= 0 {
					return protocol.MakeErrorReply("ERR invalid expire time in set")
				}
				ttl = ttlArg
				i++
			} else {
				return &protocol.SyntaxErrorReply{}
			}
		}
	}

	entity := &database.DataEntity{
		Data: value,
	}

	var result int
	switch policy {
	case upsertPolicy:
		db.PutEntity(key, entity)
		result = 1
	case insertPolicy:
		result = db.PutIfAbsent(key, entity)
	case updatePolicy:
		result = db.PutIfExist(key, entity)
	}
	if result > 0 {
		if ttl != unlimitedTTL {
			expireTime := time.Now().Add(time.Duration(ttl) * time.Millisecond)
			db.Expire(key, expireTime)
			db.addAof(CmdLine{
				[]byte("SET"),
				args[0],
				args[1],
			})
			db.addAof(aof.MakeExpireCmd(key, expireTime).Args)
		} else {
			db.Persist(key)
			db.addAof(utils.ToCmdLineByByte("set", args...))
		}
	}

	if result > 0 {
		return &protocol.OkReply{}
	}
	return &protocol.NullBulkReply{}
}

func execSetNX(db *DB, args [][]byte) redis.Reply {
	key := string(args[0])
	value := args[1]
	entity := &database.DataEntity{
		Data: value,
	}
	result := db.PutIfAbsent(key, entity)
	db.addAof(utils.ToCmdLineByByte("setnx", args...))
	return protocol.MakeIntReply(int64(result))
}

func execSetEX(db *DB, args [][]byte) redis.Reply {
	key := string(args[0])
	value := args[2]

	ttlArg, err := strconv.ParseInt(string(args[1]), 10, 64)
	if err != nil {
		return &protocol.SyntaxErrorReply{}
	}
	if ttlArg <= 0 {
		return protocol.MakeErrorReply("ERR invalid expire time in setex")
	}
	ttl := ttlArg * 1000

	entity := &database.DataEntity{
		Data: value,
	}

	db.PutEntity(key, entity)
	expireTime := time.Now().Add(time.Duration(ttl) * time.Millisecond)
	db.Expire(key, expireTime)
	db.addAof(utils.ToCmdLineByByte("setex", args...))
	db.addAof(aof.MakeExpireCmd(key, expireTime).Args)
	return &protocol.OkReply{}
}

func execPSetEX(db *DB, args [][]byte) redis.Reply {
	key := string(args[0])
	value := args[2]

	ttlArg, err := strconv.ParseInt(string(args[1]), 10, 64)
	if err != nil {
		return &protocol.SyntaxErrorReply{}
	}
	if ttlArg <= 0 {
		return protocol.MakeErrorReply("ERR invalid expire time in setex")
	}

	entity := &database.DataEntity{
		Data: value,
	}

	db.PutEntity(key, entity)
	expireTime := time.Now().Add(time.Duration(ttlArg) * time.Millisecond)
	db.Expire(key, expireTime)
	db.addAof(utils.ToCmdLineByByte("setex", args...))
	db.addAof(aof.MakeExpireCmd(key, expireTime).Args)

	return &protocol.OkReply{}
}

func prepareMSet(args [][]byte) ([]string, []string) {
	size := len(args) / 2
	keys := make([]string, size)
	for i := 0; i < size; i++ {
		keys[i] = string(args[2*i])
	}
	return keys, nil
}

func undoMSet(db *DB, args [][]byte) []CmdLine {
	writeKeys, _ := prepareMSet(args)
	return rollbackGivenKeys(db, writeKeys...)
}

func execMSet(db *DB, args [][]byte) redis.Reply {
	if len(args)%2 != 0 {
		return protocol.MakeSyntaxErrorReply()
	}

	size := len(args) / 2
	keys := make([]string, size)
	values := make([][]byte, size)
	for i := 0; i < size; i++ {
		keys[i] = string(args[2*i])
		values[i] = args[2*i+1]
	}

	for i, key := range keys {
		value := values[i]
		db.PutEntity(key, &database.DataEntity{Data: value})
	}
	db.addAof(utils.ToCmdLineByByte("mset", args...))
	return &protocol.OkReply{}
}

func prepareMGet(args [][]byte) ([]string, []string) {
	keys := make([]string, len(args))
	for i, v := range args {
		keys[i] = string(v)
	}
	return nil, keys
}

func execMGet(db *DB, args [][]byte) redis.Reply {
	keys := make([]string, len(args))
	for i, v := range args {
		keys[i] = string(v)
	}

	result := make([][]byte, len(args))
	for i, key := range keys {
		bytes, err := db.getAsString(key)
		if err != nil {
			_, isWrongType := err.(*protocol.WrongTypeErrorReply)
			if isWrongType {
				result[i] = nil
				continue
			} else {
				return err
			}
		}
		result[i] = bytes
	}

	return protocol.MakeMultiBulkReply(result)
}

func execMSetNX(db *DB, args [][]byte) redis.Reply {
	if len(args)%2 != 0 {
		return protocol.MakeSyntaxErrorReply()
	}
	size := len(args) / 2
	values := make([][]byte, size)
	keys := make([]string, size)
	for i := 0; i < size; i++ {
		keys[i] = string(args[2*i])
		values[i] = args[2*i+1]
	}

	for _, key := range keys {
		_, exists := db.GetEntity(key)
		if exists {
			return protocol.MakeIntReply(0)
		}
	}

	for i, key := range keys {
		value := values[i]
		db.PutEntity(key, &database.DataEntity{Data: value})
	}
	db.addAof(utils.ToCmdLineByByte("msetnx", args...))
	return protocol.MakeIntReply(1)
}

func execGetSet(db *DB, args [][]byte) redis.Reply {
	key := string(args[0])
	value := args[1]

	old, err := db.getAsString(key)
	if err != nil {
		return err
	}

	db.PutEntity(key, &database.DataEntity{Data: value})
	db.Persist(key)
	db.addAof(utils.ToCmdLineByByte("set", args...))
	if old == nil {
		return new(protocol.NullBulkReply)
	}
	return protocol.MakeBulkReply(old)
}

func execGetDel(db *DB, args [][]byte) redis.Reply {
	key := string(args[0])

	old, err := db.getAsString(key)
	if err != nil {
		return err
	}
	if old == nil {
		return new(protocol.NullBulkReply)
	}
	db.Remove(key)

	db.addAof(utils.ToCmdLineByByte("del", args...))
	return protocol.MakeBulkReply(old)
}

func execIncr(db *DB, args [][]byte) redis.Reply {
	key := string(args[0])

	bytes, err := db.getAsString(key)
	if err != nil {
		return err
	}
	if bytes != nil {
		val, err := strconv.ParseInt(string(bytes), 10, 64)
		if err != nil {
			return protocol.MakeErrorReply("ERR value is not an integer or out of range")
		}
		db.PutEntity(key, &database.DataEntity{
			Data: []byte(strconv.FormatInt(val+1, 10)),
		})
		db.addAof(utils.ToCmdLineByByte("incr", args...))
		return protocol.MakeIntReply(val + 1)
	}
	db.PutEntity(key, &database.DataEntity{
		Data: []byte("1"),
	})
	db.addAof(utils.ToCmdLineByByte("incr", args...))
	return protocol.MakeIntReply(1)
}

func execIncrBy(db *DB, args [][]byte) redis.Reply {
	key := string(args[0])
	rawDelta := string(args[1])
	delta, err := strconv.ParseInt(rawDelta, 10, 64)
	if err != nil {
		return protocol.MakeErrorReply("ERR value is not an integer or out of range")
	}

	bytes, errReply := db.getAsString(key)
	if errReply != nil {
		return errReply
	}
	if bytes != nil {
		val, err := strconv.ParseInt(string(bytes), 10, 64)
		if err != nil {
			return protocol.MakeErrorReply("ERR value is not an integer or out of range")
		}
		db.PutEntity(key, &database.DataEntity{
			Data: []byte(strconv.FormatInt(val+delta, 10)),
		})
		db.addAof(utils.ToCmdLineByByte("incrby", args...))
		return protocol.MakeIntReply(val + delta)
	}
	db.PutEntity(key, &database.DataEntity{
		Data: args[1],
	})
	db.addAof(utils.ToCmdLineByByte("incrby", args...))
	return protocol.MakeIntReply(delta)
}

func execIncrByFloat(db *DB, args [][]byte) redis.Reply {
	key := string(args[0])
	rawDelta := string(args[1])
	delta, err := decimal.NewFromString(rawDelta)
	if err != nil {
		return protocol.MakeErrorReply("ERR value is not a valid float")
	}

	bytes, errReply := db.getAsString(key)
	if errReply != nil {
		return errReply
	}
	if bytes != nil {
		val, err := decimal.NewFromString(string(bytes))
		if err != nil {
			return protocol.MakeErrorReply("ERR value is not a valid float")
		}
		resultBytes := []byte(val.Add(delta).String())
		db.PutEntity(key, &database.DataEntity{
			Data: resultBytes,
		})
		db.addAof(utils.ToCmdLineByByte("incrbyfloat", args...))
		return protocol.MakeBulkReply(resultBytes)
	}
	db.PutEntity(key, &database.DataEntity{
		Data: args[1],
	})
	db.addAof(utils.ToCmdLineByByte("incrbyfloat", args...))
	return protocol.MakeBulkReply(args[1])
}

func execDecr(db *DB, args [][]byte) redis.Reply {
	key := string(args[0])

	bytes, errReply := db.getAsString(key)
	if errReply != nil {
		return errReply
	}
	if bytes != nil {
		val, err := strconv.ParseInt(string(bytes), 10, 64)
		if err != nil {
			return protocol.MakeErrorReply("ERR value is not an integer or out of range")
		}
		db.PutEntity(key, &database.DataEntity{
			Data: []byte(strconv.FormatInt(val-1, 10)),
		})
		db.addAof(utils.ToCmdLineByByte("decr", args...))
		return protocol.MakeIntReply(val - 1)
	}
	entity := &database.DataEntity{
		Data: []byte("-1"),
	}
	db.PutEntity(key, entity)
	db.addAof(utils.ToCmdLineByByte("decr", args...))
	return protocol.MakeIntReply(-1)
}

func execDecrBy(db *DB, args [][]byte) redis.Reply {
	key := string(args[0])
	rawDelta := string(args[1])
	delta, err := strconv.ParseInt(rawDelta, 10, 64)
	if err != nil {
		return protocol.MakeErrorReply("ERR value is not an integer or out of range")
	}

	bytes, errReply := db.getAsString(key)
	if errReply != nil {
		return errReply
	}
	if bytes != nil {
		val, err := strconv.ParseInt(string(bytes), 10, 64)
		if err != nil {
			return protocol.MakeErrorReply("ERR value is not an integer or out of range")
		}
		db.PutEntity(key, &database.DataEntity{
			Data: []byte(strconv.FormatInt(val-delta, 10)),
		})
		db.addAof(utils.ToCmdLineByByte("decrby", args...))
		return protocol.MakeIntReply(val - delta)
	}
	valueStr := strconv.FormatInt(-delta, 10)
	db.PutEntity(key, &database.DataEntity{
		Data: []byte(valueStr),
	})
	db.addAof(utils.ToCmdLineByByte("decrby", args...))
	return protocol.MakeIntReply(-delta)
}

func execStrLen(db *DB, args [][]byte) redis.Reply {
	key := string(args[0])
	bytes, err := db.getAsString(key)
	if err != nil {
		return err
	}
	if bytes == nil {
		return protocol.MakeIntReply(0)
	}
	return protocol.MakeIntReply(int64(len(bytes)))
}

func execAppend(db *DB, args [][]byte) redis.Reply {
	key := string(args[0])
	bytes, err := db.getAsString(key)
	if err != nil {
		return err
	}
	bytes = append(bytes, args[1]...)
	db.PutEntity(key, &database.DataEntity{
		Data: bytes,
	})
	db.addAof(utils.ToCmdLineByByte("append", args...))
	return protocol.MakeIntReply(int64(len(bytes)))
}

func execSetRange(db *DB, args [][]byte) redis.Reply {
	key := string(args[0])
	offset, errNative := strconv.ParseInt(string(args[1]), 10, 64)
	if errNative != nil {
		return protocol.MakeErrorReply(errNative.Error())
	}
	value := args[2]
	bytes, err := db.getAsString(key)
	if err != nil {
		return err
	}
	bytesLen := int64(len(bytes))
	if bytesLen < offset {
		diff := offset - bytesLen
		diffArray := make([]byte, diff)
		bytes = append(bytes, diffArray...)
		bytesLen = int64(len(bytes))
	}
	for i := 0; i < len(value); i++ {
		idx := offset + int64(i)
		if idx >= bytesLen {
			bytes = append(bytes, value[i])
		} else {
			bytes[idx] = value[i]
		}
	}
	db.PutEntity(key, &database.DataEntity{
		Data: bytes,
	})
	db.addAof(utils.ToCmdLineByByte("setRange", args...))
	return protocol.MakeIntReply(int64(len(bytes)))
}

func execGetRange(db *DB, args [][]byte) redis.Reply {
	key := string(args[0])
	startIdx, err2 := strconv.ParseInt(string(args[1]), 10, 64)
	if err2 != nil {
		return protocol.MakeErrorReply("ERR value is not an integer or out of range")
	}
	endIdx, err2 := strconv.ParseInt(string(args[2]), 10, 64)
	if err2 != nil {
		return protocol.MakeErrorReply("ERR value is not an integer or out of range")
	}

	bs, err := db.getAsString(key)
	if err != nil {
		return err
	}
	if bs == nil {
		return protocol.MakeNullBulkReply()
	}
	bytesLen := int64(len(bs))
	beg, end := utils.ConvertRange(startIdx, endIdx, bytesLen)
	if beg < 0 {
		return protocol.MakeNullBulkReply()
	}
	return protocol.MakeBulkReply(bs[beg:end])
}

func execSetBit(db *DB, args [][]byte) redis.Reply {
	key := string(args[0])
	offset, err := strconv.ParseInt(string(args[1]), 10, 64)
	if err != nil {
		return protocol.MakeErrorReply("ERR bit offset is not an integer or out of range")
	}
	valStr := string(args[2])
	var v byte
	if valStr == "1" {
		v = 1
	} else if valStr == "0" {
		v = 0
	} else {
		return protocol.MakeErrorReply("ERR bit is not an integer or out of range")
	}
	bs, errReply := db.getAsString(key)
	if errReply != nil {
		return errReply
	}
	bm := bitmap.FromBytes(bs)
	former := bm.GetBit(offset)
	bm.SetBit(offset, v)
	db.PutEntity(key, &database.DataEntity{Data: bm.ToBytes()})
	return protocol.MakeIntReply(int64(former))
}

func execGetBit(db *DB, args [][]byte) redis.Reply {
	key := string(args[0])
	offset, err := strconv.ParseInt(string(args[1]), 10, 64)
	if err != nil {
		return protocol.MakeErrorReply("ERR bit offset is not an integer or out of range")
	}
	bs, errReply := db.getAsString(key)
	if errReply != nil {
		return errReply
	}
	if bs == nil {
		return protocol.MakeIntReply(0)
	}
	bm := bitmap.FromBytes(bs)
	return protocol.MakeIntReply(int64(bm.GetBit(offset)))
}

func execBitCount(db *DB, args [][]byte) redis.Reply {
	key := string(args[0])
	bs, err := db.getAsString(key)
	if err != nil {
		return err
	}
	if bs == nil {
		return protocol.MakeIntReply(0)
	}
	byteMode := true
	if len(args) > 3 {
		mode := strings.ToLower(string(args[3]))
		if mode == "bit" {
			byteMode = false
		} else if mode == "byte" {
			byteMode = true
		} else {
			return protocol.MakeErrorReply("ERR syntax error")
		}
	}
	var size int64
	bm := bitmap.FromBytes(bs)
	if byteMode {
		size = int64(len(*bm))
	} else {
		size = int64(bm.BitSize())
	}
	var beg, end int
	if len(args) > 1 {
		var err2 error
		var startIdx, endIdx int64
		startIdx, err2 = strconv.ParseInt(string(args[1]), 10, 64)
		if err2 != nil {
			return protocol.MakeErrorReply("ERR value is not an integer or out of range")
		}
		endIdx, err2 = strconv.ParseInt(string(args[2]), 10, 64)
		if err2 != nil {
			return protocol.MakeErrorReply("ERR value is not an integer or out of range")
		}
		beg, end = utils.ConvertRange(startIdx, endIdx, size)
		if beg < 0 {
			return protocol.MakeIntReply(0)
		}
	}
	var count int64
	if byteMode {
		bm.ForEachByte(beg, end, func(offset int64, val byte) bool {
			count += int64(bits.OnesCount8(val))
			return true
		})
	} else {
		bm.ForEachBit(int64(beg), int64(end), func(offset int64, val byte) bool {
			if val > 0 {
				count++
			}
			return true
		})
	}
	return protocol.MakeIntReply(count)
}

func execBitPos(db *DB, args [][]byte) redis.Reply {
	key := string(args[0])
	bs, err := db.getAsString(key)
	if err != nil {
		return err
	}
	if bs == nil {
		return protocol.MakeIntReply(-1)
	}
	valStr := string(args[1])
	var v byte
	if valStr == "1" {
		v = 1
	} else if valStr == "0" {
		v = 0
	} else {
		return protocol.MakeErrorReply("ERR bit is not an integer or out of range")
	}
	byteMode := true
	if len(args) > 4 {
		mode := strings.ToLower(string(args[4]))
		if mode == "bit" {
			byteMode = false
		} else if mode == "byte" {
			byteMode = true
		} else {
			return protocol.MakeErrorReply("ERR syntax error")
		}
	}
	var size int64
	bm := bitmap.FromBytes(bs)
	if byteMode {
		size = int64(len(*bm))
	} else {
		size = int64(bm.BitSize())
	}
	var beg, end int
	if len(args) > 2 {
		var err2 error
		var startIdx, endIdx int64
		startIdx, err2 = strconv.ParseInt(string(args[2]), 10, 64)
		if err2 != nil {
			return protocol.MakeErrorReply("ERR value is not an integer or out of range")
		}
		endIdx, err2 = strconv.ParseInt(string(args[3]), 10, 64)
		if err2 != nil {
			return protocol.MakeErrorReply("ERR value is not an integer or out of range")
		}
		beg, end = utils.ConvertRange(startIdx, endIdx, size)
		if beg < 0 {
			return protocol.MakeIntReply(0)
		}
	}
	if byteMode {
		beg *= 8
		end *= 8
	}
	var offset = int64(-1)
	bm.ForEachBit(int64(beg), int64(end), func(o int64, val byte) bool {
		if val == v {
			offset = o
			return false
		}
		return true
	})
	return protocol.MakeIntReply(offset)
}

func init() {
	RegisterCommand("Set", execSet, writeFirstKey, rollbackFirstKey, -3, flagWrite)
	RegisterCommand("SetNx", execSetNX, writeFirstKey, rollbackFirstKey, 3, flagWrite)
	RegisterCommand("SetEX", execSetEX, writeFirstKey, rollbackFirstKey, 4, flagWrite)
	RegisterCommand("PSetEX", execPSetEX, writeFirstKey, rollbackFirstKey, 4, flagWrite)
	RegisterCommand("MSet", execMSet, prepareMSet, undoMSet, -3, flagWrite)
	RegisterCommand("MGet", execMGet, prepareMGet, nil, -2, flagReadOnly)
	RegisterCommand("MSetNX", execMSetNX, prepareMSet, undoMSet, -3, flagWrite)
	RegisterCommand("Get", execGet, readFirstKey, nil, 2, flagReadOnly)
	RegisterCommand("GetEX", execGetEX, writeFirstKey, rollbackFirstKey, -2, flagReadOnly)
	RegisterCommand("GetSet", execGetSet, writeFirstKey, rollbackFirstKey, 3, flagWrite)
	RegisterCommand("GetDel", execGetDel, writeFirstKey, rollbackFirstKey, 2, flagWrite)
	RegisterCommand("Incr", execIncr, writeFirstKey, rollbackFirstKey, 2, flagWrite)
	RegisterCommand("IncrBy", execIncrBy, writeFirstKey, rollbackFirstKey, 3, flagWrite)
	RegisterCommand("IncrByFloat", execIncrByFloat, writeFirstKey, rollbackFirstKey, 3, flagWrite)
	RegisterCommand("Decr", execDecr, writeFirstKey, rollbackFirstKey, 2, flagWrite)
	RegisterCommand("DecrBy", execDecrBy, writeFirstKey, rollbackFirstKey, 3, flagWrite)
	RegisterCommand("StrLen", execStrLen, readFirstKey, nil, 2, flagReadOnly)
	RegisterCommand("Append", execAppend, writeFirstKey, rollbackFirstKey, 3, flagWrite)
	RegisterCommand("SetRange", execSetRange, writeFirstKey, rollbackFirstKey, 4, flagWrite)
	RegisterCommand("GetRange", execGetRange, readFirstKey, nil, 4, flagReadOnly)
	RegisterCommand("SetBit", execSetBit, writeFirstKey, rollbackFirstKey, 4, flagWrite)
	RegisterCommand("GetBit", execGetBit, readFirstKey, nil, 3, flagReadOnly)
	RegisterCommand("BitCount", execBitCount, readFirstKey, nil, -2, flagReadOnly)
	RegisterCommand("BitPos", execBitPos, readFirstKey, nil, -3, flagReadOnly)

}
