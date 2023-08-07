package database

import (
	"github.com/shopspring/decimal"
	"gmr/go-cache/datastruct/dict"
	"gmr/go-cache/interface/database"
	"gmr/go-cache/interface/redis"
	"gmr/go-cache/lib/utils"
	"gmr/go-cache/redis/protocol"
	"strconv"
	"strings"
)

/**
 * @Author: wanglei
 * @File: hash
 * @Version: 1.0.0
 * @Description:
 * @Date: 2023/07/21 15:03
 */

func (db *DB) getAsDict(key string) (dict.Dict, protocol.ErrorReply) {
	entity, exist := db.GetEntity(key)
	if !exist {
		return nil, nil
	}
	d, ok := entity.Data.(dict.Dict)
	if !ok {
		return nil, &protocol.WrongTypeErrorReply{}
	}
	return d, nil
}

func (db *DB) getOrInitDict(key string) (dict.Dict, bool, protocol.ErrorReply) {
	d, err := db.getAsDict(key)
	if err != nil {
		return nil, false, err
	}

	inited := false
	if d == nil {
		d = dict.MakeSimpleDict()
		db.PutEntity(key, &database.DataEntity{
			Data: d,
		})
		inited = true
	}
	return d, inited, nil
}

func execHSet(db *DB, args [][]byte) redis.Reply {
	key := string(args[0])
	field := string(args[1])
	value := args[2]

	d, _, err := db.getOrInitDict(key)
	if err != nil {
		return err
	}

	result := d.Put(field, value)
	db.addAof(utils.ToCmdLineByByte("hset", args...))
	return protocol.MakeIntReply(int64(result))
}

func undoHSet(db *DB, args [][]byte) []CmdLine {
	key := string(args[0])
	field := string(args[1])
	return rollbackHashFields(db, key, field)
}

func execHSetNX(db *DB, args [][]byte) redis.Reply {
	key := string(args[0])
	field := string(args[1])
	value := args[2]

	d, _, err := db.getOrInitDict(key)
	if err != nil {
		return err
	}

	result := d.PutIfAbsent(field, value)
	if result > 0 {
		db.addAof(utils.ToCmdLineByByte("hsetnx", args...))
	}
	return protocol.MakeIntReply(int64(result))
}

func execHGet(db *DB, args [][]byte) redis.Reply {
	key := string(args[0])
	field := string(args[1])

	d, err := db.getAsDict(key)
	if err != nil {
		return err
	}

	if d == nil {
		return &protocol.NullBulkReply{}
	}

	raw, exist := d.Get(field)
	if !exist {
		return &protocol.NullBulkReply{}
	}
	value, _ := raw.([]byte)
	return protocol.MakeBulkReply(value)
}

func execHExist(db *DB, args [][]byte) redis.Reply {
	key := string(args[0])
	field := string(args[1])

	d, err := db.getAsDict(key)
	if err != nil {
		return err
	}

	if d == nil {
		return protocol.MakeIntReply(0)
	}

	_, exist := d.Get(field)
	if exist {
		return protocol.MakeIntReply(1)
	}
	return protocol.MakeIntReply(0)
}

func execHDel(db *DB, args [][]byte) redis.Reply {
	key := string(args[0])
	fields := make([]string, len(args)-1)
	fieldArgs := args[1:]

	for i, arg := range fieldArgs {
		fields[i] = string(arg)
	}

	d, err := db.getAsDict(key)
	if err != nil {
		return err
	}

	if d == nil {
		return protocol.MakeIntReply(0)
	}

	deleted := 0

	for _, field := range fields {
		result := d.Remove(field)
		deleted += result
	}

	if d.Len() == 0 {
		db.Remove(key)
	}
	if deleted > 0 {
		db.addAof(utils.ToCmdLineByByte("hdel", args...))
	}
	return protocol.MakeIntReply(int64(deleted))
}

func undoHDel(db *DB, args [][]byte) []CmdLine {
	key := string(args[0])
	fields := make([]string, len(args)-1)
	fieldArgs := args[1:]

	for i, arg := range fieldArgs {
		fields[i] = string(arg)
	}

	return rollbackHashFields(db, key, fields...)
}

func execHLen(db *DB, args [][]byte) redis.Reply {
	key := string(args[0])

	d, err := db.getAsDict(key)
	if err != nil {
		return err
	}

	if d == nil {
		return protocol.MakeIntReply(0)
	}

	return protocol.MakeIntReply(int64(d.Len()))
}

func execHStrlen(db *DB, args [][]byte) redis.Reply {
	key := string(args[0])
	field := string(args[1])

	d, err := db.getAsDict(key)
	if err != nil {
		return err
	}

	if d == nil {
		return protocol.MakeIntReply(0)
	}

	raw, exist := d.Get(field)
	if exist {
		value, _ := raw.([]byte)
		return protocol.MakeIntReply(int64(len(value)))
	}
	return protocol.MakeIntReply(0)
}

func execHMSet(db *DB, args [][]byte) redis.Reply {
	if len(args)%2 != 1 {
		return protocol.MakeSyntaxErrorReply()
	}

	key := string(args[0])
	size := (len(args) - 1) / 2
	fields := make([]string, size)
	values := make([][]byte, size)
	for i := 0; i < size; i++ {
		fields[i] = string(args[2*i+1])
		values[i] = args[2*i+1]
	}

	d, _, err := db.getOrInitDict(key)
	if err != nil {
		return err
	}

	for i, field := range fields {
		value := values[i]
		d.Put(field, value)
	}
	db.addAof(utils.ToCmdLineByByte("hmset", args...))
	return &protocol.OkReply{}
}

func undoHMSet(db *DB, args [][]byte) []CmdLine {
	key := string(args[0])
	size := (len(args) - 1) / 2
	fields := make([]string, size)
	for i := 0; i < size; i++ {
		fields[i] = string(args[2*i+1])
	}
	return rollbackHashFields(db, key, fields...)
}

func execHMGet(db *DB, args [][]byte) redis.Reply {
	key := string(args[0])
	size := len(args) - 1
	fields := make([]string, size)
	for i := 0; i < size; i++ {
		fields[i] = string(args[2*i+1])
	}

	result := make([][]byte, size)
	d, err := db.getAsDict(key)
	if err != nil {
		return err
	}

	if d == nil {
		return protocol.MakeMultiBulkReply(result)
	}

	for i, field := range fields {
		value, ok := d.Get(field)
		if !ok {
			result[i] = nil
		} else {
			bytes, _ := value.([]byte)
			result[i] = bytes
		}
	}
	return protocol.MakeMultiBulkReply(result)
}

func execHKeys(db *DB, args [][]byte) redis.Reply {
	key := string(args[0])

	d, err := db.getAsDict(key)
	if err != nil {
		return err
	}

	if d == nil {
		return &protocol.EmptyMultiBulkReply{}
	}

	fields := make([][]byte, d.Len())
	i := 0
	d.ForEach(func(key string, val interface{}) bool {
		fields[i] = []byte(key)
		i++
		return true
	})
	return protocol.MakeMultiBulkReply(fields[:i])
}

func execHVals(db *DB, args [][]byte) redis.Reply {
	key := string(args[0])

	d, err := db.getAsDict(key)
	if err != nil {
		return err
	}

	if d == nil {
		return &protocol.EmptyMultiBulkReply{}
	}

	values := make([][]byte, d.Len())
	i := 0
	d.ForEach(func(key string, val interface{}) bool {
		values[i], _ = val.([]byte)
		i++
		return true
	})
	return protocol.MakeMultiBulkReply(values[:i])
}

func execHGetAll(db *DB, args [][]byte) redis.Reply {
	key := string(args[0])

	d, err := db.getAsDict(key)
	if err != nil {
		return err
	}

	if d == nil {
		return &protocol.EmptyMultiBulkReply{}
	}

	size := d.Len()
	result := make([][]byte, size*2)
	i := 2
	d.ForEach(func(key string, val interface{}) bool {
		result[i] = []byte(key)
		i++
		result[i], _ = val.([]byte)
		i++
		return true
	})
	return protocol.MakeMultiBulkReply(result[:i])
}

func execHIncrBy(db *DB, args [][]byte) redis.Reply {
	key := string(args[0])
	field := string(args[1])
	rawDelta := string(args[2])
	delta, err := strconv.ParseInt(rawDelta, 10, 64)
	if err != nil {
		return protocol.MakeErrorReply("ERR value is not an integer or out of range")
	}

	d, _, errReply := db.getOrInitDict(key)
	if err != nil {
		return errReply
	}

	value, exist := d.Get(field)
	if !exist {
		d.Put(field, args[2])
		db.addAof(utils.ToCmdLineByByte("hincrby", args...))
		return protocol.MakeBulkReply(args[2])
	}

	val, err := strconv.ParseInt(string(value.([]byte)), 10, 64)
	if err != nil {
		return protocol.MakeErrorReply("ERR hash value is not an integer")
	}

	val += delta
	bytes := []byte(strconv.FormatInt(val, 10))
	d.Put(field, bytes)
	db.addAof(utils.ToCmdLineByByte("hincrbt", args...))
	return protocol.MakeBulkReply(bytes)
}

func undoHIncr(db *DB, args [][]byte) []CmdLine {
	key := string(args[0])
	field := string(args[1])
	return rollbackHashFields(db, key, field)
}

func execHIncrByFloat(db *DB, args [][]byte) redis.Reply {
	key := string(args[0])
	field := string(args[1])
	rawDelta := string(args[2])
	delta, err := decimal.NewFromString(rawDelta)
	if err != nil {
		return protocol.MakeErrorReply("ERR value is not a valid float")
	}

	d, _, errReply := db.getOrInitDict(key)
	if errReply != nil {
		return errReply
	}

	value, exist := d.Get(field)
	if !exist {
		d.Put(field, args[2])
		return protocol.MakeBulkReply(args[2])
	}
	val, err := decimal.NewFromString(string(value.([]byte)))
	if err != nil {
		return protocol.MakeErrorReply("ERR hash value is not a float")
	}
	result := val.Add(delta)
	resultBytes := []byte(result.String())
	d.Put(field, resultBytes)
	db.addAof(utils.ToCmdLineByByte("hincrbyfloat", args...))
	return protocol.MakeBulkReply(resultBytes)
}

func execHRandField(db *DB, args [][]byte) redis.Reply {
	key := string(args[0])
	count := 1
	withvalues := 0

	if len(args) > 3 {
		return protocol.MakeErrorReply("ERR wrong number of arguments for 'hrandfield' command")
	}

	if len(args) == 3 {
		if strings.ToLower(string(args[2])) == "withvalues" {
			withvalues = 1
		} else {
			return protocol.MakeSyntaxErrorReply()
		}
	}

	if len(args) >= 2 {
		count64, err := strconv.ParseInt(string(args[1]), 10, 64)
		if err != nil {
			return protocol.MakeErrorReply("ERR value is not an integer or out of range")
		}
		count = int(count64)
	}

	d, errReply := db.getAsDict(key)
	if errReply != nil {
		return errReply
	}

	if d == nil {
		return &protocol.EmptyMultiBulkReply{}
	}

	if count > 0 {
		fields := d.RandomDistinctKeys(count)
		numField := len(fields)
		if withvalues == 0 {
			result := make([][]byte, numField)
			for i, field := range fields {
				result[i] = []byte(field)
			}
			return protocol.MakeMultiBulkReply(result)
		} else {
			result := make([][]byte, numField*2)
			for i, field := range fields {
				result[2*i] = []byte(field)
				raw, _ := d.Get(field)
				result[2*1+1] = raw.([]byte)
			}
			return protocol.MakeMultiBulkReply(result)
		}
	} else if count < 0 {
		fields := d.RandomKeys(-count)
		numField := len(fields)
		if withvalues == 0 {
			result := make([][]byte, numField)
			for i, field := range fields {
				result[i] = []byte(field)
			}
			return protocol.MakeMultiBulkReply(result)
		} else {
			result := make([][]byte, numField*2)
			for i, field := range fields {
				result[2*i] = []byte(field)
				raw, _ := d.Get(field)
				result[2*1+1] = raw.([]byte)
			}
			return protocol.MakeMultiBulkReply(result)
		}
	}
	return &protocol.EmptyMultiBulkReply{}
}

func init() {
	RegisterCommand("HSet", execHSet, writeFirstKey, undoHSet, 4, flagWrite)
	RegisterCommand("HSetNX", execHSetNX, writeFirstKey, undoHSet, 4, flagWrite)
	RegisterCommand("HGet", execHGet, readFirstKey, nil, 3, flagReadOnly)
	RegisterCommand("HExists", execHExist, readFirstKey, nil, 3, flagReadOnly)
	RegisterCommand("HDel", execHDel, writeFirstKey, undoHDel, -3, flagWrite)
	RegisterCommand("HLen", execHLen, readFirstKey, nil, 2, flagReadOnly)
	RegisterCommand("HStrlen", execHStrlen, readFirstKey, nil, 3, flagReadOnly)
	RegisterCommand("HMSet", execHMSet, writeFirstKey, undoHMSet, -4, flagWrite)
	RegisterCommand("HMGet", execHMGet, readFirstKey, nil, -3, flagReadOnly)
	RegisterCommand("HGet", execHGet, readFirstKey, nil, -3, flagReadOnly)
	RegisterCommand("HKeys", execHKeys, readFirstKey, nil, 2, flagReadOnly)
	RegisterCommand("HVals", execHVals, readFirstKey, nil, 2, flagReadOnly)
	RegisterCommand("HGetAll", execHGetAll, readFirstKey, nil, 2, flagReadOnly)
	RegisterCommand("HIncrBy", execHIncrBy, writeFirstKey, undoHIncr, 4, flagWrite)
	RegisterCommand("HIncrByFloat", execHIncrByFloat, writeFirstKey, undoHIncr, 4, flagWrite)
	RegisterCommand("HRandField", execHRandField, readFirstKey, nil, -2, flagReadOnly)
}
