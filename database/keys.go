package database

import (
	"gmr/go-cache/datastruct/dict"
	"gmr/go-cache/datastruct/list"
	"gmr/go-cache/datastruct/set"
	"gmr/go-cache/datastruct/sortedset"
	"gmr/go-cache/interface/redis"
	"gmr/go-cache/lib/utils"
	"gmr/go-cache/redis/protocol"
	"strconv"
	"strings"
	"time"
)

/**
 * @Author: wanglei
 * @File: keys
 * @Version: 1.0.0
 * @Description:
 * @Date: 2023/07/21 15:31
 */

func execDel(db *DB, args [][]byte) redis.Reply {
	keys := make([]string, len(args))
	for i, k := range args {
		keys[i] = string(k)
	}

	deleted := db.Removes(keys...)
	if deleted > 0 {
		db.addAof(utils.ToCmdLineByByte("del", args...))
	}
	return protocol.MakeIntReply(int64(deleted))
}

func undoDel(db *DB, args [][]byte) []CmdLine {
	keys := make([]string, len(args))
	for i, k := range args {
		keys[i] = string(k)
	}
	return rollbackGivenKeys(db, keys...)
}

func execExist(db *DB, args [][]byte) redis.Reply {
	result := int64(0)
	for _, k := range args {
		key := string(k)
		_, exist := db.GetEntity(key)
		if exist {
			result++
		}
	}
	return protocol.MakeIntReply(result)
}

func execFlushDB(db *DB, args [][]byte) redis.Reply {
	db.Flush()
	db.addAof(utils.ToCmdLineByByte("flushdb", args...))
	return &protocol.OkReply{}
}

func execType(db *DB, args [][]byte) redis.Reply {
	key := string(args[0])
	entity, exist := db.GetEntity(key)
	if !exist {
		return protocol.MakeStatusReply("none")
	}
	switch entity.Data.(type) {
	case []byte:
		return protocol.MakeStatusReply("string")
	case list.List:
		return protocol.MakeStatusReply("list")
	case dict.Dict:
		return protocol.MakeStatusReply("hash")
	case *set.Set:
		return protocol.MakeStatusReply("set")
	case *sortedset.SortedSet:
		return protocol.MakeStatusReply("zset")
	}
	return &protocol.UnknownErrorReply{}
}

func prepareRename(args [][]byte) ([]string, []string) {
	src := string(args[0])
	dest := string(args[1])
	return []string{dest}, []string{src}
}

func execRename(db *DB, args [][]byte) redis.Reply {
	if len(args) != 2 {
		return protocol.MakeErrorReply("ERR wrong number of arguments for 'rename' command")
	}

	src := string(args[0])
	dest := string(args[1])

	entity, ok := db.GetEntity(src)
	if !ok {
		return protocol.MakeErrorReply("no such key")
	}

	rawTTL, hasTTl := db.ttlMap.Get(src)
	db.PutEntity(dest, entity)
	if hasTTl {
		db.Persist(src)
		db.Persist(dest)
		expireTime, _ := rawTTL.(time.Time)
		db.Expire(dest, expireTime)
	}
	db.addAof(utils.ToCmdLineByByte("rename", args...))
	return &protocol.OkReply{}
}

func undoRename(db *DB, args [][]byte) []CmdLine {
	src := string(args[0])
	dest := string(args[1])
	return rollbackGivenKeys(db, src, dest)
}

func execRenameNx(db *DB, args [][]byte) redis.Reply {
	src := string(args[0])
	dest := string(args[1])

	_, ok := db.GetEntity(dest)
	if ok {
		return protocol.MakeIntReply(0)
	}

	entity, ok := db.GetEntity(src)
	if !ok {
		return protocol.MakeErrorReply("no such key")
	}

	rawTTL, hasTTl := db.ttlMap.Get(src)
	db.Removes(src, dest)
	db.PutEntity(dest, entity)
	if hasTTl {
		db.Persist(src)
		db.Persist(dest)
		expireTime, _ := rawTTL.(time.Time)
		db.Expire(dest, expireTime)
	}
	db.addAof(utils.ToCmdLineByByte("renamenx", args...))
	return protocol.MakeIntReply(1)
}

func execExpire(db *DB, args [][]byte) redis.Reply {
	key := string(args[0])

	ttlArg, err := strconv.ParseInt(string(args[1]), 10, 64)
	if err != nil {
		return protocol.MakeErrorReply("ERR value is not an integer or out of range")
	}

	ttl := time.Duration(ttlArg) * time.Second

	_, exist := db.GetEntity(key)
	if !exist {
		return protocol.MakeIntReply(0)
	}

	expireAt := time.Now().Add(ttl)
	db.Expire(key, expireAt)
	//db.addAof(aof.MakeExpireCmd(key, expireAt).Args)
	return protocol.MakeIntReply(1)
}

func execExpireAt(db *DB, args [][]byte) redis.Reply {
	key := string(args[0])

	raw, err := strconv.ParseInt(string(args[1]), 10, 64)
	if err != nil {
		return protocol.MakeErrorReply("ERR value is not an integer or out of range")
	}

	expireAt := time.Unix(raw, 0)

	_, exist := db.GetEntity(key)
	if !exist {
		return protocol.MakeIntReply(0)
	}

	db.Expire(key, expireAt)
	//db.addAof(aof.MakeExporCmd(key,expireAt).Args)
	return protocol.MakeIntReply(1)
}

func execPExpire(db *DB, args [][]byte) redis.Reply {
	key := string(args[0])

	ttlArg, err := strconv.ParseInt(string(args[1]), 10, 64)
	if err != nil {
		return protocol.MakeErrorReply("ERR value is not an integer or out of range")
	}

	ttl := time.Duration(ttlArg) * time.Millisecond

	_, exist := db.GetEntity(key)
	if !exist {
		return protocol.MakeIntReply(0)
	}

	expireAt := time.Now().Add(ttl)
	db.Expire(key, expireAt)
	//db.addAof(aof.MakeExporCmd(key,expireAt).Args)
	return protocol.MakeIntReply(1)
}

func execPExpireAt(db *DB, args [][]byte) redis.Reply {
	key := string(args[0])

	raw, err := strconv.ParseInt(string(args[1]), 10, 64)
	if err != nil {
		return protocol.MakeErrorReply("ERR value is not an integer or out of range")
	}

	expireAt := time.Unix(0, raw*int64(time.Millisecond))

	_, exist := db.GetEntity(key)
	if !exist {
		return protocol.MakeIntReply(0)
	}

	db.Expire(key, expireAt)
	//db.addAof(aof.MakeExporCmd(key,expireAt).Args)
	return protocol.MakeIntReply(1)
}

func execTTL(db *DB, args [][]byte) redis.Reply {
	key := string(args[0])
	_, exist := db.GetEntity(key)
	if !exist {
		return protocol.MakeIntReply(-2)
	}

	raw, exist := db.ttlMap.Get(key)
	if !exist {
		return protocol.MakeIntReply(-1)
	}

	expireTime, _ := raw.(time.Time)
	ttl := expireTime.Sub(time.Now())
	return protocol.MakeIntReply(int64(ttl / time.Second))
}

func execPTTL(db *DB, args [][]byte) redis.Reply {
	key := string(args[0])
	_, exist := db.GetEntity(key)
	if !exist {
		return protocol.MakeIntReply(-2)
	}

	raw, exist := db.ttlMap.Get(key)
	if !exist {
		return protocol.MakeIntReply(-1)
	}

	expireTime, _ := raw.(time.Time)
	ttl := expireTime.Sub(time.Now())
	return protocol.MakeIntReply(int64(ttl / time.Millisecond))
}

func execPersist(db *DB, args [][]byte) redis.Reply {
	key := string(args[0])
	_, exist := db.GetEntity(key)
	if !exist {
		return protocol.MakeIntReply(0)
	}

	_, exist = db.ttlMap.Get(key)
	if !exist {
		return protocol.MakeIntReply(0)
	}

	db.Persist(key)
	db.addAof(utils.ToCmdLineByByte("persist", args...))
	return protocol.MakeIntReply(1)
}

func execKeys(db *DB, args [][]byte) redis.Reply {
	pattern, err := wildcard.CompilePattern(string(args[0]))
	if err != nil {
		return protocol.MakeErrorReply("ERR illegal wildcard")
	}
	result := make([][]byte, 0)
	db.data.ForEach(func(key string, val interface{}) bool {
		if pattern.IsMatch(key) {
			result = append(result, []byte(key))
		}
		return true
	})
	return protocol.MakeMultiBulkReply(result)
}

func toTTLCmd(db *DB, key string) *protocol.MultiBulkReply {
	raw, exist := db.ttlMap.Get(key)
	if !exist {
		return protocol.MakeMultiBulkReply(utils.ToCmdLine("PERSIST", key))
	}
	expire, _ := raw.(time.Time)
	timestamp := strconv.FormatInt(expire.UnixNano()/1000/1000, 10)
	return protocol.MakeMultiBulkReply(utils.ToCmdLine("PEXPIREAT", key, timestamp))
}

func undoExpire(db *DB, args [][]byte) []CmdLine {
	key := string(args[0])
	return []CmdLine{
		toTTLCmd(db, key).Args,
	}
}

func execCopy(mdb *MultiDB, conn redis.Connection, args [][]byte) redis.Reply {
	dbIndex := conn.GetDBIndex()
	db := mdb.mustSelectDB(dbIndex)
	replaceFlag := false
	srcKey := string(args[0])
	destKey := string(args[1])

	if len(args) > 2 {
		for i := 2; i < len(args); i++ {
			arg := strings.ToLower(string(args[i]))

			if arg == "db" {
				if i+1 >= len(args) {
					return &protocol.SyntaxErrorReply{}
				}

				idx, err := strconv.Atoi(string(args[i+1]))
				if err != nil {
					return &protocol.SyntaxErrorReply{}
				}
				if idx >= len(mdb.dbSet) || db < 0 {
					return protocol.MakeErrorReply("ERR DB index is out of range")
				}
				dbIndex = idx
				i++
			} else if arg == "replace" {
				replaceFlag = true
			} else {
				return &protocol.SyntaxErrorReply{}
			}
		}
	}

	if srcKey == destKey && dbIndex == conn.GetDBIndex() {
		return protocol.MakeErrorReply("ERR source and destination objects are the same")
	}

	src, exist := db.GetEntity(srcKey)
	if !exist {
		return protocol.MakeIntReply(0)
	}

	destDB := mdb.mustSelectDB(dbIndex)
	if _, exist = destDB.GetEntity(destKey); exist != false {
		if replaceFlag == false {
			return protocol.MakeIntReply(0)
		}
	}

	destDB.PutEntity(destKey, src)
	raw, exist := db.ttlMap.Get(srcKey)
	if exist {
		expire := raw.(time.Time)
		destDB.Expire(destKey, expire)
	}
	mdb.aofHandler.AddAof(conn.GetDBIndex(), utils.ToCmdLineByByte("copy", args...))
	return protocol.MakeIntReply(1)
}

func init() {
	RegisterCommand("Del", execDel, writeAllKeys, undoDel, -2, flagWrite)
	RegisterCommand("Expire", execExpire, writeFirstKey, undoExpire, 3, flagWrite)
	RegisterCommand("ExpireAt", execExpireAt, writeFirstKey, undoExpire, 3, flagWrite)
	RegisterCommand("PExpire", execPExpire, writeFirstKey, undoExpire, 3, flagWrite)
	RegisterCommand("PExpireAt", execPExpireAt, writeFirstKey, undoExpire, 3, flagWrite)
	RegisterCommand("TTL", execTTL, readFirstKey, nil, 2, flagReadOnly)
	RegisterCommand("PTTL", execPTTL, readFirstKey, nil, 2, flagReadOnly)
	RegisterCommand("Persist", execPersist, writeFirstKey, undoExpire, 2, flagWrite)
	RegisterCommand("Exists", execExist, readAllKeys, nil, -2, flagReadOnly)
	RegisterCommand("Type", execType, readFirstKey, nil, 2, flagReadOnly)
	RegisterCommand("Rename", execRename, prepareRename, undoRename, 3, flagReadOnly)
	RegisterCommand("RenameNx", execRenameNx, prepareRename, undoRename, 3, flagReadOnly)
	RegisterCommand("Keys", execKeys, noPrepare, nil, 2, flagReadOnly)
}
