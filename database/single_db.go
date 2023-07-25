package database

import (
	"gmr/go-cache/datastruct/dict"
	"gmr/go-cache/datastruct/lockmap"
	"gmr/go-cache/interface/database"
	"gmr/go-cache/interface/redis"
	"gmr/go-cache/redis/protocol"
	"strings"
	"time"
)

/**
 * @Author: wanglei
 * @File: single_db
 * @Version: 1.0.0
 * @Description:
 * @Date: 2023/07/20 16:00
 */

const (
	dataDictSize = 1 << 16
	ttlDictSize  = 1 << 10
	lockerSize   = 1024
)

// CmdLine 命令行参数
type CmdLine = [][]byte

// ExecFunc 命令行执行器接口
type ExecFunc func(db *DB, args [][]byte) redis.Reply

// PreFunc 当命令中有multi时，PreFunc会分析命令，返回相关的write keys和read keys
type PreFunc func(args [][]byte) ([]string, []string)

// UndoFunc 返回undo log，undo时从头到尾执行
type UndoFunc func(db *DB, args [][]byte) []CmdLine

// DB 单个DB实例
type DB struct {
	index int
	// key:DataEntity
	data dict.Dict
	// key:expireTime
	ttlMap dict.Dict
	// key:version
	versionMap dict.Dict

	// mutex执行复杂命令
	locker *lockmap.Locks
	addAof func(CmdLine)
}

// 返回DB实例
func makeDB() *DB {
	return &DB{
		data:       dict.MakeConcurrentDict(dataDictSize),
		ttlMap:     dict.MakeConcurrentDict(ttlDictSize),
		versionMap: dict.MakeConcurrentDict(dataDictSize),
		locker:     lockmap.MakeLocks(lockerSize),
		addAof:     func(line CmdLine) {},
	}
}

// 返回基本DB实例，只有基础功能，不是并发安全的
func makeBasicDB() *DB {
	return &DB{
		data:       dict.MakeSimpleDict(),
		ttlMap:     dict.MakeSimpleDict(),
		versionMap: dict.MakeSimpleDict(),
		locker:     lockmap.MakeLocks(1),
		addAof:     func(line CmdLine) {},
	}
}

func (db *DB) Exec(conn redis.Connection, cmdLine [][]byte) redis.Reply {
	cmdName := strings.ToLower(string(cmdLine[0]))

	switch cmdName {
	case "multi":
		if len(cmdName) != 1 {
			return protocol.MakeArgNumErrorReply(cmdName)
		}
		return StartMulti(conn)
	case "discard":
		if len(cmdName) != 1 {
			return protocol.MakeArgNumErrorReply(cmdName)
		}
		return DiscardMulti(conn)
	case "exec":
		if len(cmdName) != 1 {
			return protocol.MakeArgNumErrorReply(cmdName)
		}
		return ExecMulti(db, conn)
	case "watch":
		if !validateArity(-2, cmdLine) {
			return protocol.MakeArgNumErrorReply(cmdName)
		}
		return Watch(db, conn, cmdLine[1:])
	}

	if conn != nil && conn.InMultiState() {
		EnqueueCmd(conn, cmdLine)
		return protocol.MakeQueuedReply()
	}

	return db.execNormalCommand(cmdLine)
}

func (db *DB) execNormalCommand(cmdLine [][]byte) redis.Reply {
	cmdName := strings.ToLower(string(cmdLine[0]))
	cmd, ok := cmdTable[cmdName]

	if !ok {
		return protocol.MakeErrorReply("ERR unknown command '" + cmdName + "'")
	}

	if !validateArity(cmd.arity, cmdLine) {
		return protocol.MakeArgNumErrorReply(cmdName)
	}

	prepare := cmd.prepare
	write, read := prepare(cmdLine[1:])
	db.addVersion(write...)
	db.RWLocks(write, read)
	defer db.RWUnLocks(write, read)
	function := cmd.executor
	return function(db, cmdLine[1:])
}

func (db *DB) execWithLock(cmdLine [][]byte) redis.Reply {
	cmdName := strings.ToLower(string(cmdLine[0]))
	cmd, ok := cmdTable[cmdName]
	if !ok {
		return protocol.MakeErrorReply("ERR unknown command '" + cmdName + "'")
	}
	if !validateArity(cmd.arity, cmdLine) {
		return protocol.MakeArgNumErrorReply(cmdName)
	}
	fun := cmd.executor
	return fun(db, cmdLine[1:])
}

func validateArity(arity int, cmdArgs [][]byte) bool {
	argNum := len(cmdArgs)

	if arity >= 0 {
		return argNum == arity
	}
	return argNum >= -arity
}

func (db *DB) GetEntity(key string) (*database.DataEntity, bool) {
	raw, ok := db.data.Get(key)
	if !ok {
		return nil, false
	}
	if db.IsExpired(key) {
		return nil, false
	}

	entity, _ := raw.(*database.DataEntity)
	return entity, true
}

func (db *DB) PutEntity(key string, value *database.DataEntity) int {
	return db.data.Put(key, value)
}

func (db *DB) PutIfExist(key string, value *database.DataEntity) int {
	return db.data.PutIfExist(key, value)
}

func (db *DB) PutIfAbsent(key string, value *database.DataEntity) int {
	return db.data.PutIfAbsent(key, value)
}

func (db *DB) Remove(key string) {
	db.data.Remove(key)
	db.ttlMap.Remove(key)
	expiredTask := genExpiredTask(key)
	timewheel.Cancel(expiredTask)
}

func (db *DB) Removes(keys ...string) int {
	var deleted = 0
	for _, key := range keys {
		if _, exist := db.data.Get(key); exist {
			db.Remove(key)
			deleted++
		}
	}
	return deleted
}

func (db *DB) FlushAll() {
	db.data.Clear()
	db.ttlMap.Clear()
	db.locker = lockmap.MakeLocks(lockerSize)
}

func (db *DB) RWLocks(writeKeys, readKeys []string) {
	db.locker.RWLock(writeKeys, readKeys)
}

func (db *DB) RWUnLocks(writeKeys, readKeys []string) {
	db.locker.RWUnLocks(writeKeys, readKeys)
}

func genExpiredTask(key string) string {
	return "expired" + key
}

// todo: implement
func (db *DB) Expire(key string, expire time.Time) {

}

// todo: implement
func (db *DB) Persist(key string) {

}

func (db *DB) IsExpired(key string) bool {
	rawExpired, ok := db.ttlMap.Get(key)
	if !ok {
		return false
	}
	expireTime := rawExpired.(time.Time)
	expire := time.Now().After(expireTime)
	if expire {
		db.Remove(key)
	}
	return expire
}

func (db *DB) GetVersion(key string) uint32 {
	entity, ok := db.versionMap.Get(key)
	if !ok {
		return 0
	}
	return entity.(uint32)
}

func (db *DB) addVersion(keys ...string) {
	for _, key := range keys {
		version := db.GetVersion(key)
		db.versionMap.Put(key, version+1)
	}
}

func (db *DB) ForEach(cb func(key string, data *database.DataEntity, expiration *time.Time) bool) {
	db.data.ForEach(func(key string, val interface{}) bool {
		entity, _ := val.(*database.DataEntity)
		var expiration time.Time
		rawExpired, ok := db.ttlMap.Get(key)
		if ok {
			expiration, _ = rawExpired.(time.Time)
		}
		return cb(key, entity, &expiration)
	})
}
