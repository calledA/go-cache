package database

import (
	"gmr/go-cache/interface/redis"
	"time"
)

/**
 * @Author: wanglei
 * @File: db.go
 * @Version: 1.0.0
 * @Description: db方法接口
 * @Date: 2023/07/10 11:39
 */

// CmdLine 命令行命令
type CmdLine = [][]byte

// DB redis风格的存储引擎
type DB interface {
	Exec(client redis.Connection, cmdLine [][]byte) redis.Reply
	AfterClientClose(client redis.Connection)
	Close()
}

// EmbedDB 在db接口上新增方法，适用于更复杂的application
type EmbedDB interface {
	DB
	ExecWithLock(client redis.Connection, cmdLine [][]byte) redis.Reply
	ExecMulti(client redis.Connection, watching map[string]uint32, cmdLine []CmdLine) redis.Reply
	GetUndoLogs(dbIndex int, cmdLine [][]byte) []CmdLine
	ForEach(dbIndex int, cb func(key string, data *DataEntity, expiration *time.Time) bool)
	RWLocks(dbIndex int, writeKeys []string, readKeys []string)
	RWUnLocks(dbIndex int, writeKeys []string, readKeys []string)
	GetDBSize(dbIndex int) (int, int)
}

// DataEntity 为不同的key存储值(list、hash、set等)
type DataEntity struct {
	Data interface{}
}
