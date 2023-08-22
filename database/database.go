package database

import (
	"fmt"
	"gmr/go-cache/aof"
	"gmr/go-cache/config"
	"gmr/go-cache/interface/database"
	"gmr/go-cache/interface/redis"
	"gmr/go-cache/lib/logger"
	"gmr/go-cache/lib/utils"
	"gmr/go-cache/pubsub"
	"gmr/go-cache/redis/connection"
	"gmr/go-cache/redis/protocol"
	"runtime/debug"
	"strconv"
	"strings"
	"sync/atomic"
	"time"
)

/**
 * @Author: wanglei
 * @File: database
 * @Version: 1.0.0
 * @Description:
 * @Date: 2023/07/10 20:07
 */

type MultiDB struct {
	dbSet []*atomic.Value
	// hanle pub/sub
	hub *pubsub.Hub
	// handleaof持久化
	aofHandler *aof.Handler

	// 存储master节点地址
	slaveOf     string
	role        int32
	replication *replicationStatus
}

func NewStandaloneServer() *MultiDB {
	mdb := &MultiDB{}

	if config.Properties.Databases == 0 {
		config.Properties.Databases = 16
	}

	mdb.dbSet = make([]*atomic.Value, config.Properties.Databases)
	for i := range mdb.dbSet {
		singleDB := makeDB()
		singleDB.index = i
		holder := &atomic.Value{}
		holder.Store(singleDB)
		mdb.dbSet[i] = holder
	}

	mdb.hub = pubsub.MakeHub()
	validAof := false
	if config.Properties.AppendOnly {
		aofHandler, err := aof.NewAOFHandler(mdb, func() database.EmbedDB {
			return MakeBasicMultiDB()
		})
		if err != nil {
			panic(err)
		}

		mdb.aofHandler = aofHandler

		for _, db := range mdb.dbSet {
			singleDB := db.Load().(*DB)
			singleDB.addAof = func(line CmdLine) {
				mdb.adoHandler.AddAof(singleDB.index, line)
			}
		}
		validAof = true
	}

	if config.Properties.RDBFilename != "" && !validAof {
		loadRdbFile(mdb)
	}

	mdb.replication = initReplStatus()
	mdb.startReplCron()
	mdb.role = masterRole
	return mdb
}

func MakeBasicMultiDB() *MultiDB {
	mdb := &MultiDB{}
	mdb.dbSet = make([]*atomic.Value, config.Properties.Databases)

	for i := range mdb.dbSet {
		holder := &atomic.Value{}
		holder.Store(makeBasicDB())
		mdb.dbSet[i] = holder
	}
	return mdb
}

func (mdb *MultiDB) Exec(c redis.Connection, cmdLine [][]byte) (result redis.Reply) {
	defer func() {
		if err := recover(); err != nil {
			logger.Error(fmt.Sprintf("error occurs: %v\n%s", err, string(debug.Stack())))
			result = &protocol.UnknownErrorReply{}
		}
	}()

	cmdName := strings.ToLower(string(cmdLine[0]))
	if cmdName == "auth" {
		return Auth(c, cmdLine[1:])
	}
	if !isAuthenticated(c) {
		return protocol.MakeErrorReply("NOAUTH Authentication required")
	}
	if cmdName == "slaveof" {
		if c != nil && c.InMultiState() {
			return protocol.MakeErrorReply("cannot use slave of database within multi")
		}
		if len(cmdLine) != 3 {
			return protocol.MakeArgNumErrorReply("SLAVEOF")
		}
		return mdb.execSlaveOf(c, cmdLine[1:])
	}

	role := atomic.LoadInt32(&mdb.role)
	if role == slaveRole &&
		c.GetRole() != connection.ReplicationRecvCli {
		if !isReadOnlyCommand(cmdName) {
			return protocol.MakeErrorReply("READONLY You can't write against a read only slave.")
		}
	}

	if cmdName == "subscribe" {
		if len(cmdLine) < 2 {
			return protocol.MakeArgNumErrorReply("subscribe")
		}
		return pubsub.Subscribe(mdb.hub, c, cmdLine[1:])
	} else if cmdName == "publish" {
		return pubsub.Publish(mdb.hub, cmdLine[1:])
	} else if cmdName == "unsubscribe" {
		return pubsub.Unsubscribe(mdb.hub, c, cmdLine[1:])
	} else if cmdName == "bgrewriteaof" {
		return BGRewriteAOF(mdb, cmdLine[1:])
	} else if cmdName == "rewriteaof" {
		return RewriteAOF(mdb, cmdLine[1:])
	} else if cmdName == "flushall" {
		return mdb.flushAll()
	} else if cmdName == "flushdb" {
		if !validateArity(1, cmdLine) {
			return protocol.MakeArgNumErrorReply(cmdName)
		}
		if c.InMultiState() {
			return protocol.MakeErrorReply("ERR command 'FlushDB' cannot be used in MULTI")
		}
		return mdb.flushDB(c.GetDBIndex())
	} else if cmdName == "save" {
		return SaveRDB(mdb, cmdLine[1:])
	} else if cmdName == "bgsave" {
		return BGSaveRDB(mdb, cmdLine[1:])
	} else if cmdName == "select" {
		if c != nil && c.InMultiState() {
			return protocol.MakeErrorReply("cannot select database within multi")
		}
		if len(cmdLine) != 2 {
			return protocol.MakeArgNumErrorReply("select")
		}
		return execSelect(c, mdb, cmdLine[1:])
	} else if cmdName == "copy" {
		if len(cmdLine) < 3 {
			return protocol.MakeArgNumErrorReply("copy")
		}
		return execCopy(mdb, c, cmdLine[1:])
	}
	// todo: support multi database transaction

	// normal commands
	dbIndex := c.GetDBIndex()
	selectedDB, errReply := mdb.selectDB(dbIndex)
	if errReply != nil {
		return errReply
	}
	return selectedDB.Exec(c, cmdLine)
}

func (mdb *MultiDB) AfterClientClose(c redis.Connection) {
	pubsub.UnsubscribeAll(mdb.hub, c)
}

func (mdb *MultiDB) Close() {
	mdb.replication.close()
	if mdb.aofHandler != nil {
		mdb.aofHandler.Close()
	}
}

func execSelect(c redis.Connection, mdb *MultiDB, args [][]byte) redis.Reply {
	dbIndex, err := strconv.Atoi(string(args[0]))
	if err != nil {
		return protocol.MakeErrorReply("ERR invalid DB index")
	}

	if dbIndex >= len(mdb.dbSet) || dbIndex < 0 {
		return protocol.MakeErrorReply("ERR DB index is out of range")
	}
	c.SelectDB(dbIndex)
	return protocol.MakeOkReply()
}

func (mdb *MultiDB) flushDB(dbIndex int) redis.Reply {
	if dbIndex >= len(mdb.dbSet) || dbIndex < 0 {
		return protocol.MakeErrorReply("ERR DB index is out of range")
	}
	newDB := makeDB()
	mdb.loadDB(dbIndex, newDB)
	return &protocol.OkReply{}
}

func (mdb *MultiDB) loadDB(dbIndex int, newDB *DB) redis.Reply {
	if dbIndex >= len(mdb.dbSet) || dbIndex < 0 {
		return protocol.MakeErrorReply("ERR DB index is out of range")
	}
	oldDB := mdb.mustSelectDB(dbIndex)
	newDB.index = dbIndex
	newDB.addAof = oldDB.addAof
	mdb.dbSet[dbIndex].Store(newDB)
	return &protocol.OkReply{}
}

func (mdb *MultiDB) flushAll() redis.Reply {
	for i := range mdb.dbSet {
		mdb.flushDB(i)
	}

	if mdb.aofHandler != nil {
		mdb.aofHandler.AddAof(0, utils.ToCmdLine("FlushAll"))
	}
	return &protocol.OkReply{}
}

func (mdb *MultiDB) selectDB(dbIndex int) (*DB, *protocol.StandardErrorReply) {
	if dbIndex >= len(mdb.dbSet) || dbIndex < 0 {
		return nil, protocol.MakeErrorReply("ERR DB index is out of range")
	}
	return mdb.dbSet[dbIndex].Load().(*DB), nil
}

func (mdb *MultiDB) mustSelectDB(dbIndex int) *DB {
	selectDB, err := mdb.selectDB(dbIndex)
	if err != nil {
		panic(err)
	}
	return selectDB
}

func (mdb *MultiDB) ForEach(dbIndex int, cb func(key string, data *database.DataEntity, expiration *time.Time) bool) {
	mdb.mustSelectDB(dbIndex).ForEach(cb)
}

func (mdb *MultiDB) ExecMulti(conn redis.Connection, watching map[string]uint32, cmdLines []CmdLine) redis.Reply {
	selectDB, errReply := mdb.selectDB(conn.GetDBIndex())
	if errReply != nil {
		return errReply
	}
	return selectDB.ExecMulti(conn, watching, cmdLines)
}

func (mdb *MultiDB) RWLocks(dbIndex int, writeKeys []string, readKeys []string) {
	mdb.mustSelectDB(dbIndex).RWLocks(writeKeys, readKeys)
}

func (mdb *MultiDB) RWUnLocks(dbIndex int, writeKeys []string, readKeys []string) {
	mdb.mustSelectDB(dbIndex).RWUnLocks(writeKeys, readKeys)
}

func (mdb *MultiDB) GetUndoLogs(dbIndex int, cmdLine [][]byte) []CmdLine {
	return mdb.mustSelectDB(dbIndex).GetUndoLogs(cmdLine)
}

func (mdb *MultiDB) ExecWithLock(conn redis.Connection, cmdLine [][]byte) redis.Reply {
	db, errReply := mdb.selectDB(conn.GetDBIndex())
	if errReply != nil {
		return errReply
	}
	return db.execWithLock(cmdLine)
}

func BGRewriteAOF(db *MultiDB, args [][]byte) redis.Reply {
	go db.aofHandler.Rewrite()
	return protocol.MakeStatusReply("Background append only file rewriting started")
}

func Rewrite(db *MultiDB, args [][]byte) redis.Reply {
	err := db.aofHandler.Rewrite()
	if err != nil {
		return protocol.MakeErrorReply(err.Error())
	}
	return protocol.MakeOkReply()
}

func SaveRDB(db *MultiDB, args [][]byte) redis.Reply {
	if db.aofHandler == nil {
		return protocol.MakeErrorReply("please enable aof before using save")
	}
	err := db.aofHandler.Rewirte2RDB()
	if err != nil {
		return protocol.MakeErrorReply(err.Error())
	}
	return protocol.MakeOkReply()
}

func BGSaveRDB(db *MultiDB, args [][]byte) redis.Reply {
	if db.aofHandler == nil {
		return protocol.MakeErrorReply("please enable aof before using save")
	}

	go func() {
		defer func() {
			if err := recover(); err != nil {
				logger.Error(err)
			}
		}()
		err := db.aofHandler.Rewrite2RDB()
		if err != nil {
			logger.Error(err)
		}
	}()
	return protocol.MakeStatusReply("Background saving started")
}

func (mdb *MultiDB) GetDBSize(dbIndex int) (int, int) {
	db := mdb.mustSelectDB(dbIndex)
	return db.data.Len(), db.ttlMap.Len()
}
