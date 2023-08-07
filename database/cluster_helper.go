package database

import (
	"gmr/go-cache/aof"
	"gmr/go-cache/interface/redis"
	"gmr/go-cache/redis/parser"
	"gmr/go-cache/redis/protocol"
)

/**
 * @Author: wanglei
 * @File: cluster_helper
 * @Version: 1.0.0
 * @Description:
 * @Date: 2023/08/07 16:21
 */
func execExistIn(db *DB, args [][]byte) redis.Reply {
	var result [][]byte
	for _, arg := range args {
		key := string(arg)
		_, exist := db.GetEntity(key)
		if exist {
			result = append(result, []byte(key))
		}
	}

	if len(result) == 0 {
		return protocol.MakeEmptyMultiBulkReply()
	}
	return protocol.MakeMultiBulkReply(result)
}

func execDumpKey(db *DB, args [][]byte) redis.Reply {
	key := string(args[0])
	entity, ok := db.GetEntity(key)
	if !ok {
		return protocol.MakeEmptyMultiBulkReply()
	}

	dumpCmd := aof.EntityToCmd(key, entity)
	ttlCmd := toTTLCmd(db, key)
	resp := protocol.MakeMultiBulkReply([][]byte{
		dumpCmd.ToBytes(),
		ttlCmd.ToBytes(),
	})
	return resp
}

func execRenameFrom(db *DB, args [][]byte) redis.Reply {
	key := string(args[0])
	db.Remove(key)
	return protocol.MakeOkReply()
}

func execRenameTo(db *DB, args [][]byte) redis.Reply {
	key := args[0]
	dumpRawCmd, err := parser.ParseOne(args[1])
	if err != nil {
		return protocol.MakeErrorReply("illegal dump cmd: " + err.Error())
	}
	dumpCmd, ok := dumpRawCmd.(*protocol.MultiBulkReply)
	if !ok {
		return protocol.MakeErrorReply("dump cmd is not multi bulk reply")
	}

	dumpCmd.Args[1] = key
	ttlRawCmd, err := parser.ParseOne(args[2])
	if err != nil {
		return protocol.MakeErrorReply("illegal ttl cmd: " + err.Error())
	}

	ttlCmd, ok := ttlRawCmd.(*protocol.MultiBulkReply)
	if !ok {
		return protocol.MakeErrorReply("ttl cmd is not multi bulk reply")
	}

	ttlCmd.Args[1] = key
	db.Remove(string(key))
	dumpResult := db.execWithLock(dumpCmd.Args)
	if protocol.IsErrorReply(dumpResult) {
		return dumpResult
	}
	tllResult := db.execWithLock(ttlCmd.Args)
	if protocol.IsErrorReply(tllResult) {
		return tllResult
	}
	return protocol.MakeOkReply()
}

func execRenameNxTo(db *DB, args [][]byte) redis.Reply {
	return execRename(db, args)
}

func execCopyFrom(db *DB, args [][]byte) redis.Reply {
	return protocol.MakeOkReply()
}

func execCopyTo(db *DB, args [][]byte) redis.Reply {
	key := args[0]
	dumpRawCmd, err := parser.ParseOne(args[1])
	if err != nil {
		return protocol.MakeErrorReply("illegal dump cmd: " + err.Error())
	}
	dumpCmd, ok := dumpRawCmd.(*protocol.MultiBulkReply)
	if !ok {
		return protocol.MakeErrorReply("dump cmd is not multi bulk reply")
	}

	dumpCmd.Args[1] = key
	ttlRawCmd, err := parser.ParseOne(args[2])
	if err != nil {
		return protocol.MakeErrorReply("illegal ttl cmd: " + err.Error())
	}

	ttlCmd, ok := ttlRawCmd.(*protocol.MultiBulkReply)
	if !ok {
		return protocol.MakeErrorReply("ttl cmd is not multi bulk reply")
	}

	ttlCmd.Args[1] = key
	db.Remove(string(key))
	dumpResult := db.execWithLock(dumpCmd.Args)
	if protocol.IsErrorReply(dumpResult) {
		return dumpResult
	}
	tllResult := db.execWithLock(ttlCmd.Args)
	if protocol.IsErrorReply(tllResult) {
		return tllResult
	}
	return protocol.MakeOkReply()
}

func init() {
	RegisterCommand("DumpKey", execDumpKey, writeAllKeys, undoDel, 2, flagReadOnly)
	RegisterCommand("ExistIn", execExistIn, readAllKeys, nil, -1, flagReadOnly)
	RegisterCommand("RenameFrom", execRenameFrom, readFirstKey, nil, 2, flagWrite)
	RegisterCommand("RenameTo", execRenameTo, writeFirstKey, rollbackFirstKey, 4, flagWrite)
	RegisterCommand("RenameNxTo", execRenameTo, writeFirstKey, rollbackFirstKey, 4, flagWrite)
	RegisterCommand("CopyFrom", execCopyFrom, readFirstKey, nil, 2, flagReadOnly)
	RegisterCommand("CopyTo", execCopyTo, writeFirstKey, rollbackFirstKey, 5, flagWrite)
}
