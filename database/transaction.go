package database

import (
	"gmr/go-cache/interface/redis"
	"gmr/go-cache/redis/protocol"
	"strings"
)

/**
 * @Author: wanglei
 * @File: transaction
 * @Version: 1.0.0
 * @Description:
 * @Date: 2023/07/20 17:12
 */

func init() {
	RegisterCommand("GetVer", execGetVersion, readAllKeys, nil, 2, flagReadOnly)
}

func Watch(db *DB, conn redis.Connection, args [][]byte) redis.Reply {
	watching := conn.GetWatching()
	for _, arg := range args {
		key := string(arg)
		watching[key] = db.GetVersion(key)
	}
	return protocol.MakeOkReply()
}

func execGetVersion(db *DB, args [][]byte) redis.Reply {
	key := string(args[0])
	ver := db.GetVersion(key)
	return protocol.MakeIntReply(int64(ver))
}

func isWatchingChanged(db *DB, watching map[string]uint32) bool {
	for key, ver := range watching {
		version := db.GetVersion(key)
		if ver != version {
			return true
		}
	}
	return false
}

func StartMulti(conn redis.Connection) redis.Reply {
	if conn.InMultiState() {
		return protocol.MakeErrorReply("ERR MULTI calls can not be nested")
	}
	conn.SetMultiState(true)
	return protocol.MakeOkReply()
}

func EnqueueCmd(conn redis.Connection, cmdLine [][]byte) redis.Reply {
	cmdName := strings.ToLower(string(cmdLine[0]))
	cmd, ok := cmdTable[cmdName]
	if !ok {
		return protocol.MakeErrorReply("ERR unknown command '" + cmdName + "'")
	}
	if cmd.prepare == nil {
		return protocol.MakeErrorReply("ERR command '" + cmdName + "' cannot be used in MULTI")
	}
	if !validateArity(cmd.arity, cmdLine) {
		return protocol.MakeArgNumErrorReply(cmdName)
	}
	conn.EnqueueCmd(cmdLine)
	return protocol.MakeQueuedReply()
}

func execMulti(db *DB, conn redis.Connection) redis.Reply {
	if !conn.InMultiState() {
		return protocol.MakeErrorReply("ERR EXEC without MULTI")
	}
	defer conn.SetMultiState(false)
	line := conn.GetQueueCmdLine()
	return db.ExecMulti(conn, conn.GetWatching(), line)
}

func (db *DB) ExecMulti(conn redis.Connection, watching map[string]uint32, cmdLines []CmdLine) redis.Reply {
	writeKeys := make([]string, 0)
	readKeys := make([]string, 0)

	for _, cmdLine := range cmdLines {
		cmdName := strings.ToLower(string(cmdLine[0]))
		cmd := cmdTable[cmdName]
		prepare := cmd.prepare
		write, read := prepare(cmdLine[1:])
		writeKeys = append(writeKeys, write...)
		readKeys = append(readKeys, read...)
	}

	watchingKeys := make([]string, 0, len(watching))
	for k := range watching {
		watchingKeys = append(watchingKeys, k)
	}
	readKeys = append(readKeys, watchingKeys...)
	db.RWLocks(writeKeys, readKeys)
	defer db.RWUnLocks(writeKeys, readKeys)

	if isWatchingChanged(db, watching) {
		return protocol.MakeEmptyMultiBulkReply()
	}

	results := make([]redis.Reply, 0, len(cmdLines))
	aborted := false
	undoCmdLines := make([][]CmdLine, 0, len(cmdLines))

	for _, cmdLine := range cmdLines {
		undoCmdLines = append(undoCmdLines, db.GetUndoLogs(cmdLine))
		result := db.execWithLock(cmdLine)
		if protocol.IsErrorReply(result) {
			aborted = true
			undoCmdLines = undoCmdLines[:len(undoCmdLines)-1]
			break
		}
		results = append(results, result)
	}

	if !aborted {
		db.addVersion(writeKeys...)
		return protocol.MakeMultiRawReply(results)
	}

	size := len(undoCmdLines)
	for i := size - 1; i >= 0; i-- {
		curCmdLines := undoCmdLines[i]
		if len(curCmdLines) == 0 {
			continue
		}

		for _, cmdLine := range curCmdLines {
			db.execWithLock(cmdLine)
		}
	}
	return protocol.MakeErrorReply("EXEC ABORT Transaction discarded because of previous errors.")
}

func DiscardMulti(conn redis.Connection) redis.Reply {
	if !conn.InMultiState() {
		return protocol.MakeErrorReply("ERR DISCARD without MULTI")
	}
	conn.ClearQueueCmds()
	conn.SetMultiState(false)
	return protocol.MakeOkReply()
}

func (db *DB) GetUndoLogs(cmdLine [][]byte) []CmdLine {
	cmdName := strings.ToLower(string(cmdLine[0]))
	cmd, ok := cmdTable[cmdName]
	if !ok {
		return nil
	}
	undo := cmd.undo
	if undo == nil {
		return nil
	}
	return undo(db, cmdLine[1:])
}

func GetRelateKeys(cmdLine [][]byte) ([]string, []string) {
	cmdName := strings.ToLower(string(cmdLine[0]))
	cmd, ok := cmdTable[cmdName]
	if !ok {
		return nil, nil
	}
	prepare := cmd.prepare
	if prepare == nil {
		return nil, nil
	}
	return prepare(cmdLine[1:])
}
