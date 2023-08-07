package database

import (
	"gmr/go-cache/datastruct/list"
	"gmr/go-cache/interface/database"
	"gmr/go-cache/interface/redis"
	"gmr/go-cache/lib/utils"
	"gmr/go-cache/redis/protocol"
	"strconv"
)

/**
 * @Author: wanglei
 * @File: list
 * @Version: 1.0.0
 * @Description:
 * @Date: 2023/08/04 10:13
 */

func (db *DB) getAsList(key string) (list.List, protocol.ErrorReply) {
	entity, ok := db.GetEntity(key)
	if !ok {
		return nil, nil
	}
	list, ok := entity.Data.(list.List)
	if !ok {
		return nil, &protocol.WrongTypeErrorReply{}
	}
	return list, nil
}

func (db *DB) getOrInitList(key string) (l list.List, isNew bool, errReply protocol.ErrorReply) {
	l, errReply = db.getAsList(key)
	if errReply != nil {
		return nil, false, errReply
	}
	isNew = false
	if l == nil {
		l = list.NewQuickList()
		db.PutEntity(key, &database.DataEntity{
			Data: l,
		})
		isNew = true
	}
	return
}

func execLIndex(db *DB, args [][]byte) redis.Reply {
	key := string(args[0])

	index64, err := strconv.ParseInt(string(args[1]), 10, 64)
	if err != nil {
		return protocol.MakeErrorReply("ERR value is not an integer or out of range")
	}
	index := int(index64)

	l, errReply := db.getAsList(key)
	if errReply != nil {
		return errReply
	}
	if l == nil {
		return &protocol.NullBulkReply{}
	}

	size := l.Len()
	if index < -1*size {
		return &protocol.NullBulkReply{}
	} else if index < 0 {
		index = size + index
	} else if index >= size {
		return &protocol.NullBulkReply{}
	}

	val, _ := l.Get(index).([]byte)
	return protocol.MakeBulkReply(val)
}

func execLLen(db *DB, args [][]byte) redis.Reply {
	key := string(args[0])
	l, errReply := db.getAsList(key)
	if errReply != nil {
		return errReply
	}
	if l == nil {
		return protocol.MakeIntReply(0)
	}

	size := int64(l.Len())
	return protocol.MakeIntReply(size)
}

func execLPop(db *DB, args [][]byte) redis.Reply {
	key := string(args[0])
	l, errReply := db.getAsList(key)
	if errReply != nil {
		return errReply
	}
	if l == nil {
		return &protocol.NullBulkReply{}
	}

	val, _ := l.Remove(0).([]byte)
	if l.Len() == 0 {
		db.Remove(key)
	}

	db.addAof(utils.ToCmdLineByByte("lpop", args...))
	return protocol.MakeBulkReply(val)
}

var lPushCmd = []byte("LPUSH")

func undoLPop(db *DB, args [][]byte) []CmdLine {
	key := string(args[0])
	l, errReply := db.getAsList(key)
	if errReply != nil {
		return nil
	}
	if l == nil || l.Len() == 0 {
		return nil
	}

	element, _ := l.Get(0).([]byte)
	return []CmdLine{
		{
			lPushCmd,
			args[0],
			element,
		},
	}
}

func execLPush(db *DB, args [][]byte) redis.Reply {
	key := string(args[0])
	values := args[1:]

	l, _, errReply := db.getOrInitList(key)
	if errReply != nil {
		return errReply
	}

	for _, value := range values {
		l.Insert(0, value)
	}

	db.addAof(utils.ToCmdLineByByte("lpush", args...))
	return protocol.MakeIntReply(int64(l.Len()))
}

func undoLPush(db *DB, args [][]byte) []CmdLine {
	key := string(args[0])
	count := len(args) - 1
	cmdLines := make([]CmdLine, 0, count)
	for i := 0; i < count; i++ {
		cmdLines = append(cmdLines, utils.ToCmdLine("LPOP", key))
	}
	return cmdLines
}

func execLPushX(db *DB, args [][]byte) redis.Reply {
	key := string(args[0])
	values := args[1:]
	l, errReply := db.getAsList(key)
	if errReply != nil {
		return errReply
	}
	if l == nil {
		return protocol.MakeIntReply(0)
	}

	for _, value := range values {
		l.Insert(0, value)
	}

	db.addAof(utils.ToCmdLineByByte("lpushx", args...))
	return protocol.MakeIntReply(int64(l.Len()))
}

func execLRange(db *DB, args [][]byte) redis.Reply {
	key := string(args[0])

	start64, err := strconv.ParseInt(string(args[1]), 10, 64)
	if err != nil {
		return protocol.MakeErrorReply("ERR value is not an integer or out of range")
	}
	start := int(start64)

	stop64, err := strconv.ParseInt(string(args[2]), 10, 64)
	if err != nil {
		return protocol.MakeErrorReply("ERR value is not an integer or out of range")
	}
	stop := int(stop64)

	l, errReply := db.getAsList(key)
	if errReply != nil {
		return errReply
	}
	if l == nil {
		return &protocol.EmptyMultiBulkReply{}
	}

	size := l.Len()
	if start < -1*size {
		start = 0
	} else if start < 0 {
		start = size + start
	} else if start >= size {
		return &protocol.EmptyMultiBulkReply{}
	}

	if stop < -1*size {
		stop = 0
	} else if stop < 0 {
		stop = size + stop + 1
	} else if start >= size {
		stop = size
	}

	if stop < start {
		stop = start
	}

	slice := l.Range(start, stop)
	result := make([][]byte, len(slice))
	for i, raw := range slice {
		bytes, _ := raw.([]byte)
		result[i] = bytes
	}
	return protocol.MakeMultiBulkReply(result)
}

func execLRem(db *DB, args [][]byte) redis.Reply {
	key := string(args[0])

	count64, err := strconv.ParseInt(string(args[1]), 10, 64)
	if err != nil {
		return protocol.MakeErrorReply("ERR value is not an integer or out of range")
	}

	count := int(count64)
	value := args[2]

	l, errReply := db.getAsList(key)
	if errReply != nil {
		return errReply
	}

	if l == nil {
		return protocol.MakeIntReply(0)
	}

	var removed int

	if count == 0 {
		removed = l.RemoveAllByValue(func(val interface{}) bool {
			return utils.Equals(val, value)
		})
	} else if count > 0 {
		removed = l.RemoveByVal(func(val interface{}) bool {
			return utils.Equals(val, value)
		}, count)
	} else {
		removed = l.ReverseRemoveByValue(func(val interface{}) bool {
			return utils.Equals(val, value)
		}, -count)
	}

	if l.Len() == 0 {
		db.Remove(key)
	}

	if removed > 0 {
		db.addAof(utils.ToCmdLineByByte("lrem", args...))
	}

	return protocol.MakeIntReply(int64(removed))
}

func execLSet(db *DB, args [][]byte) redis.Reply {
	key := string(args[0])

	index64, err := strconv.ParseInt(string(args[1]), 10, 64)
	if err != nil {
		return protocol.MakeErrorReply("ERR value is not an integer or out of range")
	}

	index := int(index64)
	value := args[2]

	l, errReply := db.getAsList(key)
	if errReply != nil {
		return errReply
	}

	if l == nil {
		return protocol.MakeErrorReply("ERR no such key")
	}

	size := l.Len()
	if index < -1*size {
		return protocol.MakeErrorReply("ERR index out of range")
	} else if index < 0 {
		index = size + index
	} else if index >= size {
		return protocol.MakeErrorReply("ERR index out of range")
	}

	l.Set(index, value)
	db.addAof(utils.ToCmdLineByByte("lset", args...))
	return &protocol.OkReply{}
}

func undoLSet(db *DB, args [][]byte) []CmdLine {
	key := string(args[0])

	index64, err := strconv.ParseInt(string(args[1]), 10, 64)
	if err != nil {
		return nil
	}

	index := int(index64)

	l, errReply := db.getAsList(key)
	if errReply != nil {
		return nil
	}

	if l == nil {
		return nil
	}

	size := l.Len()

	if index < -1*size {
		return nil
	} else if index < 0 {
		index = size + index
	} else if index >= size {
		return nil
	}

	value, _ := l.Get(index).([]byte)
	return []CmdLine{
		{
			[]byte("LSET"),
			args[0],
			args[1],
			value,
		},
	}
}

func execRPop(db *DB, args [][]byte) redis.Reply {
	key := string(args[0])

	l, errReply := db.getAsList(key)
	if errReply != nil {
		return errReply
	}

	if l == nil {
		return &protocol.NullBulkReply{}
	}

	val, _ := l.RemoveLast().([]byte)
	if l.Len() == 0 {
		db.Remove(key)
	}
	db.addAof(utils.ToCmdLineByByte("rpop", args...))
	return protocol.MakeBulkReply(val)
}

var rPushCmd = []byte("RPUSH")

func undoRPop(db *DB, args [][]byte) []CmdLine {
	key := string(args[0])

	l, errReply := db.getAsList(key)
	if errReply != nil {
		return nil
	}

	if l == nil || l.Len() == 0 {
		return nil
	}

	element, _ := l.Get(l.Len() - 1).([]byte)
	return []CmdLine{
		{
			rPushCmd,
			args[0],
			element,
		},
	}
}

func prepareRPopLPush(args [][]byte) ([]string, []string) {
	return []string{
		string(args[0]),
		string(args[1]),
	}, nil
}

func execRPopLPush(db *DB, args [][]byte) redis.Reply {
	sourceKey := string(args[0])
	destKey := string(args[1])

	sourceList, errReply := db.getAsList(sourceKey)
	if errReply != nil {
		return errReply
	}
	if sourceList == nil {
		return &protocol.NullBulkReply{}
	}

	destList, _, errReply := db.getOrInitList(destKey)
	if errReply != nil {
		return errReply
	}

	val, _ := sourceList.RemoveLast().([]byte)
	destList.Insert(0, val)

	if sourceList.Len() == 0 {
		db.Remove(sourceKey)
	}

	db.addAof(utils.ToCmdLineByByte("rpoplpush", args...))
	return protocol.MakeBulkReply(val)
}

func undoRPopLPush(db *DB, args [][]byte) []CmdLine {
	sourceKey := string(args[0])

	l, errReply := db.getAsList(sourceKey)
	if errReply != nil {
		return nil
	}

	if l == nil || l.Len() == 0 {
		return nil
	}

	element, _ := l.Get(l.Len() - 1).([]byte)
	return []CmdLine{
		{
			rPushCmd,
			args[0],
			element,
		},
		{
			[]byte("LPOP"),
			args[1],
		},
	}
}

func execRPush(db *DB, args [][]byte) redis.Reply {
	key := string(args[0])
	values := args[1:]

	l, _, errReply := db.getOrInitList(key)
	if errReply != nil {
		return errReply
	}

	for _, value := range values {
		l.Add(value)
	}

	db.addAof(utils.ToCmdLineByByte("rpush", args...))
	return protocol.MakeIntReply(int64(l.Len()))
}

func undoRPush(db *DB, args [][]byte) []CmdLine {
	key := string(args[0])
	count := len(args) - 1
	cmdLines := make([]CmdLine, 0, count)
	for i := 0; i < count; i++ {
		cmdLines = append(cmdLines, utils.ToCmdLine("RPOP", key))
	}
	return cmdLines
}

func execRPushX(db *DB, args [][]byte) redis.Reply {
	if len(args) < 2 {
		return protocol.MakeErrorReply("ERR wrong number of arguments for 'rpush' command")
	}

	key := string(args[0])
	values := args[1:]

	l, errReply := db.getAsList(key)
	if errReply != nil {
		return errReply
	}
	if l == nil {
		return protocol.MakeIntReply(0)
	}

	for _, value := range values {
		l.Add(value)
	}
	db.addAof(utils.ToCmdLineByByte("rpushx", args...))
	return protocol.MakeIntReply(int64(l.Len()))
}

func init() {
	RegisterCommand("LPush", execLPush, writeFirstKey, undoLPush, -3, flagWrite)
	RegisterCommand("LPushX", execLPushX, writeFirstKey, undoLPush, -3, flagWrite)
	RegisterCommand("RPush", execRPush, writeFirstKey, undoRPush, -3, flagWrite)
	RegisterCommand("RPushX", execRPushX, writeFirstKey, undoRPush, -3, flagWrite)
	RegisterCommand("LPop", execLPop, writeFirstKey, undoLPop, 2, flagWrite)
	RegisterCommand("RPop", execRPop, writeFirstKey, undoRPop, 2, flagWrite)
	RegisterCommand("RPopLPush", execRPopLPush, prepareRPopLPush, undoRPopLPush, 3, flagWrite)
	RegisterCommand("LRem", execLRem, writeFirstKey, rollbackFirstKey, 4, flagWrite)
	RegisterCommand("LLen", execLLen, readFirstKey, nil, 2, flagReadOnly)
	RegisterCommand("LIndex", execLIndex, readFirstKey, nil, 3, flagReadOnly)
	RegisterCommand("LSet", execLSet, writeFirstKey, undoLSet, 4, flagWrite)
	RegisterCommand("LRange", execLRange, readFirstKey, nil, 4, flagReadOnly)
}
