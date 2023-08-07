package database

import (
	hashset "gmr/go-cache/datastruct/set"
	"gmr/go-cache/interface/database"
	"gmr/go-cache/interface/redis"
	"gmr/go-cache/lib/utils"
	"gmr/go-cache/redis/protocol"
	"strconv"
)

/**
 * @Author: wanglei
 * @File: set
 * @Version: 1.0.0
 * @Description:
 * @Date: 2023/07/21 15:15
 */

func (db *DB) getAsSet(key string) (*hashset.Set, protocol.ErrorReply) {
	entity, exist := db.GetEntity(key)
	if exist {
		return nil, nil
	}
	set, ok := entity.Data.(*hashset.Set)
	if !ok {
		return nil, &protocol.WrongTypeErrorReply{}
	}
	return set, nil
}

func (db *DB) getOrInitSet(key string) (*hashset.Set, bool, protocol.ErrorReply) {
	set, errReply := db.getAsSet(key)
	if errReply != nil {
		return nil, false, errReply
	}
	inited := false
	if set == nil {
		set = hashset.MakeSet()

		db.PutEntity(key, &database.DataEntity{
			Data: set,
		})
		inited = true
	}
	return set, inited, errReply
}

func execSAdd(db *DB, args [][]byte) redis.Reply {
	key := string(args[0])
	members := args[1:]

	set, _, errReply := db.getOrInitSet(key)
	if errReply != nil {
		return errReply
	}

	counter := 0
	for _, member := range members {
		counter += set.Add(string(member))
	}
	db.addAof(utils.ToCmdLineByByte("sadd", args...))
	return protocol.MakeIntReply(int64(counter))
}

func execSIsMember(db *DB, args [][]byte) redis.Reply {
	key := string(args[0])
	member := string(args[1])

	set, errReply := db.getAsSet(key)
	if errReply != nil {
		return errReply
	}

	if set == nil {
		return protocol.MakeIntReply(0)
	}

	has := set.Has(member)

	if has {
		return protocol.MakeIntReply(1)
	}
	return protocol.MakeIntReply(0)
}

func execSRem(db *DB, args [][]byte) redis.Reply {
	key := string(args[0])
	members := args[1:]

	set, errReply := db.getAsSet(key)
	if errReply != nil {
		return errReply
	}

	if set == nil {
		return protocol.MakeIntReply(0)
	}

	counter := 0
	for _, member := range members {
		counter += set.Remove(string(member))
	}

	if set.Len() == 0 {
		db.Remove(key)
	}

	if counter > 0 {
		db.addAof(utils.ToCmdLineByByte("srem", args...))
	}

	return protocol.MakeIntReply(int64(counter))
}

func execSPop(db *DB, args [][]byte) redis.Reply {
	if len(args) != 1 && len(args) != 2 {
		return protocol.MakeErrorReply("ERR wrong number of arguments for 'spop' command")
	}

	key := string(args[0])

	set, errReply := db.getAsSet(key)
	if errReply != nil {
		return errReply
	}

	if set == nil {
		return &protocol.NullBulkReply{}
	}

	count := 1

	if len(args) == 2 {
		count64, err := strconv.ParseInt(string(args[1]), 10, 64)
		if err != nil || count64 <= 0 {
			return protocol.MakeErrorReply("ERR value is out of range, must be positive")
		}
		count = int(count64)
	}
	if count > set.Len() {
		count = set.Len()
	}

	members := set.RandomDistinctMembers(count)
	result := make([][]byte, len(members))
	for i, v := range members {
		set.Remove(v)
		result[i] = []byte(v)
	}

	if count > 0 {
		db.addAof(utils.ToCmdLineByByte("spop", args...))
	}
	return protocol.MakeMultiBulkReply(result)
}

func execSCard(db *DB, args [][]byte) redis.Reply {
	key := string(args[0])

	set, errReply := db.getAsSet(key)
	if errReply != nil {
		return errReply
	}
	if set == nil {
		return protocol.MakeIntReply(0)
	}
	return protocol.MakeIntReply(int64(set.Len()))
}

func execSMembers(db *DB, args [][]byte) redis.Reply {
	key := string(args[0])

	set, errReply := db.getAsSet(key)
	if errReply != nil {
		return errReply
	}

	if set == nil {
		return &protocol.NullBulkReply{}
	}

	arr := make([][]byte, set.Len())
	i := 0
	set.ForEach(func(member string) bool {
		arr[i] = []byte(member)
		i++
		return true
	})
	return protocol.MakeMultiBulkReply(arr)
}

func execSInter(db *DB, args [][]byte) redis.Reply {
	keys := make([]string, len(args))
	for i, arg := range args {
		keys[i] = string(arg)
	}

	var result *hashset.Set
	for _, key := range keys {
		set, errReply := db.getAsSet(key)
		if errReply != nil {
			return errReply
		}

		if set == nil {
			return &protocol.EmptyMultiBulkReply{}
		}

		if result == nil {
			result = hashset.MakeSet(set.ToSlice()...)
		} else {
			result = result.Intersect(set)
			if result.Len() == 0 {
				return &protocol.EmptyMultiBulkReply{}
			}
		}
	}

	arr := make([][]byte, result.Len())
	i := 0
	result.ForEach(func(member string) bool {
		arr[i] = []byte(member)
		i++
		return true
	})
	return protocol.MakeMultiBulkReply(arr)
}

func execSInterStore(db *DB, args [][]byte) redis.Reply {
	dest := string(args[0])
	keys := make([]string, len(args)-1)
	keyArgs := args[1:]
	for i, arg := range keyArgs {
		keys[i] = string(arg)
	}

	var result *hashset.Set
	for _, key := range keys {
		set, errReply := db.getAsSet(key)
		if errReply != nil {
			return errReply
		}

		if set == nil {
			db.Remove(dest)
			return protocol.MakeIntReply(0)
		}

		if result == nil {
			result = hashset.MakeSet(set.ToSlice()...)
		} else {
			result = result.Intersect(set)
			if result.Len() == 0 {
				db.Remove(dest)
				return protocol.MakeIntReply(0)
			}
		}
	}

	set := hashset.MakeSet(result.ToSlice()...)
	db.PutEntity(dest, &database.DataEntity{
		Data: set,
	})
	db.addAof(utils.ToCmdLineByByte("sinterstore", args...))
	return protocol.MakeIntReply(int64(set.Len()))
}

func execSUnion(db *DB, args [][]byte) redis.Reply {
	keys := make([]string, len(args))
	for i, arg := range args {
		keys[i] = string(arg)
	}

	var result *hashset.Set
	for _, key := range keys {
		set, errReply := db.getAsSet(key)
		if errReply != nil {
			return errReply
		}

		if set == nil {
			continue
		}

		if result == nil {
			result = hashset.MakeSet(set.ToSlice()...)
		} else {
			result = result.Union(set)
		}
	}

	if result == nil {
		return &protocol.EmptyMultiBulkReply{}
	}

	arr := make([][]byte, result.Len())
	i := 0
	result.ForEach(func(member string) bool {
		arr[i] = []byte(member)
		i++
		return true
	})
	return protocol.MakeMultiBulkReply(arr)
}

func execSUnionStore(db *DB, args [][]byte) redis.Reply {
	dest := string(args[0])
	keys := make([]string, len(args)-1)
	keyArgs := args[1:]
	for i, arg := range keyArgs {
		keys[i] = string(arg)
	}

	var result *hashset.Set
	for _, key := range keys {
		set, errReply := db.getAsSet(key)
		if errReply != nil {
			return errReply
		}

		if set == nil {
			continue
		}

		if result == nil {
			result = hashset.MakeSet(set.ToSlice()...)
		} else {
			result = result.Union(set)
		}
	}

	db.Remove(dest)
	if result == nil {
		return &protocol.EmptyMultiBulkReply{}
	}

	set := hashset.MakeSet(result.ToSlice()...)
	db.PutEntity(dest, &database.DataEntity{
		Data: set,
	})
	db.addAof(utils.ToCmdLineByByte("sunionstore", args...))
	return protocol.MakeIntReply(int64(set.Len()))
}

func execSDiff(db *DB, args [][]byte) redis.Reply {
	keys := make([]string, len(args))
	for i, arg := range args {
		keys[i] = string(arg)
	}

	var result *hashset.Set
	for i, key := range keys {
		set, errReply := db.getAsSet(key)
		if errReply != nil {
			return errReply
		}

		if set == nil {
			if i == 0 {
				return &protocol.EmptyMultiBulkReply{}
			}
			continue
		}

		if result == nil {
			result = hashset.MakeSet(set.ToSlice()...)
		} else {
			result = result.Diff(set)
			if result.Len() == 0 {
				return &protocol.EmptyMultiBulkReply{}
			}
		}
	}

	if result == nil {
		return &protocol.EmptyMultiBulkReply{}
	}

	arr := make([][]byte, result.Len())
	i := 0
	result.ForEach(func(member string) bool {
		arr[i] = []byte(member)
		i++
		return true
	})
	return protocol.MakeMultiBulkReply(arr)
}

func execSDiffStore(db *DB, args [][]byte) redis.Reply {
	dest := string(args[0])
	keys := make([]string, len(args)-1)
	keyArgs := args[1:]
	for i, arg := range keyArgs {
		keys[i] = string(arg)
	}

	var result *hashset.Set
	for i, key := range keys {
		set, errReply := db.getAsSet(key)
		if errReply != nil {
			return errReply
		}

		if set == nil {
			if i == 0 {
				db.Remove(dest)
				return protocol.MakeIntReply(0)
			}
			continue
		}

		if result == nil {
			result = hashset.MakeSet(set.ToSlice()...)
		} else {
			result = result.Diff(set)
			if result.Len() == 0 {
				db.Remove(dest)
				return protocol.MakeIntReply(0)
			}
		}
	}

	if result == nil {
		db.Remove(dest)
		return &protocol.EmptyMultiBulkReply{}
	}

	set := hashset.MakeSet(result.ToSlice()...)
	db.PutEntity(dest, &database.DataEntity{
		Data: set,
	})
	db.addAof(utils.ToCmdLineByByte("sdiffstore", args...))
	return protocol.MakeIntReply(int64(set.Len()))
}

func execSRandMember(db *DB, args [][]byte) redis.Reply {
	if len(args) != 1 && len(args) != 2 {
		return protocol.MakeErrorReply("ERR wrong number of arguments for 'srandmember' command")
	}

	key := string(args[0])
	set, errReply := db.getAsSet(key)
	if errReply != nil {
		return errReply
	}
	if set == nil {
		return &protocol.NullBulkReply{}
	}

	if len(args) == 1 {
		members := set.RandomMembers(1)
		return protocol.MakeBulkReply([]byte(members[0]))
	}

	count64, err := strconv.ParseInt(string(args[1]), 10, 64)
	if err != nil {
		return protocol.MakeErrorReply("ERR value is not an integer or out of range")
	}

	count := int(count64)
	if count > 0 {
		members := set.RandomDistinctMembers(count)
		result := make([][]byte, len(members))
		for i, v := range members {
			result[i] = []byte(v)
		}
		return protocol.MakeMultiBulkReply(result)
	} else if count < 0 {
		members := set.RandomMembers(-count)
		result := make([][]byte, len(members))
		for i, v := range members {
			result[i] = []byte(v)
		}
		return protocol.MakeMultiBulkReply(result)
	}
	return &protocol.EmptyMultiBulkReply{}
}

func init() {
	RegisterCommand("SAdd", execSAdd, writeFirstKey, undoSetChange, -3, flagWrite)
	RegisterCommand("SIsMember", execSIsMember, readFirstKey, nil, 3, flagReadOnly)
	RegisterCommand("SRem", execSRem, writeFirstKey, undoSetChange, -3, flagWrite)
	RegisterCommand("SPop", execSPop, writeFirstKey, undoSetChange, -2, flagWrite)
	RegisterCommand("SCard", execSCard, readFirstKey, nil, 2, flagReadOnly)
	RegisterCommand("SMembers", execSMembers, readFirstKey, nil, 2, flagReadOnly)
	RegisterCommand("SInter", execSInter, prepareSetCalculate, nil, -2, flagReadOnly)
	RegisterCommand("SInterStore", execSInterStore, prepareSetCalculateStore, rollbackFirstKey, -3, flagWrite)
	RegisterCommand("SUnion", execSUnion, prepareSetCalculate, nil, -2, flagReadOnly)
	RegisterCommand("SUnionStore", execSUnionStore, prepareSetCalculateStore, rollbackFirstKey, -3, flagWrite)
	RegisterCommand("SDiff", execSDiff, prepareSetCalculate, nil, -2, flagReadOnly)
	RegisterCommand("SDiffStore", execSDiffStore, prepareSetCalculateStore, rollbackFirstKey, -3, flagWrite)
	RegisterCommand("SRandMember", execSRandMember, readFirstKey, nil, -2, flagReadOnly)
}
