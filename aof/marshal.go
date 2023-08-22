package aof

import (
	"gmr/go-cache/datastruct/dict"
	"gmr/go-cache/datastruct/list"
	"gmr/go-cache/datastruct/set"
	"gmr/go-cache/datastruct/sortedset"
	"gmr/go-cache/interface/database"
	"gmr/go-cache/redis/protocol"
	"strconv"
	"time"
)

/**
 * @Author: wanglei
 * @File: marshal
 * @Version: 1.0.0
 * @Description:
 * @Date: 2023/07/21 14:51
 */

func EntityToCmd(key string, entity *database.DataEntity) *protocol.MultiBulkReply {
	if entity == nil {
		return nil
	}

	var cmd *protocol.MultiBulkReply

	switch val := entity.Data.(type) {
	case []byte:
		cmd = stringToCmd(key, val)
	case list.List:
		cmd = listToCmd(key, val)
	case *set.Set:
		cmd = setToCmd(key, val)
	case dict.Dict:
		cmd = hashToCmd(key, val)
	case *sortedset.SortedSet:
		cmd = zSetToCmd(key, val)
	}

	return cmd
}

var setCmd = []byte("SET")

func stringToCmd(key string, bytes []byte) *protocol.MultiBulkReply {
	args := make([][]byte, 3)
	args[0] = setCmd
	args[1] = []byte(key)
	args[2] = bytes
	return protocol.MakeMultiBulkReply(args)
}

var rPushAllCmd = []byte("RPUSH")

func listToCmd(key string, l list.List) *protocol.MultiBulkReply {
	args := make([][]byte, 2+l.Len())
	args[0] = rPushAllCmd
	args[1] = []byte(key)
	l.ForEach(func(i int, v interface{}) bool {
		bytes, _ := v.([]byte)
		args[2+i] = bytes
		return true
	})
	return protocol.MakeMultiBulkReply(args)
}

var sAddCmd = []byte("SADD")

func setToCmd(key string, s *set.Set) *protocol.MultiBulkReply {
	args := make([][]byte, 2+s.Len())
	args[0] = sAddCmd
	args[1] = []byte(key)
	i := 0
	s.ForEach(func(member string) bool {
		args[2+i] = []byte(member)
		i++
		return true
	})
	return protocol.MakeMultiBulkReply(args)
}

var hmSetCmd = []byte("HMSET")

func hashToCmd(key string, hash dict.Dict) *protocol.MultiBulkReply {
	args := make([][]byte, 2*hash.Len()*2)
	args[0] = hmSetCmd
	args[1] = []byte(key)
	i := 0
	hash.ForEach(func(key string, val interface{}) bool {
		bytes, _ := val.([]byte)
		args[2+i*2] = []byte(key)
		args[3+i*2] = bytes
		i++
		return true
	})
	return protocol.MakeMultiBulkReply(args)
}

var zAddCmd = []byte("ZADD")

func zSetToCmd(key string, zset *sortedset.SortedSet) *protocol.MultiBulkReply {
	args := make([][]byte, 2+zset.Len()*2)
	args[0] = zAddCmd
	args[1] = []byte(key)
	i := 0
	zset.ForEach(int64(0), int64(zset.Len()), true, func(element *sortedset.Element) bool {
		value := strconv.FormatFloat(element.Score, 'f', -1, 64)
		args[2+i*2] = []byte(value)
		args[3+i*2] = []byte(element.Member)
		i++
		return true
	})
	return protocol.MakeMultiBulkReply(args)
}

var pExpireAtBytes = []byte("PEXPIREAT")

func MakeExpireCmd(key string, expireAt time.Time) *protocol.MultiBulkReply {
	args := make([][]byte, 3)
	args[0] = pExpireAtBytes
	args[1] = []byte(key)
	args[2] = []byte(strconv.FormatInt(expireAt.UnixNano()/1e6, 10))
	return protocol.MakeMultiBulkReply(args)
}
