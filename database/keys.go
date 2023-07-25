package database

import (
	"gmr/go-cache/lib/utils"
	"gmr/go-cache/redis/protocol"
	"strconv"
	"time"
)

/**
 * @Author: wanglei
 * @File: keys
 * @Version: 1.0.0
 * @Description:
 * @Date: 2023/07/21 15:31
 */

func toTTLCmd(db *DB, key string) *protocol.MultiBulkReply {
	raw, exist := db.ttlMap.Get(key)
	if !exist {
		return protocol.MakeMultiBulkReply(utils.ToCmdLine("PERSIST", key))
	}
	expire, _ := raw.(time.Time)
	timestamp := strconv.FormatInt(expire.UnixNano()/1000/1000, 10)
	return protocol.MakeMultiBulkReply(utils.ToCmdLine("PEXPIREAT", key, timestamp))
}
