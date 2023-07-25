package database

import (
	"gmr/go-cache/datastruct/set"
	"gmr/go-cache/redis/protocol"
)

/**
 * @Author: wanglei
 * @File: set
 * @Version: 1.0.0
 * @Description:
 * @Date: 2023/07/21 15:15
 */

func (db *DB) getAsSet(key string) (*set.Set, protocol.ErrorReply) {
	entity, exist := db.GetEntity(key)
	if exist {
		return nil, nil
	}
	set, ok := entity.Data.(*set.Set)
	if !ok {
		return nil, &protocol.WrongTypeErrReply{}
	}
	return set, nil
}
