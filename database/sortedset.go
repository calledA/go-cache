package database

import (
	"gmr/go-cache/datastruct/sortedset"
	"gmr/go-cache/redis/protocol"
)

/**
 * @Author: wanglei
 * @File: sortedset
 * @Version: 1.0.0
 * @Description:
 * @Date: 2023/07/21 15:27
 */

func (db *DB) getAsSortedSet(key string) (*sortedset.SortedSet, protocol.ErrorReply) {
	entity, exists := db.GetEntity(key)
	if !exists {
		return nil, nil
	}
	sortedSet, ok := entity.Data.(*sortedset.SortedSet)
	if !ok {
		return nil, &protocol.WrongTypeErrorReply{}
	}
	return sortedSet, nil
}
