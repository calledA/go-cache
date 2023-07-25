package aof

import (
	"gmr/go-cache/interface/database"
	"gmr/go-cache/redis/protocol"
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

	// todo: implement
	switch _ := entity.Data.(type) {

	}

	return cmd
}
