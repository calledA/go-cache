package pubsub

import (
	"gmr/tiny-redis/datastruct/dict"
	"gmr/tiny-redis/datastruct/lockmap"
)

/**
 * @Author: wanglei
 * @File: hub
 * @Version: 1.0.0
 * @Description:
 * @Date: 2023/07/10 20:15
 */

// 存储所有订阅关系
type Hub struct {
	subs      dict.Dict
	subLocker *lockmap.Locks
}

func MakeHub() *Hub {
	return &Hub{
		subs:      dict.MakeConcurrentDict(4),
		subLocker: lockmap.MakeLocks(16),
	}
}
