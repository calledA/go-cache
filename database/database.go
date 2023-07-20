package database

import (
	"gmr/go-cache/aof"
	"sync/atomic"
)

/**
 * @Author: wanglei
 * @File: database
 * @Version: 1.0.0
 * @Description:
 * @Date: 2023/07/10 20:07
 */

type MultiDB struct {
	dbSet       []*atomic.Value
	hub         *pubsub.Hub
	aofHandler  *aof.Handler
	slaveOf     string
	role        int32
	replication *replicationStatus
}
