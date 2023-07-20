package database

import (
	"gmr/go-cache/aof"
	"gmr/go-cache/config"
	"gmr/go-cache/pubsub"
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
	dbSet []*atomic.Value
	// hanle pub/sub
	hub *pubsub.Hub
	// handleaof持久化
	aofHandler *aof.Handler

	// 存储master节点地址
	slaveOf     string
	role        int32
	replication *replicationStatus
}

func NewStandaloneServer() *MultiDB {
	mdb := &MultiDB{}

	if config.Properties.Databases == 0 {
		config.Properties.Databases = 16
	}

	mdb.dbSet = make([]*atomic.Value, config.Properties.Databases)
	for i := range mdb.dbSet {

	}
}
