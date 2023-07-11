package server

import (
	idatabase "gmr/tiny-redis/interface/database"
	"gmr/tiny-redis/lib/sync/atomic"
	"sync"
)

/**
 * @Author: wanglei
 * @File: server
 * @Version: 1.0.0
 * @Description: 使用RESP协议的tcp handler
 * @Date: 2023/07/06 18:12
 */

var (
	unknownErrorReplyBytes = []byte("-ERR unknown\r\n")
)

// todo: add
type Handler struct {
	activeConn sync.Map
	db         idatabase.DB
	closing    atomic.Boolean
}

func MakeHandler() *Handler {
	var db idatabase.DB
	if config.Properties.Self != "" && len(config.Properties.Peer) > 0 {
		db = cluster.MakeCluster()
	} else {
		db = database.NewStandaloneServer()
	}
	return &Handler{
		db: db,
	}
}

// server后续实现
