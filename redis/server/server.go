package server

import (
	"context"
	idatabase "gmr/tiny-redis/interface/database"
	"gmr/tiny-redis/lib/logger"
	"gmr/tiny-redis/lib/sync/atomic"
	"gmr/tiny-redis/redis/connection"
	"gmr/tiny-redis/redis/parser"
	"gmr/tiny-redis/redis/protocol"
	"io"
	"net"
	"strings"
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

type Handler struct {
	activeConn sync.Map
	db         idatabase.DB
	closing    atomic.Boolean
}

func MakeHandler() *Handler {
	var db idatabase.DB
	// todo:cluster waiting
	//if config.Properties.Self != "" && len(config.Properties.Peer) > 0 {
	//	db = cluster.MakeCluster()
	//} else {
	//	db = database.NewStandaloneServer()
	//}
	return &Handler{
		db: db,
	}
}

// server后续实现
func (h *Handler) closeClient(client *connection.Connection) {
	client.Close()
	h.db.AfterClientClosed(client)
	h.activeConn.Delete(client)
}

func (h *Handler) Handle(ctx context.Context, conn net.Conn) {
	if h.closing.Get() {
		conn.Close()
		return
	}

	client := connection.NewConnection(conn)
	h.activeConn.Store(client, 1)

	ch := parser.ParseStream(conn)

	for payload := range ch {
		if payload.Err != nil {
			if payload.Err == io.EOF || payload.Err == io.ErrUnexpectedEOF || strings.Contains(payload.Err.Error(), "use of closed network connection") {
				h.closeClient(client)
				logger.Info("connection closed" + client.RemoteAddr().String())
				return
			}

			errReply := protocol.MakeErrorReply(payload.Err.Error())
			err := client.Write(errReply.ToBytes())
			if err != nil {
				h.closeClient(client)
				logger.Info("connection closed" + client.RemoteAddr().String())
				return
			}
			continue
		}
		if payload.Data == nil {
			logger.Error("empty payload error")
			continue
		}

		r, ok := payload.Data.(*protocol.MultiBulkReply)
		if !ok {
			logger.Error("require multi bulk protocol")
			continue
		}
		result := h.db.Exec(client, r.Args)
		if result != nil {
			client.Write(result.ToBytes())
		} else {
			client.Write(unknownErrorReplyBytes)
		}
	}
}

func (h *Handler) Close() error {
	logger.Info("handler shutting down...")
	h.closing.Set(true)
	h.activeConn.Range(func(key, value any) bool {
		client := key.(*connection.Connection)
		client.Close()
		return true
	})
	h.db.Close()
	return nil
}
