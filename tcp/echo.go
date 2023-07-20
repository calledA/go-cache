package tcp

import (
	"bufio"
	"context"
	"gmr/go-cache/lib/logger"
	"gmr/go-cache/lib/sync/atomic"
	"gmr/go-cache/lib/sync/wait"
	"io"
	"net"
	"sync"
	"time"
)

/**
 * @Author: wanglei
 * @File: echo.go
 * @Version: 1.0.0
 * @Description:
 * @Date: 2023/07/12 14:11
 */

// 接收client请求
type EchoHandler struct {
	activeConn sync.Map
	closing    atomic.Boolean
}

func MakeEchoHandler() *EchoHandler {
	return &EchoHandler{}
}

type EchoClient struct {
	Conn    net.Conn
	Waiting wait.Wait
}

// 关闭连接
func (c *EchoClient) Close() error {
	c.Waiting.WaitWithTimeout(10 * time.Second)
	c.Conn.Close()
	return nil
}

// 处理client请求echo
func (h *EchoHandler) Handle(ctx context.Context, conn net.Conn) {
	if h.closing.Get() {
		// 关闭handler后，拒绝新的连接
		conn.Close()
		return
	}

	client := &EchoClient{
		Conn: conn,
	}
	h.activeConn.Store(client, struct{}{})

	reader := bufio.NewReader(conn)

	for {
		// 可能情况：客户端EOF、客户端超时、服务器提前关闭
		msg, err := reader.ReadString('\n')
		if err != nil {
			if err == io.EOF {
				logger.Info("connection closed")
				h.activeConn.Delete(client)
			} else {
				logger.Error(err)
			}
			return
		}

		client.Waiting.Add(1)
		b := []byte(msg)
		conn.Write(b)
		client.Waiting.Done()
	}
}

// 关闭echo handler
func (h *EchoHandler) Close() error {
	logger.Info("handler shutting down...")
	h.closing.Set(true)
	h.activeConn.Range(func(key, value interface{}) bool {
		client := key.(*EchoClient)
		client.Close()
		return true
	})
	return nil
}
