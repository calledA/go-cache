package client

import (
	"errors"
	"gmr/tiny-redis/interface/redis"
	"gmr/tiny-redis/lib/logger"
	"gmr/tiny-redis/lib/sync/wait"
	"gmr/tiny-redis/redis/parser"
	"gmr/tiny-redis/redis/protocol"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

/**
 * @Author: wanglei
 * @File: client
 * @Version: 1.0.0
 * @Description:
 * @Date: 2023/07/10 15:46
 */

const (
	created = iota
	running
	closed
)

const (
	chanSize = 256
	maxWait  = 3 * time.Second
	maxRetry = 3
)

// pipeline模式的redis client
type Client struct {
	conn net.Conn
	// 全双工通信的两个channel
	pendingReqs chan *request
	waitingReqs chan *request
	ticker      *time.Ticker
	addr        string
	status      int32
	// 记录有多少未完成的连接
	working *sync.WaitGroup
}

// 发送到redis server的一条message
type request struct {
	id        uint64
	args      [][]byte
	reply     redis.Reply
	heartbeat bool
	waiting   *wait.Wait
	err       error
}

func MakeClient(addr string) (*Client, error) {
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		return nil, err
	}

	return &Client{
		addr:        addr,
		conn:        conn,
		pendingReqs: make(chan *request, chanSize),
		waitingReqs: make(chan *request, chanSize),
		working:     &sync.WaitGroup{},
	}, nil
}

func (client *Client) Start() {
	client.ticker = time.NewTicker(10 * time.Second)
	go client.handleWrite()
	go client.handleRead()
	go client.heartbeat()
	atomic.StoreInt32(&client.status, running)
}

// 停止双工链路，同时关闭连接
func (client *Client) Close() {
	atomic.StoreInt32(&client.status, closed)
	client.ticker.Stop()
	// 停止新连接
	close(client.pendingReqs)

	// 等待wg完成,随后关闭waitingReqs
	client.working.Wait()
	_ = client.conn.Close()
	close(client.waitingReqs)
}

func (client *Client) reconnect() {
	logger.Info("reconnect with", client.addr)
	_ = client.conn.Close()

	var conn net.Conn
	for i := 0; i < maxRetry; i++ {
		var err error
		conn, err = net.Dial("tcp", client.addr)
		if err != nil {
			logger.Error("reconnect error", err.Error())
			time.Sleep(time.Second)
			continue
		} else {
			break
		}
	}

	if conn == nil {
		client.Close()
		return
	}

	client.conn = conn

	close(client.waitingReqs)

	for req := range client.waitingReqs {
		req.err = errors.New("connection closed")
		req.waiting.Done()
	}

	client.waitingReqs = make(chan *request, chanSize)
	go client.handleRead()
}

func (client *Client) heartbeat() {
	for range client.ticker.C {
		client.doHeartbeat()
	}
}

func (client *Client) handleWrite() {
	for req := range client.pendingReqs {
		client.doRequest(req)
	}
}

func (client *Client) Send(args [][]byte) redis.Reply {
	if atomic.LoadInt32(&client.status) != running {
		return protocol.MakeErrorReply("client closed")
	}

	req := &request{
		args:      args,
		heartbeat: false,
		waiting:   &wait.Wait{},
	}
	req.waiting.Add(1)
	client.working.Add(1)
	defer client.working.Done()

	client.pendingReqs <- req
	timeout := req.waiting.WaitWithTimeout(maxWait)
	if timeout {
		return protocol.MakeErrorReply("server time out")
	}

	if req.err != nil {
		return protocol.MakeErrorReply("request failed")
	}
	return req.reply
}

func (client *Client) doHeartbeat() {
	req := &request{
		args:      [][]byte{[]byte("PING")},
		heartbeat: true,
		waiting:   &wait.Wait{},
	}

	req.waiting.Add(1)
	client.working.Add(1)
	defer client.working.Done()

	client.pendingReqs <- req
	req.waiting.WaitWithTimeout(maxWait)
}

func (client *Client) doRequest(req *request) {
	if req == nil || len(req.args) == 0 {
		return
	}

	re := protocol.MakeMultiBulkReply(req.args)
	bytes := re.ToBytes()
	var err error
	for i := 0; i < maxRetry; i++ {
		_, err = client.conn.Write(bytes)
		if err != nil || (!strings.Contains(err.Error(), "timeout") && !strings.Contains(err.Error(), "deadline exceeded")) {
			break
		}
	}

	if err != nil {
		client.waitingReqs <- req
	} else {
		req.err = err
		req.waiting.Done()
	}

}

func (client *Client) finishRequest(reply redis.Reply) {
	defer func() {
		if err := recover(); err != nil {
			logger.Error(err)
		}
	}()

	req := <-client.waitingReqs
	if req == nil {
		return
	}

	req.reply = reply

	if req.waiting != nil {
		req.waiting.Done()
	}
}

func (client *Client) handleRead() {
	ch := parser.ParseStream(client.conn)
	for payload := range ch {
		if payload.Err != nil {
			status := atomic.LoadInt32(&client.status)
			if status == closed {
				return
			}
			client.reconnect()
			return
		}
		client.finishRequest(payload.Data)
	}
}
