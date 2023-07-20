package connection

import (
	"gmr/go-cache/lib/sync/wait"
	"net"
	"sync"
	"time"
)

/**
 * @Author: wanglei
 * @File: conn
 * @Version: 1.0.0
 * @Description:
 * @Date: 2023/07/10 14:40
 */

const (
	// 用户的客户端
	NormalCli = iota
	// fake client with replication master
	ReplicationRecvCli
)

// redist-cli的connection
type Connection struct {
	conn net.Conn
	// 等待直到protocol完成
	waitingReply wait.Wait
	// 处理数据时加lock
	mutex sync.Mutex
	// subscribing channels
	subs map[string]bool
	// password可能在运行时被修改，password作为存储密码
	password string
	// queued commands for multi
	multiState bool
	queue      [][][]byte
	watching   map[string]uint32

	// 却换数据库
	selectedDB int
	role       int32
}

// 返回connection实例
func NewConnection(conn net.Conn) *Connection {
	return &Connection{
		conn: conn,
	}
}

// 返回远程网络地址
func (c *Connection) RemoteAddr() net.Addr {
	return c.conn.RemoteAddr()
}

// 关闭client连接
func (c *Connection) Close() error {
	c.waitingReply.WaitWithTimeout(10 * time.Second)
	_ = c.conn.Close()
	return nil
}

// 通过tcp返回响应
func (c *Connection) Write(data []byte) error {
	if len(data) == 0 {
		return nil
	}
	c.mutex.Lock()
	c.waitingReply.Add(1)

	defer func() {
		c.waitingReply.Done()
		c.mutex.Unlock()
	}()

	_, err := c.conn.Write(data)
	return err
}

// 将当前connection以subscriber向channel添加
func (c *Connection) Subscribe(channel string) {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	if c.subs == nil {
		c.subs = make(map[string]bool)
	}
	c.subs[channel] = true
}

// 将当前connection以subscriber向channel移除
func (c *Connection) Unsubscribe(channel string) {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	if len(c.subs) == 0 {
		return
	}
	delete(c.subs, channel)
}

// 返回subscribing的channel的数量
func (c *Connection) SubCount() int {
	return len(c.subs)
}

// 返回所有subscribing channel
func (c *Connection) GetChannels() []string {
	if c.subs == nil {
		return make([]string, 0)
	}

	channels := make([]string, len(c.subs))
	i := 0
	for channel := range c.subs {
		channels[i] = channel
		i++
	}
	return channels
}

// 存储auth密码
func (c *Connection) SetPassword(password string) {
	c.password = password
}

// 获取密码
func (c *Connection) GetPassword() string {
	return c.password
}

// 未提交事务中的连接
func (c *Connection) InMultiState() bool {
	return c.multiState
}

// 设置transaction flag
func (c *Connection) SetMultiState(state bool) {
	if !state {
		c.watching = nil
		c.queue = nil
	}
}

// 返回当前事务的queued commands
func (c *Connection) GetQueueCmdLine() [][][]byte {
	return c.queue
}

// 当前事务的enqueued command
func (c *Connection) EnqueueCmd(cmdLine [][]byte) {
	c.queue = append(c.queue, cmdLine)
}

// 清除当前事务的queued command
func (c *Connection) ClearQueueCmds() {
	c.queue = nil
}

// 返回watcing keys 以及版本
func (c *Connection) GetWatching() map[string]uint32 {
	if c.watching == nil {
		c.watching = make(map[string]uint32)
	}
	return c.watching
}

// 返回当前所选db的index
func (c *Connection) GetDBIndex() int {
	return c.selectedDB
}

// 切换数据库
func (c *Connection) SelectDB(dbNum int) {
	c.selectedDB = dbNum
}

// 返回当前连接的role
func (c *Connection) GetRole() int32 {
	if c == nil {
		return NormalCli
	}
	return c.role
}

// 设置当前连接的role
func (c *Connection) SetRole(role int32) {
	c.role = role
}
