package redis

/**
 * @Author: wanglei
 * @File: Connection
 * @Version: 1.0.0
 * @Description: client connection 方法
 * @Date: 2023/07/10 11:40
 */

// Connection client连接方法接口
type Connection interface {
	Write([]byte) error
	SetPassword(string)
	GetPassword() string

	// subscribe channel
	Subscribe(channel string)
	Unsubscribe(channel string)
	SubCount() int
	GetChannels() []string

	// mutli 命令
	InMultiState() bool
	SetMultiState(bool)
	GetQueueCmdLine() [][][]byte
	EnqueueCmd([][]byte)
	ClearQueueCmds()
	GetWatching() map[string]uint32

	// multi database
	GetDBIndex() int
	SelectDB(int)

	// 获取连接的role
	GetRole() int32
	SetRole(int32)
}
