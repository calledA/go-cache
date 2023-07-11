package aof

import (
	"gmr/tiny-redis/interface/database"
	"os"
	"sync"
)

/**
 * @Author: wanglei
 * @File: aof
 * @Version: 1.0.0
 * @Description:
 * @Date: 2023/07/10 20:08
 */

// 命令行命令
type CmdLine = [][]byte

const (
	aofQueueSize = 1 << 16
)

type payload struct {
	cmdLine CmdLine
	dbIndex int
}

// Handler接收channel数据，写入到AOF file
type Handler struct {
	db          database.EmbedDB
	tmpDBMaker  func() database.EmbedDB
	aofChan     chan *payload
	aofFile     *os.File
	aofFilename string
	// aof协程向主协程通信的channel
	aofFinished chan struct{}
	// 暂停aof以启动/完成aof重写进度
	pausingAof sync.RWMutex
	currentDB  int
}

// todo aof实现方法
