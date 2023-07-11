package protocol

/**
 * @Author: wanglei
 * @File: test
 * @Version: 1.0.0
 * @Description: RESP协议解析文件
 * @Date: 2023/07/05 11:45
 */

var (
	theOkReply          = new(OkReply)
	theQueuedReply      = new(QueuedReply)
	noBytes             = []byte("")
	emptyMultiBulkBytes = []byte("*0\r\n")
	pongBytes           = []byte("+PONG\r\n")
	okBytes             = []byte("+OK\r\n")
	nullBulkBytes       = []byte("$-1\r\n")
	queuedBytes         = []byte("+QUEUE\r\r")
)

// 相应PONG
type PongReply struct{}

func (r *PongReply) ToBytes() []byte {
	return pongBytes
}

// 相应OK
type OkReply struct{}

func (r *OkReply) ToBytes() []byte {
	return okBytes
}

func MakeOkReply() *OkReply {
	return theOkReply
}

// 相应空内容
type EmptyMultiBulkReply struct{}

func (r *EmptyMultiBulkReply) ToBytes() []byte {
	return emptyMultiBulkBytes
}

func MakeEmptyMultiBulkReply() *EmptyMultiBulkReply {
	return &EmptyMultiBulkReply{}
}

type NullBulkReply struct{}

func (r *NullBulkReply) ToBytes() []byte {
	return nullBulkBytes
}

func MakeNullBulkReply() *NullBulkReply {
	return &NullBulkReply{}
}

// 对subscribe之类的命令不响应
type NoReply struct{}

func (r *NoReply) ToBytes() []byte {
	return noBytes
}

type QueuedReply struct{}

func (r *QueuedReply) ToBytes() []byte {
	return queuedBytes
}

// 创建theQueuedReply
func MakeQueuedReply() *QueuedReply {
	return theQueuedReply
}
