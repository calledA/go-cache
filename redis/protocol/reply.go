package protocol

/**
 * @Author: wanglei
 * @File: test
 * @Version: 1.0.0
 * @Description: RESP协议解析文件
 * @Date: 2023/07/05 12:14
 */

import (
	"bytes"
	"gmr/tiny-redis/interface/redis"
	"strconv"
)

var (
	nullBulkReplyBytes = []byte("$-1")
	//序列化协议分隔符
	CRLF = "\r\n"
)

/*  Bulk reply  */

type BulkReply struct {
	Arg []byte
}

func MakeBulkReply(arg []byte) *BulkReply {
	return &BulkReply{
		Arg: arg,
	}
}

func (r *BulkReply) ToBytes() []byte {
	if len(r.Arg) == 0 {
		return nullBulkReplyBytes
	}
	return []byte("$" + strconv.Itoa(len(r.Arg)) + CRLF + string(r.Arg) + CRLF)
}

/*  MultiBulk reply  */

type MultiBulkReply struct {
	Args [][]byte
}

func MakeMultiBulkReply(args [][]byte) *MultiBulkReply {
	return &MultiBulkReply{
		Args: args,
	}
}

func (r *MultiBulkReply) ToBytes() []byte {
	argLen := len(r.Args)
	var buf bytes.Buffer
	buf.WriteString("*" + strconv.Itoa(argLen) + CRLF)

	for _, arg := range r.Args {
		if arg == nil {
			buf.WriteString("$-1" + CRLF)
		} else {
			buf.WriteString("$" + strconv.Itoa(len(arg)) + CRLF + string(arg) + CRLF)
		}
	}
	return buf.Bytes()
}

/*  MultiRaw reply  */

type MultiRawReply struct {
	Replies []redis.Reply
}

func MakeMultiRawReply(replies []redis.Reply) *MultiRawReply {
	return &MultiRawReply{
		Replies: replies,
	}
}

func (r *MultiRawReply) ToBytes() []byte {
	argLen := len(r.Replies)
	var buf bytes.Buffer
	buf.WriteString("*" + strconv.Itoa(argLen) + CRLF)
	for _, arg := range r.Replies {
		buf.Write(arg.ToBytes())
	}
	return buf.Bytes()
}

/*  Status reply  */

//status类型
type StatusReply struct {
	Status string
}

func MakeStatusReply(status string) *StatusReply {
	return &StatusReply{
		Status: status,
	}
}

func (r *StatusReply) ToBytes() []byte {
	return []byte("+" + r.Status + CRLF)
}

/*  Int reply  */

type IntReply struct {
	Code int64
}

func MakeIntReply(code int64) *IntReply {
	return &IntReply{
		Code: code,
	}
}

func (r *IntReply) ToBytes() []byte {
	return []byte(":" + strconv.FormatInt(r.Code, 10) + CRLF)
}

/*  Error reply  */

//在redis.Reply基础上加了错误方法
type ErrorReply interface {
	Error() string
	ToBytes() []byte
}

//返回服务错误
type StandardErrorReply struct {
	Status string
}

//生成标准错误
func MakeErrorReply(status string) *StandardErrorReply {
	return &StandardErrorReply{
		Status: status,
	}
}

//判断协议是否有错误
func IsErrorReply(reply redis.Reply) bool {
	return reply.ToBytes()[0] == '-'
}

func (r *StandardErrorReply) ToBytes() []byte {
	return []byte("-" + r.Status + CRLF)
}

func (r *StandardErrorReply) Error() string {
	return r.Status
}
