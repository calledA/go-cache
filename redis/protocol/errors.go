package protocol

/**
 * @Author: wanglei
 * @File: errors
 * @Version: 1.0.0
 * @Description: 错误参数响应内容
 * @Date: 2023/07/06 17:56
 */

var (
	theSyntaxErrorReply = new(SyntaxErrorReply)
	unknownErrorBytes   = []byte("-Err unknown\r\r")
	syntaxErrorBytes    = []byte("-Err syntax error\r\n")
	wrongTypeErrorBytes = []byte("-WRONGTYPE Operation against a key holding the wrong kind of value\r\n")
)

// 未知的错误
type UnknownErrorReply struct{}

func (r *UnknownErrorReply) ToBytes() []byte {
	return unknownErrorBytes
}

func (r *UnknownErrorReply) Error() string {
	return "Err unknown"
}

// 参数数量不对
type ArgNumErrorReply struct {
	Cmd string
}

func (r *ArgNumErrorReply) ToBytes() []byte {
	return []byte("-Err wrong number of arguments for '" + r.Cmd + "' command\r\n")
}

func (r *ArgNumErrorReply) Error() string {
	return "ERR wrong number of arguments for '" + r.Cmd + "' command"
}

func MakeArgNumErrorReply(cmd string) *ArgNumErrorReply {
	return &ArgNumErrorReply{Cmd: cmd}
}

// 遇到非期望的参数
type SyntaxErrorReply struct{}

func (r *SyntaxErrorReply) ToBytes() []byte {
	return syntaxErrorBytes
}

func (r *SyntaxErrorReply) Error() string {
	return "Err syntax error"
}

func MakeSyntaxErrorReply() *SyntaxErrorReply {
	return theSyntaxErrorReply
}

// 表示对错误类型值的键的操作
type WrongTypeErrorReply struct{}

func (w *WrongTypeErrorReply) ToBytes() []byte {
	return wrongTypeErrorBytes
}

func (r *WrongTypeErrorReply) Error() string {
	return "WRONGTYPE Operation against a key holding the wrong kind of value"
}

// 解析协议时遇到非期望的字节
type ProtocolErrorReply struct {
	Msg string
}

func (r *ProtocolErrorReply) ToBytes() []byte {
	return []byte("-ERR Protocol error: '" + r.Msg + "'\r\n")
}

func (r *ProtocolErrorReply) Error() string {
	return "ERR Protocol error: '" + r.Msg
}
