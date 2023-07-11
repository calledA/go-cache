package redis

//redis协议格式消息接口
type Reply interface {
	ToBytes() []byte
}
