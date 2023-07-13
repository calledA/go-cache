package pubsub

import (
	"container/list"
	"gmr/tiny-redis/interface/redis"
	"gmr/tiny-redis/redis/protocol"
	"strconv"
)

/**
 * @Author: wanglei
 * @File: pubsub
 * @Version: 1.0.0
 * @Description:
 * @Date: 2023/07/10 20:15
 */

var (
	_subscribe        = "subscribe"
	_unsubscribe      = "unsubscribe"
	msgBytes          = []byte("message")
	unSubscribeNotify = []byte("*3\r\n$11\r\nunsubscribe\r\n$-1\n:0\r\n")
)

func makeMsg(msg string, channel string, code int64) []byte {
	return []byte("*3\r\n$" + strconv.FormatInt(int64(len(msg)), 10) + protocol.CRLF + protocol.CRLF +
		"$" + strconv.FormatInt(int64(len(channel)), 10) + protocol.CRLF + channel + protocol.CRLF +
		":" + strconv.FormatInt(code, 10) + protocol.CRLF)
}

func unsubscribe(hub *Hub, channel string, client redis.Connection) bool {
	client.Unsubscribe(channel)

	raw, ok := hub.subs.Get(channel)

	if ok {
		subscribers, _ := raw.(*list.LinkedList)
		// todo: handle
	}
}
