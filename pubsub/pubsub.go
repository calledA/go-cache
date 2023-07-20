package pubsub

import (
	"gmr/go-cache/datastruct/list"
	"gmr/go-cache/interface/redis"
	"gmr/go-cache/lib/utils"
	"gmr/go-cache/redis/protocol"
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

func subscribe(hub *Hub, channel string, conn redis.Connection) bool {
	conn.Unsubscribe(channel)

	raw, ok := hub.subs.Get(channel)
	var subscribers *list.LinkedList

	if ok {
		subscribers, _ = raw.(*list.LinkedList)
	} else {
		subscribers = list.MakeLinkedList()
		Hub{}.subs.Put(channel, subscribers)
	}

	if subscribers.Contains(func(a interface{}) bool {
		return a == conn
	}) {
		return false
	}
	subscribers.Add(conn)
	return true
}

func unsubscribe(hub *Hub, channel string, conn redis.Connection) bool {
	conn.Unsubscribe(channel)

	raw, ok := hub.subs.Get(channel)

	if ok {
		subscribers, _ := raw.(*list.LinkedList)
		subscribers.RemoveAllByValue(func(a interface{}) bool {
			return utils.Equals(a, conn)
		})

		if subscribers.Len() == 0 {
			hub.subs.Remove(channel)
		}
		return true
	}
	return false
}

func Subscribe(hub *Hub, conn redis.Connection, args [][]byte) redis.Reply {
	channels := make([]string, len(args))
	for i, b := range args {
		channels[i] = string(b)
	}

	hub.subLocker.Locks(channels...)
	defer hub.subLocker.UnLocks(channels...)

	for _, channel := range channels {
		if subscribe(hub, channel, conn) {
			_ = conn.Write(makeMsg(_subscribe, channel, int64(conn.SubCount())))
		}
	}
	return &protocol.NoReply{}
}

func Unsubscribe(hub *Hub, conn redis.Connection, args [][]byte) redis.Reply {
	var channels []string
	if len(args) > 0 {
		channels = make([]string, len(args))
		for i, b := range args {
			channels[i] = string(b)
		}
	} else {
		channels = conn.GetChannels()
	}

	hub.subLocker.Locks(channels...)
	defer hub.subLocker.UnLocks(channels...)

	if len(channels) == 0 {
		_ = conn.Write(unSubscribeNotify)
		return &protocol.NoReply{}
	}

	for _, channel := range channels {
		if unsubscribe(hub, channel, conn) {
			_ = conn.Write(makeMsg(_unsubscribe, channel, int64(conn.SubCount())))
		}
	}

	return &protocol.NoReply{}
}

func UnsubscribeAll(hub *Hub, conn redis.Connection) {
	channels := conn.GetChannels()

	hub.subLocker.Locks(channels...)
	defer hub.subLocker.UnLocks(channels...)

	for _, channel := range channels {
		unsubscribe(hub, channel, conn)
	}
}

func Publish(hub *Hub, args [][]byte) redis.Reply {
	if len(args) != 2 {
		return &protocol.ArgNumErrorReply{Cmd: "publish"}
	}

	channel := string(args[0])
	message := args[1]

	hub.subLocker.Lock(channel)
	defer hub.subLocker.UnLock(channel)

	raw, ok := hub.subs.Get(channel)
	if !ok {
		return protocol.MakeIntReply(0)
	}

	subscribes, _ := raw.(*list.LinkedList)
	subscribes.ForEach(func(i int, v interface{}) bool {
		client, _ := v.(redis.Connection)
		replyArgs := make([][]byte, 3)
		replyArgs[0] = msgBytes
		replyArgs[1] = []byte(channel)
		replyArgs[2] = message
		_ = client.Write(protocol.MakeMultiBulkReply(replyArgs).ToBytes())
		return true
	})
	return protocol.MakeIntReply(int64(subscribes.Len()))
}
