package database

import (
	"gmr/go-cache/config"
	"gmr/go-cache/interface/redis"
	"gmr/go-cache/redis/protocol"
)

/**
 * @Author: wanglei
 * @File: sys
 * @Version: 1.0.0
 * @Description:
 * @Date: 2023/08/07 16:07
 */

func Ping(db *DB, args [][]byte) redis.Reply {
	if len(args) == 0 {
		return &protocol.PongReply{}
	} else if len(args) == 1 {
		return protocol.MakeStatusReply(string(args[0]))
	} else {
		return protocol.MakeErrorReply("ERR wrong number of arguments for 'ping' command")
	}
}

func Auth(c redis.Connection, args [][]byte) redis.Reply {
	if len(args) != 1 {
		return protocol.MakeErrorReply("ERR wrong number of arguments for 'auth' command")
	}

	if config.Properties.RequirePass == "" {
		return protocol.MakeErrorReply("ERR Client sent AUTH, but no password is set")
	}

	pwd := string(args[0])
	c.SetPassword(pwd)
	if config.Properties.RequirePass != pwd {
		return protocol.MakeErrorReply("ERR invalid password")
	}
	return &protocol.OkReply{}
}

func isAuthenticated(c redis.Connection) bool {
	if config.Properties.RequirePass == "" {
		return true
	}
	return c.GetPassword() == config.Properties.RequirePass
}

func init() {
	RegisterCommand("ping", Ping, noPrepare, nil, -1, flagReadOnly)
}
