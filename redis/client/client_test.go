package client

import (
	"bytes"
	"gmr/go-cache/lib/logger"
	"gmr/go-cache/redis/protocol"
	"strconv"
	"testing"
	"time"
)

/**
 * @Author: wanglei
 * @File: client_test
 * @Version: 1.0.0
 * @Description:
 * @Date: 2023/07/10 17:27
 */

func TestClient(t *testing.T) {
	client, err := MakeClient("localhost:6379")
	if err != nil {
		logger.Error(err)
	}
	client.Start()

	result := client.Send([][]byte{
		[]byte("PING"),
	})
	if statusRet, ok := result.(*protocol.StatusReply); ok {
		if statusRet.Status != "PONG" {
			logger.Error("`ping` failed, result: " + statusRet.Status)
		}
	}

	result = client.Send([][]byte{
		[]byte("SET"),
		[]byte("a"),
		[]byte("a"),
	})
	if statusRet, ok := result.(*protocol.StatusReply); ok {
		if statusRet.Status != "OK" {
			logger.Error("`set` failed, result: " + statusRet.Status)
		}
	}

	result = client.Send([][]byte{
		[]byte("GET"),
		[]byte("a"),
	})
	if bulkRet, ok := result.(*protocol.BulkReply); ok {
		if string(bulkRet.Arg) != "a" {
			logger.Error("`get` failed, result: " + string(bulkRet.Arg))
		}
	}

	result = client.Send([][]byte{
		[]byte("DEL"),
		[]byte("a"),
	})
	if intRet, ok := result.(*protocol.IntReply); ok {
		if intRet.Code != 1 {
			logger.Error("`del` failed, result: " + strconv.FormatInt(intRet.Code, 10))
		}
	}

	client.doHeartbeat() // random do heartbeat
	result = client.Send([][]byte{
		[]byte("GET"),
		[]byte("a"),
	})
	if _, ok := result.(*protocol.NullBulkReply); !ok {
		logger.Error("`get` failed, result: " + string(result.ToBytes()))
	}

	result = client.Send([][]byte{
		[]byte("DEL"),
		[]byte("arr"),
	})

	result = client.Send([][]byte{
		[]byte("RPUSH"),
		[]byte("arr"),
		[]byte("1"),
		[]byte("2"),
		[]byte("c"),
	})
	if intRet, ok := result.(*protocol.IntReply); ok {
		if intRet.Code != 3 {
			logger.Error("`rpush` failed, result: " + strconv.FormatInt(intRet.Code, 10))
		}
	}

	result = client.Send([][]byte{
		[]byte("LRANGE"),
		[]byte("arr"),
		[]byte("0"),
		[]byte("-1"),
	})
	if multiBulkRet, ok := result.(*protocol.MultiBulkReply); ok {
		if len(multiBulkRet.Args) != 3 ||
			string(multiBulkRet.Args[0]) != "1" ||
			string(multiBulkRet.Args[1]) != "2" ||
			string(multiBulkRet.Args[2]) != "c" {
			logger.Error("`lrange` failed, result: " + string(multiBulkRet.ToBytes()))
		}
	}

	client.Close()
}

func TestReconnect(t *testing.T) {
	client, err := MakeClient("localhost:6379")
	if err != nil {
		logger.Error(err)
	}
	client.Start()

	_ = client.conn.Close()
	time.Sleep(time.Second) // wait for reconnecting
	success := false
	for i := 0; i < 3; i++ {
		result := client.Send([][]byte{
			[]byte("PING"),
		})
		if bytes.Equal(result.ToBytes(), []byte("+PONG\r\n")) {
			success = true
			break
		}
	}
	if !success {
		logger.Error("reconnect error")
	}
}
