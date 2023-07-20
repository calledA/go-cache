package parser

import (
	"bytes"
	"gmr/go-cache/interface/redis"
	"gmr/go-cache/lib/utils"
	"gmr/go-cache/redis/protocol"
	"io"
	"testing"
)

/**
 * @Author: wanglei
 * @File: parser_test
 * @Version: 1.0.0
 * @Description: parser测试
 * @Date: 2023/07/05 17:08
 */

func TestParseStream(t *testing.T) {
	replies := []redis.Reply{
		protocol.MakeIntReply(1),
		protocol.MakeStatusReply("OK"),
		protocol.MakeErrorReply("ERR unknown"),
		protocol.MakeBulkReply([]byte("a\r\nb")), // test binary safe
		protocol.MakeNullBulkReply(),
		protocol.MakeMultiBulkReply([][]byte{
			[]byte("a"),
			[]byte("\r\n"),
		}),
		protocol.MakeEmptyMultiBulkReply(),
	}

	reqs := bytes.Buffer{}

	for _, reply := range replies {
		reqs.Write(reply.ToBytes())
	}

	reqs.Write([]byte("set a a" + protocol.CRLF))
	expected := make([]redis.Reply, len(replies))
	copy(expected, replies)
	expected = append(expected, protocol.MakeMultiBulkReply([][]byte{
		[]byte("set"),
		[]byte("a"),
		[]byte("a"),
	}))

	ch := ParseStream(bytes.NewReader(reqs.Bytes()))
	i := 0
	for payload := range ch {
		if payload.Err != nil {
			if payload.Err == io.EOF {
				return
			}
			t.Error(payload.Err)
			return
		}
		if payload.Data == nil {
			t.Error("empty data")
			return
		}
		exp := expected[i]
		i++
		if !utils.BytesEquals(exp.ToBytes(), payload.Data.ToBytes()) {
			t.Error("parse failed:" + string(exp.ToBytes()))
		}
	}
}
