package parser

/**
 * @Author: wanglei
 * @File: test
 * @Version: 1.0.0
 * @Description: RESP协议解析文件
 * @Date: 2023/07/05 14:45
 */

import (
	"bufio"
	"bytes"
	"errors"
	"gmr/tiny-redis/interface/redis"
	"gmr/tiny-redis/lib/logger"
	"gmr/tiny-redis/redis/protocol"
	"io"
	"runtime/debug"
	"strconv"
	"strings"
)

type Payload struct {
	Data redis.Reply
	Err  error
}

type readState struct {
	readingMultiLine  bool
	expectedArgsCount int
	msgType           byte
	args              [][]byte // 参数的数组，用byte数组接收
	bulkLen           int64
	readingRepl       bool
}

// ParseStream 通过读取io.Reader并将结果通过 channel 将结果返回给调用者
// 流式处理的接口适合供客户端/服务端使用
func ParseStream(reader io.Reader) <-chan *Payload {
	ch := make(chan *Payload)
	go parse(reader, ch)
	return ch
}

func ParseBytes(data []byte) ([]redis.Reply, error) {
	ch := make(chan *Payload)
	reader := bytes.NewReader(data)
	go parse(reader, ch)

	var results []redis.Reply
	for payload := range ch {
		if payload == nil {
			return nil, errors.New("no protocol")
		}
		if payload.Err != nil {
			if payload.Err == io.EOF {
				break
			}
			return nil, payload.Err
		}
		results = append(results, payload.Data)
	}
	return results, nil
}

func ParseOne(data []byte) (redis.Reply, error) {
	ch := make(chan *Payload)
	reader := bytes.NewReader(data)
	go parse(reader, ch)

	payload := <-ch
	if payload == nil {
		return nil, errors.New("no protocol")
	}
	return payload.Data, payload.Err
}

func (r *readState) finished() bool {
	return r.expectedArgsCount > 0 && len(r.args) == r.expectedArgsCount
}

/**
RESP 通过第一个字符来表示格式:
简单字符串：以"+" 开始， 如："+OK\r\n"
错误：以"-" 开始，如："-ERR Invalid Synatx\r\n"
整数：以":"开始，如：":1\r\n"
字符串：以 $ 开始
数组：以 * 开始
*/
func parse(reader io.Reader, ch chan<- *Payload) {
	defer func() {
		if err := recover(); err != nil {
			logger.Error(err, string(debug.Stack()))
		}
	}()

	bufReader := bufio.NewReader(reader)
	var state readState
	var err error
	var msg []byte

	for {
		//读取单行文本
		var ioErr bool
		//RESP 是以行为单位,readLine()进行数据读取
		msg, ioErr, err = readLine(bufReader, &state)
		if err != nil {
			if ioErr {
				ch <- &Payload{
					Err: err,
				}
				close(ch)
				return
			}
			//protocol 错误，重置readState
			ch <- &Payload{
				Err: err,
			}
			state = readState{}
			continue
		}

		// 解析行
		// Reply 分为两类:
		// 单行: StatusReply, IntReply, ErrorReply
		// 多行: BulkReply, MultiBulkReply
		if !state.readingMultiLine {
			// 收到新的response
			if msg[0] == '*' {
				err = parseMultiBulkHeader(msg, &state)
				if err != nil {
					ch <- &Payload{
						Err: errors.New("protocol error: " + string(msg)),
					}
					state = readState{}
					continue
				}
				if state.expectedArgsCount == 0 {
					ch <- &Payload{
						Data: &protocol.EmptyMultiBulkReply{},
					}
					state = readState{}
					continue
				}
			} else if msg[0] == '$' {
				err = parseBulkHeader(msg, &state)
				if err != nil {
					ch <- &Payload{
						Err: errors.New("protocol error:" + string(msg)),
					}
					state = readState{}
					continue
				}

				if state.bulkLen == -1 {
					ch <- &Payload{
						Data: &protocol.NullBulkReply{},
					}
					state = readState{}
					continue
				}
			} else {
				result, err := parseSingleLineReply(msg)
				ch <- &Payload{
					Data: result,
					Err:  err,
				}
				state = readState{}
				continue
			}
		} else {
			// 收到后续的bulk protocol
			err = readBody(msg, &state)
			if err != nil {
				ch <- &Payload{
					Err: errors.New("origin error: " + string(msg)),
				}
				state = readState{}
				continue
			}

			if state.finished() {
				var result redis.Reply
				if state.msgType == '*' {
					result = protocol.MakeMultiBulkReply(state.args)
				} else if state.msgType == '$' {
					result = protocol.MakeBulkReply(state.args[0])
				}
				ch <- &Payload{
					Data: result,
					Err:  err,
				}
				state = readState{}
			}
		}
	}
}

/**
 * @Description: 数据行中简单字符串和二进制安全的BulkString进行分别读取
 */
func readLine(bufReader *bufio.Reader, state *readState) ([]byte, bool, error) {
	var msg []byte
	var err error

	if state.bulkLen == 0 {
		//读行数据
		msg, err = bufReader.ReadBytes('\n')
		if err != nil { // io错误，返回错误
			return nil, true, err
		}

		if len(msg) == 0 || msg[len(msg)-2] != '\r' {
			return nil, false, errors.New("protocol error: " + string(msg))
		}
	} else {
		// 读取bulk line（二进制安全）
		// 在正常流中BulkReply之间存在CRLF
		// 在RDB和AOF中没有CRLF
		bulkLen := state.bulkLen + 2
		if state.readingRepl {
			bulkLen -= 2
		}

		msg = make([]byte, bulkLen)
		_, err := io.ReadFull(bufReader, msg)
		if err != nil {
			return nil, true, err
		}
		state.bulkLen = 0
	}
	return msg, false, nil
}

func parseMultiBulkHeader(msg []byte, state *readState) error {
	var err error
	var expectedLine uint64
	expectedLine, err = strconv.ParseUint(string(msg[1:len(msg)-2]), 10, 32)
	if err != nil {
		return errors.New("protocol error:" + string(msg))
	}
	if expectedLine == 0 {
		state.expectedArgsCount = 0
		return nil
	} else if expectedLine > 0 {
		state.msgType = msg[0]
		state.readingMultiLine = true
		state.expectedArgsCount = int(expectedLine)
		state.args = make([][]byte, 0, expectedLine)
		return nil
	} else {
		return errors.New("protocol error:" + string(msg))
	}
}

func parseBulkHeader(msg []byte, state *readState) error {
	var err error
	state.bulkLen, err = strconv.ParseInt(string(msg[1:len(msg)-2]), 10, 64)
	if err != nil {
		return errors.New("protocol error:" + string(msg))
	}

	if state.bulkLen == -1 {
		return nil
	} else if state.bulkLen > 0 {
		state.msgType = msg[0]
		state.readingMultiLine = true
		state.expectedArgsCount = 1
		state.args = make([][]byte, 0, 1)
		return nil
	} else {
		return errors.New("protocol error:" + string(msg))
	}
}

func parseSingleLineReply(msg []byte) (redis.Reply, error) {
	str := strings.TrimSuffix(string(msg), "\r\n")
	var result redis.Reply
	switch msg[0] {
	case '+':
		result = protocol.MakeStatusReply(str[1:])
	case '-':
		result = protocol.MakeErrorReply(str[1:])
	case ':':
		val, err := strconv.ParseInt(str[1:], 10, 64)
		if err != nil {
			return nil, errors.New("protocol error:" + string(msg))
		}
		result = protocol.MakeIntReply(val)
	default:
		strs := strings.Split(str, " ")
		args := make([][]byte, len(strs))
		for i, s := range strs {
			args[i] = []byte(s)
		}
		result = protocol.MakeMultiBulkReply(args)
	}
	return result, nil
}

// 读取后续的bulk协议数据
func readBody(msg []byte, state *readState) error {
	line := msg[0 : len(msg)-2]
	var err error
	if msg[0] == '$' {
		state.bulkLen, err = strconv.ParseInt(string(line[1:]), 10, 64)
		if err != nil {
			return errors.New("protocol error:" + string(msg))
		}

		if state.bulkLen <= 0 {
			state.args = append(state.args, []byte{})
			state.bulkLen = 0
		}
	} else {
		state.args = append(state.args, line)
	}
	return nil
}
