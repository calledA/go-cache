package tcp

import (
	"bufio"
	"math/rand"
	"net"
	"strconv"
	"testing"
	"time"
)

/**
 * @Author: wanglei
 * @File: echo_test
 * @Version: 1.0.0
 * @Description:
 * @Date: 2023/07/12 16:40
 */

func TestListenAndServe(t *testing.T) {
	var err error
	closeChan := make(chan struct{})
	listener, err := net.Listen("tcp", ":0")
	if err != nil {
		t.Error(err)
		return
	}
	addr := listener.Addr().String()
	go ListenAndServe(listener, MakeEchoHandler(), closeChan)

	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Error(err)
		return
	}
	for i := 0; i < 10; i++ {
		val := strconv.Itoa(rand.Int())
		_, err = conn.Write([]byte(val + "\n"))
		if err != nil {
			t.Error(err)
			return
		}
		bufReader := bufio.NewReader(conn)
		line, _, err := bufReader.ReadLine()
		if err != nil {
			t.Error(err)
			return
		}
		if string(line) != val {
			t.Error("get wrong response")
			return
		}
	}
	_ = conn.Close()
	for i := 0; i < 5; i++ {
		// create idle connection
		_, _ = net.Dial("tcp", addr)
	}
	closeChan <- struct{}{}
	time.Sleep(time.Second)
}
