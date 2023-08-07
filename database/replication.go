package database

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	rdb "github.com/hdt3213/rdb/parser"
	"gmr/go-cache/config"
	"gmr/go-cache/interface/redis"
	"gmr/go-cache/lib/logger"
	"gmr/go-cache/lib/utils"
	"gmr/go-cache/redis/connection"
	"gmr/go-cache/redis/parser"
	"gmr/go-cache/redis/protocol"
	"net"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

/**
 * @Author: wanglei
 * @File: replication
 * @Version: 1.0.0
 * @Description:
 * @Date: 2023/07/26 11:37
 */
const (
	masterRole = iota
	slaveRole
)

type replicationStatus struct {
	mutex    sync.Mutex
	ctx      context.Context
	cancel   context.CancelFunc
	modCount int32

	masterHost string
	masterPort int

	masterConn   net.Conn
	masterChan   <-chan *parser.Payload
	replId       string
	replOffset   int64
	lastRecvtime time.Time
	running      sync.WaitGroup
}

func initReplStatus() *replicationStatus {
	return &replicationStatus{}
}

func (mdb *MultiDB) startReplCron() {
	go func() {
		defer func() {
			if err := recover(); err != nil {
				logger.Error(err)
			}
		}()

		ticker := time.Tick(time.Second)
		for range ticker {
			mdb.slaveCron()
		}
	}()
}

func (mdb *MultiDB) execSlaveOf(c redis.Connection, args [][]byte) redis.Reply {
	if strings.ToLower(string(args[0])) == "no" && strings.ToLower(string(args[1])) == "one" {
		mdb.slaveOfNone()
		return protocol.MakeOkReply()
	}
	host := string(args[0])
	port, err := strconv.Atoi(string(args[1]))
	if err != nil {
		return protocol.MakeErrorReply("ERR value is not an integer or out of range")
	}

	mdb.replication.mutex.Lock()
	atomic.StoreInt32(&mdb.role, slaveRole)
	mdb.replication.masterHost = host
	mdb.replication.masterPort = port

	atomic.AddInt32(&mdb.replication.modCount, 1)
	mdb.replication.mutex.Unlock()
	go mdb.syncWithMaster()
	return protocol.MakeOkReply()
}

func (mdb *MultiDB) slaveOfNone() {
	mdb.replication.mutex.Lock()
	defer mdb.replication.mutex.Unlock()

	mdb.replication.masterHost = ""
	mdb.replication.masterPort = 0
	mdb.replication.replId = ""
	mdb.replication.replOffset = -1
	mdb.replication.stopSlaveWithMutex()
}

func (repl *replicationStatus) stopSlaveWithMutex() {
	atomic.AddInt32(&repl.modCount, 1)
	if repl.cancel != nil {
		repl.cancel()
		repl.running.Wait()
	}

	repl.ctx = context.Background()
	repl.cancel = nil

	if repl.masterConn != nil {
		_ = repl.masterConn.Close()
	}
	repl.masterConn = nil
	repl.masterChan = nil
}

func (repl *replicationStatus) close() error {
	repl.mutex.Lock()
	defer repl.mutex.Unlock()
	repl.stopSlaveWithMutex()
	return nil
}

func (mdb *MultiDB) syncWithMaster() {
	defer func() {
		if err := recover(); err != nil {
			logger.Error(err)
		}
	}()

	mdb.replication.mutex.Lock()
	ctx, cancel := context.WithCancel(context.Background())
	mdb.replication.ctx = ctx
	mdb.replication.cancel = cancel
	mdb.replication.mutex.Unlock()

	if err := mdb.connectWithMaster(); err != nil {
		return
	}

	if err := mdb.doPsync(); err != nil {
		return
	}

	if err := mdb.receiveAOF(); err != nil {
		return
	}
}

func (mdb *MultiDB) connectWithMaster() error {
	modCount := atomic.LoadInt32(&mdb.replication.modCount)
	addr := mdb.replication.masterHost + ":" + strconv.Itoa(mdb.replication.masterPort)
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		mdb.slaveOfNone()
		return errors.New("connect master failed" + err.Error())
	}

	masterChan := parser.ParseStream(conn)

	pingCmdLine := utils.ToCmdLine("ping")
	pingReq := protocol.MakeMultiBulkReply(pingCmdLine)
	_, err = conn.Write(pingReq.ToBytes())
	if err != nil {
		return errors.New("send ping failed" + err.Error())
	}

	pingResp := <-masterChan
	if pingResp.Err != nil {
		return errors.New("read response failed" + pingResp.Err.Error())
	}

	switch reply := pingResp.Data.(type) {
	case *protocol.StandardErrorReply:
		if !strings.HasPrefix(reply.Error(), "NOAUTH") &&
			!strings.HasPrefix(reply.Error(), "NOPERM") &&
			!strings.HasPrefix(reply.Error(), "ERR operation not permitted") {
			logger.Error("Error reply to PING from master: " + string(reply.ToBytes()))
			mdb.slaveOfNone() // abort
			return nil
		}
	}

	sendCmdToMaster := func(conn net.Conn, cmdLine CmdLine, masterChan <-chan *parser.Payload) error {
		req := protocol.MakeMultiBulkReply(cmdLine)
		_, err := conn.Write(req.ToBytes())
		if err != nil {
			mdb.slaveOfNone()
			return errors.New("send failed" + err.Error())
		}

		resp := <-masterChan
		if resp.Err != nil {
			mdb.slaveOfNone()
			return errors.New("read response failed:" + resp.Err.Error())
		}

		if !protocol.IsOKReply(resp.Data) {
			mdb.slaveOfNone()
			return errors.New("unexpected auth response: " + string(resp.Data.ToBytes()))
		}
		return nil
	}

	if config.Properties.MasterAuth != "" {
		authCmdLine := utils.ToCmdLine("auth", config.Properties.MasterAuth)
		err = sendCmdToMaster(conn, authCmdLine, masterChan)
		if err != nil {
			return err
		}
	}

	var port int
	if config.Properties.SlaveAnnouncePort != 0 {
		port = config.Properties.SlaveAnnouncePort
	} else {
		port = config.Properties.Port
	}
	portCmdLine := utils.ToCmdLine("REPLCONF", "listening-port", strconv.Itoa(port))
	err = sendCmdToMaster(conn, portCmdLine, masterChan)
	if err != nil {
		return err
	}

	if config.Properties.SlaveAnnounceIP != "" {
		ipCmdLine := utils.ToCmdLine("REPLCONF", "ip-address", config.Properties.SlaveAnnounceIP)
		err = sendCmdToMaster(conn, ipCmdLine, masterChan)
		if err != nil {
			return err
		}
	}

	capaCmdLine := utils.ToCmdLine("REPLCONF", "capa", "psync2")
	err = sendCmdToMaster(conn, capaCmdLine, masterChan)
	if err != nil {
		return err
	}

	mdb.replication.mutex.Lock()
	if mdb.replication.modCount != modCount {
		return nil
	}

	mdb.replication.masterConn = conn
	mdb.replication.masterChan = masterChan
	mdb.replication.mutex.Unlock()
	return nil
}

func (mdb *MultiDB) doPsync() error {
	modCount := atomic.LoadInt32(&mdb.replication.modCount)
	psyncCmdLine := utils.ToCmdLine("psync", "?", "-1")
	psyncRep := protocol.MakeMultiBulkReply(psyncCmdLine)
	_, err := mdb.replication.masterConn.Write(psyncRep.ToBytes())
	if err != nil {
		return errors.New("send failed " + err.Error())
	}
	ch := mdb.replication.masterChan
	psyncPayload1 := <-ch
	if psyncPayload1.Err == nil {
		return errors.New("read response failed: " + psyncPayload1.Err.Error())
	}

	psyncHeader, ok := psyncPayload1.Data.(*protocol.StatusReply)
	if !ok {
		return errors.New("illegal payload header: " + string(psyncPayload1.Data.ToBytes()))
	}
	headers := strings.Split(psyncHeader.Status, " ")
	if len(headers) != 3 {
		return errors.New("illegal payload header: " + psyncHeader.Status)
	}

	logger.Info("receive psync header from master")

	psyncPayload2 := <-ch
	if psyncPayload2.Err != nil {
		return errors.New("read response failed: " + psyncPayload2.Err.Error())
	}
	psyncData, ok := psyncPayload2.Data.(*protocol.BulkReply)
	if !ok {
		return errors.New("illegal payload header: " + string(psyncPayload2.Data.ToBytes()))
	}
	logger.Info(fmt.Sprintf("receive %d bytes of rdb from master", len(psyncData.Arg)))

	rdbDec := rdb.NewDecoder(bytes.NewReader(psyncData.Arg))
	rdbHolder := MakeBasicMultiDB()
	err = dumpRDB(rdbDec, rdbHolder)
	if err != nil {
		return errors.New("dump rdb failed: " + err.Error())
	}

	mdb.replication.mutex.Lock()
	defer mdb.replication.mutex.Unlock()
	if mdb.replication.modCount != modCount {
		// replication conf changed during connecting and waiting mutex
		return nil
	}
	mdb.replication.replId = headers[1]
	mdb.replication.replOffset, err = strconv.ParseInt(headers[2], 10, 64)
	if err != nil {
		return errors.New("get illegal repl offset: " + headers[2])
	}
	logger.Info("full resync from master: " + mdb.replication.replId)
	logger.Info("current offset:", mdb.replication.replOffset)
	for i, h := range rdbHolder.dbSet {
		newDB := h.Load().(*DB)
		mdb.loadDB(i, newDB)
	}
	// there is no CRLF between RDB and following AOF, reset stream to avoid parser error
	mdb.replication.masterChan = parser.ParseStream(mdb.replication.masterConn)
	// fixme: update aof file
	return nil
}

func (mdb *MultiDB) receiveAOF() error {
	conn := connection.NewConnection(mdb.replication.masterConn)
	conn.SetRole(connection.ReplicationRecvCli)
	mdb.replication.mutex.Lock()
	modCount := mdb.replication.modCount
	done := mdb.replication.ctx.Done()
	mdb.replication.mutex.Unlock()
	if done == nil {
		return nil
	}
	mdb.replication.running.Add(1)
	defer mdb.replication.running.Done()
	for {
		select {
		case payload, open := <-mdb.replication.masterChan:
			if !open {
				return errors.New("master channel unexpected close")
			}

			if payload.Err != nil {
				return payload.Err
			}

			cmdLine, ok := payload.Data.(*protocol.MultiBulkReply)
			if !ok {
				return errors.New("unexpected payload: " + string(payload.Data.ToBytes()))
			}

			mdb.replication.mutex.Lock()
			if mdb.replication.modCount != modCount {
				return nil
			}

			mdb.Exec(conn, cmdLine.Args)
			n := len(cmdLine.ToBytes())

			mdb.replication.replOffset += int64(n)
			logger.Info(fmt.Sprintf("receive %d bytes from master, current offset %d",
				n, mdb.replication.replOffset))
			mdb.replication.mutex.Unlock()

		case <-done:
			return nil
		}
	}
}

func (mdb *MultiDB) slaveCron() {
	repl := mdb.replication
	if repl.masterConn == nil {
		return
	}

	replTimeout := 60 * time.Second
	if config.Properties.ReplTimeout != 0 {
		replTimeout = time.Duration(config.Properties.ReplTimeout) * time.Second
	}
	minLastRecvTime := time.Now().Add(-replTimeout)
	if repl.lastRecvtime.Before(minLastRecvTime) {
		err := mdb.reconnectWithMaster()
		if err != nil {
			logger.Error("send failed " + err.Error())
		}
		return
	}
	err := repl.sendAck2Master()
	if err != nil {
		logger.Error("send failed " + err.Error())
	}
}

func (mdb *MultiDB) reconnectWithMaster() error {
	logger.Info("reconnecting with master")
	mdb.replication.mutex.Lock()
	defer mdb.replication.mutex.Unlock()
	mdb.replication.stopSlaveWithMutex()
	go mdb.syncWithMaster()
	return nil
}

func (repl *replicationStatus) sendAck2Master() error {
	psyncCmdLine := utils.ToCmdLine("REPLCONF", "ACK", strconv.FormatInt(repl.replOffset, 10))
	psyncReq := protocol.MakeMultiBulkReply(psyncCmdLine)
	_, err := repl.masterConn.Write(psyncReq.ToBytes())
	return err
}
