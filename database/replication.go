package database

import (
	"context"
	"errors"
	"gmr/go-cache/config"
	"gmr/go-cache/interface/redis"
	"gmr/go-cache/lib/logger"
	"gmr/go-cache/lib/utils"
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

func (mdb *MultiDB) slaveOfNone {
	mdb.replication.mutex.Lock()
	defer mdb.replication.mutex.Unlock()

	mdb.replication.masterHost = ""
	mdb.replication.masterPort = 0
	mdb.replication.replId = ""
	mdb.replication.replOffset = -1
	mdb.replication.stopSlaveWithMutex()
}

func (repl *replicationStatus) stopSlaveWithMutex()  {
	atomic.AddInt32(&repl.modCount,1)
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
		if err := recover();err != nil {
			logger.Error(err)
		}
	}()

	mdb.replication.mutex.Lock()
	ctx, cancel := context.WithCancel(context.Background())
	mdb.replication.ctx = ctx
	mdb.replication.cancel = cancel
	mdb.replication.mutex.Unlock()

	if err := mdb.connectWithMaster();err != nil {
		return
	}

	if err := mdb.doPsync();err != nil {
		return
	}

	if err := mdb.receiveAOF();err != nil {
		return
	}
}

func (mdb *MultiDB) connectWithMaster() error {
	modCount := atomic.LoadInt32(&mdb.replication.modCount)
	addr := mdb.replication.masterHost + ":" + strconv.Itoa(mdb.replication.masterPort)
	conn,err := net.Dial("tcp",addr)
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
			!strings.HasPrefix(reply.Error(),"NOPERM") &&
			!strings.HasPrefix(reply.Error(),"ERR operation not permitted"){
			logger.Error("Error reply to PING from master: " + string(reply.ToBytes()))
			mdb.slaveOfNone() // abort
			return nil
		}
	}

	sendCmdToMaster := func(conn net.Conn,cmdLine CmdLine,masterChan <-chan *parser.Payload) error {
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

	capaCmdLine := utils.ToCmdLine("REPLCONF","capa","psync2")
	err = sendCmdToMaster(conn,capaCmdLine,masterChan)
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