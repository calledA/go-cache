package aof

import (
	"gmr/go-cache/config"
	"gmr/go-cache/interface/database"
	"gmr/go-cache/lib/logger"
	"gmr/go-cache/lib/utils"
	"gmr/go-cache/redis/protocol"
	"io"
	"io/ioutil"
	"os"
	"strconv"
	"time"
)

/**
 * @Author: wanglei
 * @File: rewrite
 * @Version: 1.0.0
 * @Description:
 * @Date: 2023/08/22 9:39
 */

func (handler *Handler) newRewriteHandler() *Handler {
	h := &Handler{}
	h.aofFilename = handler.aofFilename
	h.db = handler.tmpDBMaker()
	return h
}

// RewriteCtx holds context of an AOF rewriting procedure
type RewriteCtx struct {
	tmpFile  *os.File
	fileSize int64
	dbIdx    int // selected db index when startRewrite
}

// Rewrite carries out AOF rewrite
func (handler *Handler) Rewrite() error {
	ctx, err := handler.StartRewrite()
	if err != nil {
		return err
	}
	err = handler.DoRewrite(ctx)
	if err != nil {
		return err
	}

	handler.FinishRewrite(ctx)
	return nil
}

// DoRewrite actually rewrite aof file
// makes DoRewrite public for testing only, please use Rewrite instead
func (handler *Handler) DoRewrite(ctx *RewriteCtx) error {
	tmpFile := ctx.tmpFile

	// load aof tmpFile
	tmpAof := handler.newRewriteHandler()
	tmpAof.LoadAof(int(ctx.fileSize))

	// rewrite aof tmpFile
	for i := 0; i < config.Properties.Databases; i++ {
		// select db
		data := protocol.MakeMultiBulkReply(utils.ToCmdLine("SELECT", strconv.Itoa(i))).ToBytes()
		_, err := tmpFile.Write(data)
		if err != nil {
			return err
		}
		// dump db
		tmpAof.db.ForEach(i, func(key string, entity *database.DataEntity, expiration *time.Time) bool {
			cmd := EntityToCmd(key, entity)
			if cmd != nil {
				_, _ = tmpFile.Write(cmd.ToBytes())
			}
			if expiration != nil {
				cmd := MakeExpireCmd(key, *expiration)
				if cmd != nil {
					_, _ = tmpFile.Write(cmd.ToBytes())
				}
			}
			return true
		})
	}
	return nil
}

// StartRewrite prepares rewrite procedure
func (handler *Handler) StartRewrite() (*RewriteCtx, error) {
	handler.pausingAof.Lock() // pausing aof
	defer handler.pausingAof.Unlock()

	err := handler.aofFile.Sync()
	if err != nil {
		logger.Warn("fsync failed")
		return nil, err
	}

	// get current aof file size
	fileInfo, _ := os.Stat(handler.aofFilename)
	filesize := fileInfo.Size()

	// create tmp file
	file, err := ioutil.TempFile("", "*.aof")
	if err != nil {
		logger.Warn("tmp file create failed")
		return nil, err
	}
	return &RewriteCtx{
		tmpFile:  file,
		fileSize: filesize,
		dbIdx:    handler.currentDB,
	}, nil
}

// FinishRewrite finish rewrite procedure
func (handler *Handler) FinishRewrite(ctx *RewriteCtx) {
	handler.pausingAof.Lock() // pausing aof
	defer handler.pausingAof.Unlock()

	tmpFile := ctx.tmpFile
	// write commands executed during rewriting to tmp file
	src, err := os.Open(handler.aofFilename)
	if err != nil {
		logger.Error("open aofFilename failed: " + err.Error())
		return
	}
	defer func() {
		_ = src.Close()
	}()
	_, err = src.Seek(ctx.fileSize, 0)
	if err != nil {
		logger.Error("seek failed: " + err.Error())
		return
	}

	// sync tmpFile's db index with online aofFile
	data := protocol.MakeMultiBulkReply(utils.ToCmdLine("SELECT", strconv.Itoa(ctx.dbIdx))).ToBytes()
	_, err = tmpFile.Write(data)
	if err != nil {
		logger.Error("tmp file rewrite failed: " + err.Error())
		return
	}
	// copy data
	_, err = io.Copy(tmpFile, src)
	if err != nil {
		logger.Error("copy aof filed failed: " + err.Error())
		return
	}

	// replace current aof file by tmp file
	_ = handler.aofFile.Close()
	_ = os.Rename(tmpFile.Name(), handler.aofFilename)

	// reopen aof file for further write
	aofFile, err := os.OpenFile(handler.aofFilename, os.O_APPEND|os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		panic(err)
	}
	handler.aofFile = aofFile

	// write select command again to ensure aof file has the same db index with  handler.currentDB
	data = protocol.MakeMultiBulkReply(utils.ToCmdLine("SELECT", strconv.Itoa(handler.currentDB))).ToBytes()
	_, err = handler.aofFile.Write(data)
	if err != nil {
		panic(err)
	}
}
