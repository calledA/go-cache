package aof

import (
	rdb "github.com/hdt3213/rdb/encoder"
	"github.com/hdt3213/rdb/model"
	"gmr/go-cache/config"
	"gmr/go-cache/datastruct/dict"
	"gmr/go-cache/datastruct/list"
	"gmr/go-cache/datastruct/set"
	"gmr/go-cache/datastruct/sortedset"
	"gmr/go-cache/interface/database"
	"gmr/go-cache/lib/logger"
	"io/ioutil"
	"os"
	"strconv"
	"time"
)

/**
 * @Author: wanglei
 * @File: rdb
 * @Version: 1.0.0
 * @Description:
 * @Date: 2023/08/22 9:39
 */

func (handler *Handler) Rewrite2RDB() error {
	ctx, err := handler.startRewrite2RDB()
	if err != nil {
		return err
	}
	err = handler.rewrite2RDB(ctx)
	if err != nil {
		return err
	}
	rdbFilename := config.Properties.RDBFilename
	if rdbFilename == "" {
		rdbFilename = "dump.rdb"
	}
	err = ctx.tmpFile.Close()
	if err != nil {
		return err
	}
	err = os.Rename(ctx.tmpFile.Name(), rdbFilename)
	if err != nil {
		return err
	}
	return nil
}

func (handler *Handler) startRewrite2RDB() (*RewriteCtx, error) {
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
	}, nil
}

func (handler *Handler) rewrite2RDB(ctx *RewriteCtx) error {
	// load aof tmpFile
	tmpHandler := handler.newRewriteHandler()
	tmpHandler.LoadAof(int(ctx.fileSize))
	encoder := rdb.NewEncoder(ctx.tmpFile).EnableCompress()
	err := encoder.WriteHeader()
	if err != nil {
		return err
	}
	auxMap := map[string]string{
		"redis-ver":    "6.0.0",
		"redis-bits":   "64",
		"aof-preamble": "0",
		"ctime":        strconv.FormatInt(time.Now().Unix(), 10),
	}
	for k, v := range auxMap {
		err := encoder.WriteAux(k, v)
		if err != nil {
			return err
		}
	}

	for i := 0; i < config.Properties.Databases; i++ {
		keyCount, ttlCount := tmpHandler.db.GetDBSize(i)
		if keyCount == 0 {
			continue
		}
		err = encoder.WriteDBHeader(uint(i), uint64(keyCount), uint64(ttlCount))
		if err != nil {
			return err
		}
		// dump db
		var err2 error
		tmpHandler.db.ForEach(i, func(key string, entity *database.DataEntity, expiration *time.Time) bool {
			var opts []interface{}
			if expiration != nil {
				opts = append(opts, rdb.WithTTL(uint64(expiration.UnixNano()/1e6)))
			}
			switch obj := entity.Data.(type) {
			case []byte:
				err = encoder.WriteStringObject(key, obj, opts...)
			case list.List:
				vals := make([][]byte, 0, obj.Len())
				obj.ForEach(func(i int, v interface{}) bool {
					bytes, _ := v.([]byte)
					vals = append(vals, bytes)
					return true
				})
				err = encoder.WriteListObject(key, vals, opts...)
			case *set.Set:
				vals := make([][]byte, 0, obj.Len())
				obj.ForEach(func(m string) bool {
					vals = append(vals, []byte(m))
					return true
				})
				err = encoder.WriteSetObject(key, vals, opts...)
			case dict.Dict:
				hash := make(map[string][]byte)
				obj.ForEach(func(key string, val interface{}) bool {
					bytes, _ := val.([]byte)
					hash[key] = bytes
					return true
				})
				err = encoder.WriteHashMapObject(key, hash, opts...)
			case *sortedset.SortedSet:
				var entries []*model.ZSetEntry
				obj.ForEach(int64(0), obj.Len(), true, func(element *sortedset.Element) bool {
					entries = append(entries, &model.ZSetEntry{
						Member: element.Member,
						Score:  element.Score,
					})
					return true
				})
				err = encoder.WriteZSetObject(key, entries, opts...)
			}
			if err != nil {
				err2 = err
				return false
			}
			return true
		})
		if err2 != nil {
			return err2
		}
	}
	err = encoder.WriteEnd()
	if err != nil {
		return err
	}
	return nil
}
