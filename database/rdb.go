package database

import (
	"github.com/hdt3213/rdb/core"
	"github.com/hdt3213/rdb/model"
	rdb "github.com/hdt3213/rdb/parser"
	"gmr/go-cache/config"
	"gmr/go-cache/datastruct/dict"
	"gmr/go-cache/datastruct/list"
	"gmr/go-cache/datastruct/sortedset"
	"gmr/go-cache/interface/database"
	"gmr/go-cache/lib/logger"
	"os"
)

/**
 * @Author: wanglei
 * @File: rdb
 * @Version: 1.0.0
 * @Description:
 * @Date: 2023/08/04 14:25
 */

func loadRdbFile(mdb *MultiDB) {
	rdbFile, err := os.Open(config.Properties.RDBFilename)
	if err != nil {
		logger.Error("open rdb file failed" + err.Error())
		return
	}

	defer func() {
		rdbFile.Close()
	}()

	decoder := rdb.NewDecoder(rdbFile)
	err = dumpRDB(decoder, mdb)
	if err != nil {
		logger.Error("dump rdb file failed" + err.Error())
		return
	}
}

//todo
func dumpRDB(dec *core.Decoder, mdb *MultiDB) error {
	return dec.Parse(func(o model.RedisObject) bool {
		db := mdb.mustSelectDB(o.GetDBIndex())
		switch o.GetType() {
		case rdb.StringType:
			str := o.(*rdb.StringObject)
			db.PutEntity(o.GetKey(), &database.DataEntity{
				Data: str.Value,
			})
		case rdb.ListType:
			listObj := o.(*rdb.ListObject)
			l := list.NewQuickList()
			for _, value := range listObj.Values {
				l.Add(value)
			}
			db.PutEntity(o.GetKey(), &database.DataEntity{
				Data: l,
			})
		case rdb.HashType:
			hashObj := o.(*rdb.HashObject)
			hash := dict.MakeSimpleDict()
			for key, value := range hashObj.Values {
				hash.Put(key, value)
			}
			db.PutEntity(o.GetKey(), &database.DataEntity{
				Data: hash,
			})
		case rdb.ZSetType:
			zsetObj := o.(*rdb.ZSetObject)
			zset := sortedset.MakeSortedSet()
			for _, e := range zsetObj.Entries {
				zset.Add(e.Member, e.Score)
			}
			db.PutEntity(o.GetKey(), &database.DataEntity{
				Data: zset,
			})
		}

		if o.GetExpiration() != nil {
			db.Expire(o.GetKey, o.GetExpiration)
		}
		return true
	})
}
