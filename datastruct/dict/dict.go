package dict

/**
 * @Author: wanglei
 * @File: dict
 * @Version: 1.0.0
 * @Description:
 * @Date: 2023/07/10 20:18
 */

// Consumer consumer遍历dict，如果遍历中断则返回false
type Consumer func(key string, val interface{}) bool

// Dict key-value数据格式的interface
type Dict interface {
	Len() int
	Get(key string) (val interface{}, exists bool)
	Put(key string, val interface{}) (result int)
	PutIfAbsent(key string, val interface{}) (result int)
	PutIfExists(key string, val interface{}) (result int)
	Remove(key string) (result int)
	ForEach(consumer Consumer)
	Keys() []string
	RandomKeys(limit int) []string
	RandomDistinctKeys(limit int) []string
	Clear()
}
