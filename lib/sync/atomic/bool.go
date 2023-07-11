package atomic

import "sync/atomic"

/**
 * @Author: wanglei
 * @File: bool.go
 * @Version: 1.0.0
 * @Description:
 * @Date: 2023/07/10 19:59
 */

// 原子操作的boolean值
type Boolean uint32

// 原子读取Boolean值
func (b *Boolean) Get() bool {
	return atomic.LoadUint32((*uint32)(b)) != 0
}

// 原子存Boolean值
func (b *Boolean) Set(v bool) {
	if v {
		atomic.StoreUint32((*uint32)(b), 1)
	} else {
		atomic.StoreUint32((*uint32)(b), 0)
	}
}
