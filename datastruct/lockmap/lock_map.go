package lockmap

import "sync"

/**
 * @Author: wanglei
 * @File: lock_map
 * @Version: 1.0.0
 * @Description:
 * @Date: 2023/07/10 20:27
 */

const (
	prime32 = uint32(16777619)
)

type Locks struct {
	table []*sync.RWMutex
}

func MakeLocks(tableSize int) *Locks {
	table := make([]*sync.RWMutex, tableSize)
	for i := 0; i < tableSize; i++ {
		table[i] = &sync.RWMutex{}
	}
	return &Locks{
		table: table,
	}
}
