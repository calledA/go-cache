package lockmap

import (
	"sort"
	"sync"
)

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

func fnv32(key string) uint32 {
	hash := uint32(2166136261)
	for i := 0; i < len(key); i++ {
		hash *= prime32
		hash ^= uint32(key[i])
	}
	return hash
}

func (l *Locks) spread(hashCode uint32) uint32 {
	if l == nil {
		panic("dict is nil")
	}

	tableSize := uint32(len(l.table))
	return (tableSize - 1) & uint32(hashCode)
}

func (l *Locks) Lock(key string) {
	index := l.spread(fnv32(key))
	mutex := l.table[index]
	mutex.Lock()
}

func (l *Locks) UnLock(key string) {
	index := l.spread(fnv32(key))
	mutex := l.table[index]
	mutex.Unlock()
}

func (l *Locks) RLock(key string) {
	index := l.spread(fnv32(key))
	mutex := l.table[index]
	mutex.RLock()
}

func (l *Locks) RUnLock(key string) {
	index := l.spread(fnv32(key))
	mutex := l.table[index]
	mutex.RUnlock()
}

func (l *Locks) toLockIndices(keys []string, reverse bool) []uint32 {
	im := make(map[uint32]bool)
	for _, key := range keys {
		index := l.spread(fnv32(key))
		im[index] = true
	}

	indices := make([]uint32, 0, len(im))
	for i := range im {
		indices = append(indices, i)
	}

	sort.Slice(indices, func(i, j int) bool {
		if !reverse {
			return indices[i] < indices[j]
		}
		return indices[i] > indices[j]
	})
	return indices
}

func (l *Locks) Locks(keys ...string) {
	indeices := l.toLockIndices(keys, false)
	for _, index := range indeices {
		mutex := l.table[index]
		mutex.Lock()
	}
}

func (l *Locks) UnLocks(keys ...string) {
	indeices := l.toLockIndices(keys, true)
	for _, index := range indeices {
		mutex := l.table[index]
		mutex.Unlock()
	}
}

func (l *Locks) RLocks(keys ...string) {
	indeices := l.toLockIndices(keys, false)
	for _, index := range indeices {
		mutex := l.table[index]
		mutex.RLock()
	}
}

func (l *Locks) RUnLocks(keys ...string) {
	indeices := l.toLockIndices(keys, true)
	for _, index := range indeices {
		mutex := l.table[index]
		mutex.RUnlock()
	}
}

func (l *Locks) RWLock(writeKeys []string, readKeys []string) {
	keys := append(writeKeys, readKeys...)
	indices := l.toLockIndices(keys, false)
	writeIndices := l.toLockIndices(writeKeys, false)
	writeIndexSet := make(map[uint32]struct{})

	for _, idx := range writeIndices {
		writeIndexSet[idx] = struct{}{}
	}

	for _, idx := range indices {
		_, w := writeIndexSet[idx]
		mutex := l.table[idx]
		if w {
			mutex.Lock()
		} else {
			mutex.RLock()
		}
	}
}

func (l *Locks) RWUnLock(writeKeys []string, readKeys []string) {
	keys := append(writeKeys, readKeys...)
	indices := l.toLockIndices(keys, true)
	writeIndices := l.toLockIndices(writeKeys, true)
	writeIndexSet := make(map[uint32]struct{})

	for _, idx := range writeIndices {
		writeIndexSet[idx] = struct{}{}
	}

	for _, idx := range indices {
		_, w := writeIndexSet[idx]
		mutex := l.table[idx]
		if w {
			mutex.Unlock()
		} else {
			mutex.RUnlock()
		}
	}
}
