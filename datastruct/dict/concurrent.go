package dict

import (
	"math"
	"math/rand"
	"sync"
	"sync/atomic"
)

/**
 * @Author: wanglei
 * @File: concurrent
 * @Version: 1.0.0
 * @Description: 使用分段锁实现线程安全的ConcurrentDict
 * @Date: 2023/07/11 9:56
 */

const (
	prime32 = uint32(16777619)
)

// 使用分段锁来保证ConcurrentDict的线程安全，与concurrentHashMap类似
type ConcurrentDict struct {
	table     []*shard
	count     int32
	shadCount int
}

type shard struct {
	m     map[string]interface{}
	mutex sync.RWMutex
}

// 通过给定的shardCount生成ConcurrentDict
func MakeConcurrentDict(shardCount int) *ConcurrentDict {
	shardCount = computeCapacity(shardCount)
	table := make([]*shard, shardCount)
	for i := 0; i < shardCount; i++ {
		table[i] = &shard{
			m: make(map[string]interface{}),
		}
	}

	return &ConcurrentDict{
		count:     0,
		table:     table,
		shadCount: shardCount,
	}
}

func computeCapacity(param int) (size int) {
	if param <= 16 {
		return 16
	}

	n := param - 1
	n |= n >> 1
	n |= n >> 2
	n |= n >> 4
	n |= n >> 8
	n |= n >> 16

	if n < 0 {
		return math.MaxInt32
	}

	return n + 1
}

// 使用fnv-1算法，fnv-1a与fnv-1算法步骤相反
func fnv32(key string) uint32 {
	// hash值
	hash := uint32(2166136261)
	for i := 0; i < len(key); i++ {
		// hash值*散列质数
		hash *= prime32
		// 当前字节与hash值做异或运算
		hash ^= uint32(key[i])
	}
	return hash
}

// 获取shard位置
func (d *ConcurrentDict) spread(hashCode uint32) uint32 {
	if d == nil {
		panic("dict is nil")
	}
	tableSize := uint32(len(d.table))
	return (tableSize - 1) & uint32(hashCode)
}

// 返回shard
func (d *ConcurrentDict) getShard(index uint32) *shard {
	if d == nil {
		panic("dict is nil")
	}
	return d.table[index]
}

// dict值的长度
func (d *ConcurrentDict) Len() int {
	if d == nil {
		panic("dict is nil")
	}
	return int(atomic.LoadInt32(&d.count))
}

// 获取key的值
func (d *ConcurrentDict) Get(key string) (val interface{}, exists bool) {
	if d == nil {
		panic("dict is nil")
	}
	s := d.GetShardByKey(key)
	// 加读锁，get是读多写少场景
	s.mutex.RLock()
	// defer关闭读锁
	defer s.mutex.RUnlock()
	// 返回找到的值和exist（bool）
	val, exists = s.m[key]
	return
}

// 向dict中存值
func (d *ConcurrentDict) Put(key string, val interface{}) (result int) {
	if d == nil {
		panic("dict is nil")
	}

	s := d.GetShardByKey(key)
	// put是写场景，需要加写锁，写锁阻塞读和写
	s.mutex.Lock()
	// defer关闭写锁
	defer s.mutex.Unlock()

	// 如果值已经存在，则覆盖并返回
	if _, ok := s.m[key]; ok {
		s.m[key] = val
		return 0
	}

	// 值不存在则向shard添加key
	s.m[key] = val
	// 添加count值
	d.addCount()
	return 1
}

// 在dict没有当前key情况下再存值
func (d *ConcurrentDict) PutIfAbsent(key string, val interface{}) (result int) {
	if d == nil {
		panic("dict is nil")
	}

	s := d.GetShardByKey(key)
	s.mutex.Lock()
	defer s.mutex.Unlock()

	if _, ok := s.m[key]; ok {
		return 0
	}

	s.m[key] = val
	d.addCount()
	return 1
}

// 在dict有当前key情况下再存值
func (d *ConcurrentDict) PutIfExists(key string, val interface{}) (result int) {
	if d == nil {
		panic("dict is nil")
	}

	s := d.GetShardByKey(key)
	s.mutex.Lock()
	defer s.mutex.Unlock()

	if _, ok := s.m[key]; ok {
		s.m[key] = val
		return 1
	}
	return 0
}

// 删除当前key
func (d *ConcurrentDict) Remove(key string) (result int) {
	if d == nil {
		panic("dict is nil")
	}

	s := d.GetShardByKey(key)
	s.mutex.Lock()
	defer s.mutex.Unlock()

	if _, ok := s.m[key]; ok {
		delete(s.m, key)
		// 删除key之后减少count值
		d.decreaseCount()
		return 1
	}
	return 0
}

// 遍历当前dict
func (d *ConcurrentDict) ForEach(consumer Consumer) {
	if d == nil {
		panic("dict is nil")
	}

	for _, s := range d.table {
		s.mutex.RLock()
		// 使用匿名函数defer关闭锁
		func() {
			defer s.mutex.RUnlock()
			for key, value := range s.m {
				continues := consumer(key, value)
				if !continues {
					return
				}
			}
		}()
	}
}

func (d *ConcurrentDict) Keys() []string {
	keys := make([]string, d.Len())
	i := 0
	d.ForEach(func(key string, val interface{}) bool {
		if i < len(keys) {
			keys[i] = key
			i++
		} else {
			keys = append(keys, key)
		}
		return true
	})
	return keys
}

func (s *shard) RandomKey() string {
	if s == nil {
		panic("shard is nil")
	}

	s.mutex.RLock()
	defer s.mutex.RUnlock()

	// for range无序的，返回的key是随机的
	for key := range s.m {
		return key
	}
	return ""
}

// 随机获取keys
func (d *ConcurrentDict) RandomKeys(limit int) []string {
	size := d.Len()
	// limit超过dict长度，则返回dict全部keys
	if limit >= size {
		return d.Keys()
	}
	shardCount := len(d.table)

	result := make([]string, limit)

	for i := 0; i < limit; {
		s := d.getShard(uint32(rand.Intn(shardCount)))
		if s == nil {
			continue
		}

		key := s.RandomKey()
		if key != "" {
			result[i] = key
			i++
		}
	}
	return result
}

func (d *ConcurrentDict) RandomDistinctKeys(limit int) []string {
	size := d.Len()
	if limit >= size {
		return d.Keys()
	}
	shardCount := len(d.table)

	result := make(map[string]bool)
	for len(result) < limit {
		shardIndex := uint32(rand.Intn(shardCount))
		s := d.getShard(shardIndex)
		if s == nil {
			continue
		}
		key := s.RandomKey()
		if key != "" {
			result[key] = true
		}
	}
	arr := make([]string, limit)
	i := 0
	for k := range result {
		arr[i] = k
		i++
	}
	return arr
}

// 通过MakeConcurrentDict覆盖dict，实现清空dict
func (d *ConcurrentDict) Clear() {
	*d = *MakeConcurrentDict(d.shadCount)
}

func (d *ConcurrentDict) addCount() int32 {
	return atomic.AddInt32(&d.count, 1)
}

func (d *ConcurrentDict) decreaseCount() int32 {
	return atomic.AddInt32(&d.count, -1)
}

// 通过key获取shard
func (d *ConcurrentDict) GetShardByKey(key string) (shard *shard) {
	// 计算hash值
	hashCode := fnv32(key)
	// 通过hash值获取shard位置
	index := d.spread(hashCode)
	// 获取到当前shard
	shard = d.getShard(index)
	return
}
