package idgenerator

import (
	"gmr/go-cache/lib/logger"
	"hash/fnv"
	"sync"
	"time"
)

/**
 * @Author: wanglei
 * @File: snowflake
 * @Version: 1.0.0
 * @Description:
 * @Date: 2023/08/14 11:47
 */

const (
	expoch0     int64 = 1288834974657
	maxSequence int64 = -1 ^ (-1 << uint64(nodeLeft))
	timeLeft    uint8 = 22
	nodeLeft    uint8 = 10
	nodeMask    int64 = -1 ^ (-1 << uint64(timeLeft-nodeLeft))
)

type IDGenerator struct {
	mutex     *sync.Mutex
	lastStamp int64
	nodeID    int64
	sequence  int64
	epoch     time.Time
}

func MakeIDGenerator(node string) *IDGenerator {
	fnv64 := fnv.New64()
	_, _ = fnv64.Write([]byte(node))
	nodeID := int64(fnv64.Sum64()) & nodeMask

	var curTime = time.Now()
	epoch := curTime.Add(time.Unix(expoch0/1000, (expoch0%1000)*1000000).Sub(curTime))

	return &IDGenerator{
		mutex:     &sync.Mutex{},
		lastStamp: -1,
		nodeID:    nodeID,
		sequence:  1,
		epoch:     epoch,
	}
}

func (ig *IDGenerator) NextID() int64 {
	ig.mutex.Lock()
	defer ig.mutex.Unlock()

	timestamp := time.Since(ig.epoch).Nanoseconds() / 1000000
	if timestamp < ig.lastStamp {
		logger.Error("cannot generate id")
	}

	if ig.lastStamp == timestamp {
		ig.sequence = (ig.sequence + 1) & maxSequence
		if ig.sequence == 0 {
			for timestamp <= ig.lastStamp {
				timestamp = time.Since(ig.epoch).Nanoseconds() / 1000000
			}
		}
	} else {
		ig.sequence = 0
	}

	ig.lastStamp = timestamp
	id := (timestamp << timeLeft) | (ig.nodeID << nodeLeft) | ig.sequence
	return id
}
