package sortedset

import "math/rand"

/**
 * @Author: wanglei
 * @File: skiplist
 * @Version: 1.0.0
 * @Description:
 * @Date: 2023/07/19 12:24
 */

const maxLevel = 16

// 元素名称和score
type Element struct {
	Member string
	Score  float64
}

type Level struct {
	forward *node // 指向同层中的下一个节点
	span    int64 // 到forward跳过的节点数
}

type node struct {
	Element           // 元素名称和score
	backward *node    // 后向指针
	level    []*Level // 前向指针,level[0]为最下层
}

// 跳表的定义
type skiplist struct {
	header *node // 头指针
	tail   *node // 尾指针
	length int64
	level  int16
}

func makeNode(level int16, score float64, member string) *node {
	n := &node{
		Element: Element{
			Score:  score,
			Member: member,
		},
		level: make([]*Level, level),
	}
	for l := range n.level {
		n.level[l] = new(Level)
	}
	return n
}

func makeSkiplist() *skiplist {
	return &skiplist{
		level:  1,
		header: makeNode(maxLevel, 0, ""),
	}
}

// 随机决定新节点的层数，每向上一层，概率为前一层的25%
func randomLevel() int16 {
	level := int16(1)
	for float32(rand.Int31()&0xFFFF) < (0.25 * 0xFFFF) {
		level++
	}

	if level < maxLevel {
		return level
	}
	return maxLevel
}

// 寻找新节点的前向节点，它们的 forward 将指向新节点
// 因为每层都有一个 forward 指针, 所以每层都会对应一个前向节点
// 找到这些前向节点并保存在 update 数组中
func (s *skiplist) insert(member string, score float64) *node {
	update := make([]*node, maxLevel)
	rank := make([]int64, maxLevel) // 保存各层前向节点的排名，用于计算span

	node := s.header

	// 从上层向下查找
	for i := s.level - 1; i >= 0; i-- {
		// 初始化rank
		if i == s.level-1 {
			rank[i] = 0
		} else {
			rank[i] = rank[i+1]
		}

		if node.level[i] != nil {
			// 遍历节点
			for node.level[i].forward != nil && (node.level[i].forward.Score < score ||
				(node.level[i].forward.Score == score && node.level[i].forward.Member < member)) {
				rank[i] += node.level[i].span
				node = node.level[i].forward
			}
		}
		update[i] = node
	}

	// 随机决定新节点的层数
	level := randomLevel()
	// 可能需要创建新的层
	if level > s.level {
		for i := s.level; i < level; i++ {
			rank[i] = 0
			update[i] = s.header
			update[i].level[i].span = s.length
		}
	}

	// 创建新节点并插入跳表
	node = makeNode(level, score, member)
	for i := int16(0); i < level; i++ {
		// 新节点的forward指向前向节点的forward
		node.level[i].forward = update[i].level[i].forward
		//　前向节点的forward指向新节点
		update[i].level[i].forward = node

		// 计算前向节点和新节点的span
		node.level[i].span = update[i].level[i].span - (rank[0] - rank[i])
		update[i].level[i].span = (rank[0] - rank[i]) + 1
	}

	// 前向节点的span+1
	for i := level; i < s.level; i++ {
		update[i].level[i].span++
	}

	// 更新后向指针
	if update[0] == s.header {
		node.backward = nil
	} else {
		node.backward = update[0]
	}

	if node.level[0].forward != nil {
		node.level[0].forward.backward = node
	} else {
		s.tail = node
	}
	s.length++
	return node
}

// 传入目标节点和删除后的先驱节点
func (s *skiplist) removeNode(node *node, update []*node) {
	for i := int16(0); i < s.level; i++ {
		if update[i].level[i].forward == node {
			// 修改前向节点的指向，同时更新span大小
			update[i].level[i].span += node.level[i].span - 1
			update[i].level[i].forward = node.level[i].forward
		} else {
			update[i].level[i].span--
		}
	}

	// 修改目标节点的后向节点的指针
	if node.level[0].forward != nil {
		node.level[0].forward.backward = node.backward
	} else {
		s.tail = node.backward
	}

	// 删除空白层
	for s.level > 1 && s.header.level[s.level-1].forward == nil {
		s.level--
	}
	s.length--
}

func (s *skiplist) remove(member string, score float64) bool {
	update := make([]*node, maxLevel)
	node := s.header
	for i := s.level - 1; i >= 0; i-- {
		for node.level[i].forward != nil && (node.level[i].forward.Score < score ||
			(node.level[i].forward.Score == score && node.level[i].forward.Member < member)) {
			node = node.level[i].forward
		}
		update[i] = node
	}

	node = node.level[0].forward
	if node != nil && score == node.Score && node.Member == member {
		s.removeNode(node, update)
		return true
	}
	return false
}

func (s *skiplist) getRank(member string, score float64) int64 {
	var rank int64 = 0
	x := s.header

	for i := s.level - 1; i >= 0; i-- {
		for x.level[i].forward != nil && (x.level[i].forward.Score < score ||
			(x.level[i].forward.Score == score && x.level[i].forward.Member <= member)) {
			rank += x.level[i].span
			x = x.level[i].forward
		}

		if x.Member == member {
			return rank
		}
	}
	return 0
}

// 寻找排名为rank的节点，从rank 1开始
func (s *skiplist) getByRank(rank int64) *node {
	var i int64 = 0
	n := s.header

	// 从顶层向下查找
	for level := s.level - 1; level >= 0; level-- {
		// 从当前层向前查询
		// 若当前层的下一个节点已经超过目标 (i+n.level[level].span > rank)，则结束当前层搜索进入下一层
		// n.level[level].span为该层间隔的元素
		for n.level[level].forward != nil && (i+n.level[level].span) <= rank {
			i += n.level[level].span
			n = n.level[level].forward
		}
		if i == rank {
			return n
		}
	}
	return nil
}

func (s *skiplist) hasInRange(min *ScoreBorder, max *ScoreBorder) bool {
	if min.Value > max.Value || (min.Value == max.Value && (min.Exclude || max.Exclude)) {
		return false
	}

	n := s.tail

	if n == nil || !min.less(n.Score) {
		return false
	}

	n = s.header.level[0].forward
	if n == nil || !max.greater(n.Score) {
		return false
	}

	return true
}

func (s *skiplist) getFirstInScoreRange(min *ScoreBorder, max *ScoreBorder) *node {
	// 判断是否在限定范围内
	if !s.hasInRange(min, max) {
		return nil
	}

	n := s.header

	// 顶层向下查询
	for level := s.level - 1; level >= 0; level-- {
		// 若forward节点未在范围则向前查找
		// 若forward在范围，当 level > 0 时 forward 节点不能保证是 *第一个* 在 min 范围内的节点， 因此需进入下一层查找
		for n.level[level].forward != nil && !min.less(n.level[level].forward.Score) {
			n = n.level[level].forward
		}
	}

	// 当从外层循环退出时 level=0 (最下层), n.level[0].forward 一定是 min 范围内的第一个节点
	n = n.level[0].forward
	if !max.greater(n.Score) {
		return nil
	}
	return n
}

func (s *skiplist) getLastInScoreRange(min *ScoreBorder, max *ScoreBorder) *node {
	if !s.hasInRange(min, max) {
		return nil
	}

	n := s.header
	for level := s.level - 1; level >= 0; level-- {
		for n.level[level].forward != nil && max.greater(n.level[level].forward.Score) {
			n = n.level[level].forward
		}
	}

	if !min.less(n.Score) {
		return nil
	}
	return n
}

func (s *skiplist) RemoveRangeByScore(min *ScoreBorder, max *ScoreBorder) (removed []*Element) {
	update := make([]*node, maxLevel)
	removed = make([]*Element, 0)

	node := s.header

	for i := s.level - 1; i >= 0; i-- {
		for node.level[i].forward != nil {
			if min.less(node.level[i].forward.Score) {
				break
			}
			node = node.level[i].forward
		}
		update[i] = node
	}

	node = node.level[0].forward

	for node != nil {
		if !max.greater(node.Score) {
			break
		}

		next := node.level[0].forward
		removedElement := node.Element
		removed = append(removed, &removedElement)
		s.removeNode(node, update)
		node = next
	}
	return removed
}

// 删除操作可能一次删除多个节点
func (s *skiplist) RemoveRangeByRank(start int64, end int64) (removed []*Element) {
	// 当前指针排名
	var i int64 = 0
	update := make([]*node, maxLevel)
	removed = make([]*Element, 0)

	node := s.header
	// 从顶层向下寻找目标的前向节点
	for level := s.level - 1; level >= 0; level-- {
		for node.level[level].forward != nil && (i+node.level[level].span) < start {
			i += node.level[level].span
			node = node.level[level].forward
		}
		update[level] = node
	}

	i++
	node = node.level[0].forward // node 是目标范围内第一个节点

	// 删除范围内的所有节点
	for node != nil && i < end {
		next := node.level[0].forward
		removedElement := node.Element
		removed = append(removed, &removedElement)
		s.removeNode(node, update)
		node = next
		i++
	}
	return removed
}
