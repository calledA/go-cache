package sortedset

import "strconv"

/**
 * @Author: wanglei
 * @File: sortedset
 * @Version: 1.0.0
 * @Description:
 * @Date: 2023/07/19 16:37
 */

type SortedSet struct {
	dict     map[string]*Element
	skiplist *skiplist
}

func MakeSortedSet() *SortedSet {
	return &SortedSet{
		dict:     make(map[string]*Element),
		skiplist: makeSkiplist(),
	}
}

func (ss *SortedSet) Add(member string, score float64) bool {
	element, ok := ss.dict[member]
	ss.dict[member] = &Element{
		Member: member,
		Score:  score,
	}

	if ok {
		if score != element.Score {
			ss.skiplist.remove(member, element.Score)
			ss.skiplist.insert(member, score)
		}
		return false
	}
	ss.skiplist.insert(member, score)
	return true
}

func (ss *SortedSet) Len() int64 {
	return int64(len(ss.dict))
}

func (ss *SortedSet) Get(member string) (element *Element, ok bool) {
	element, ok = ss.dict[member]
	if !ok {
		return nil, false
	}
	return element, true
}

func (ss *SortedSet) Remove(member string) bool {
	v, ok := ss.dict[member]
	if ok {
		ss.skiplist.remove(member, v.Score)
		delete(ss.dict, member)
		return true
	}
	return false
}

func (ss *SortedSet) GetRank(member string, desc bool) (rank int64) {
	element, ok := ss.dict[member]
	if !ok {
		return -1
	}
	r := ss.skiplist.getRank(member, element.Score)
	if desc {
		r = ss.skiplist.length - r
	} else {
		r--
	}
	return r
}

func (ss *SortedSet) ForEach(start int64, stop int64, desc bool, consumer func(element *Element) bool) {
	size := int64(ss.Len())
	if start < 0 || start >= size {
		panic("illegal start " + strconv.FormatInt(start, 10))
	}
	if stop < start || stop > size {
		panic("illegal end " + strconv.FormatInt(stop, 10))
	}

	// find start node
	var node *node
	if desc {
		node = ss.skiplist.tail
		if start > 0 {
			node = ss.skiplist.getByRank(int64(size - start))
		}
	} else {
		node = ss.skiplist.header.level[0].forward
		if start > 0 {
			node = ss.skiplist.getByRank(int64(start + 1))
		}
	}

	sliceSize := int(stop - start)
	for i := 0; i < sliceSize; i++ {
		if !consumer(&node.Element) {
			break
		}
		if desc {
			node = node.backward
		} else {
			node = node.level[0].forward
		}
	}
}

func (ss *SortedSet) Range(start int64, stop int64, desc bool) []*Element {
	sliceSize := int(stop - start)
	slice := make([]*Element, sliceSize)
	i := 0
	ss.ForEach(start, stop, desc, func(element *Element) bool {
		slice[i] = element
		i++
		return true
	})
	return slice
}

func (ss *SortedSet) Count(min *ScoreBorder, max *ScoreBorder) int64 {
	var i int64 = 0
	ss.ForEach(0, ss.Len(), false, func(element *Element) bool {
		gtMin := min.less(element.Score) // greater than min
		if !gtMin {
			return true
		}
		ltMax := max.greater(element.Score) // less than max
		if !ltMax {
			return false
		}
		i++
		return true
	})
	return i
}

func (ss *SortedSet) ForEachByScore(min *ScoreBorder, max *ScoreBorder, offset int64, limit int64, desc bool, consumer func(element *Element) bool) {
	var node *node
	if desc {
		node = ss.skiplist.getLastInScoreRange(min, max)
	} else {
		node = ss.skiplist.getFirstInScoreRange(min, max)
	}

	for node != nil && offset > 0 {
		if desc {
			node = node.backward
		} else {
			node = node.level[0].forward
		}
		offset--
	}

	// A negative limit returns all elements from the offset
	for i := 0; (i < int(limit) || limit < 0) && node != nil; i++ {
		if !consumer(&node.Element) {
			break
		}
		if desc {
			node = node.backward
		} else {
			node = node.level[0].forward
		}
		if node == nil {
			break
		}
		gtMin := min.less(node.Element.Score) // greater than min
		ltMax := max.greater(node.Element.Score)
		if !gtMin || !ltMax {
			break // break through score border
		}
	}
}

// RangeByScore returns members which score within the given border
// param limit: <0 means no limit
func (ss *SortedSet) RangeByScore(min *ScoreBorder, max *ScoreBorder, offset int64, limit int64, desc bool) []*Element {
	if limit == 0 || offset < 0 {
		return make([]*Element, 0)
	}
	slice := make([]*Element, 0)
	ss.ForEachByScore(min, max, offset, limit, desc, func(element *Element) bool {
		slice = append(slice, element)
		return true
	})
	return slice
}

// RemoveByScore removes members which score within the given border
func (ss *SortedSet) RemoveByScore(min *ScoreBorder, max *ScoreBorder) int64 {
	removed := ss.skiplist.RemoveRangeByScore(min, max)
	for _, element := range removed {
		delete(ss.dict, element.Member)
	}
	return int64(len(removed))
}

func (ss *SortedSet) RemoveByRank(start int64, stop int64) int64 {
	removed := ss.skiplist.RemoveRangeByRank(start+1, stop+1)
	for _, element := range removed {
		delete(ss.dict, element.Member)
	}
	return int64(len(removed))
}
