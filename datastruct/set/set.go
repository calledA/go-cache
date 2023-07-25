package set

import "gmr/go-cache/datastruct/dict"

/**
 * @Author: wanglei
 * @File: set
 * @Version: 1.0.0
 * @Description:
 * @Date: 2023/07/19 11:48
 */

type Set struct {
	dict dict.Dict
}

func MakeSet(members ...string) *Set {
	set := &Set{
		dict: dict.MakeSimpleDict(),
	}

	for _, member := range members {
		set.Add(member)
	}
	return set
}

func (s *Set) Add(val string) int {
	return s.dict.Put(val, nil)
}

func (s *Set) Remove(val string) int {
	return s.dict.Remove(val)
}

func (s *Set) Has(val string) bool {
	_, exist := s.dict.Get(val)
	return exist
}

func (s *Set) Len() int {
	return s.dict.Len()
}

func (s *Set) ToSlice() []string {
	slice := make([]string, s.Len())
	i := 0
	s.dict.ForEach(func(key string, val interface{}) bool {
		if i < len(slice) {
			slice[i] = key
		} else {
			slice = append(slice, key)
		}
		i++
		return true
	})
	return slice
}

func (s *Set) ForEach(consumer func(member string) bool) {
	s.dict.ForEach(func(key string, val interface{}) bool {
		return consumer(key)
	})
}

func (s *Set) Intersect(another *Set) *Set {
	if s == nil {
		panic("set is nil")
	}

	result := MakeSet()
	another.ForEach(func(member string) bool {
		if s.Has(member) {
			result.Add(member)
		}
		return true
	})
	return result
}

func (s *Set) Union(another *Set) *Set {
	if s == nil {
		panic("set is nil")
	}
	result := MakeSet()

	another.ForEach(func(member string) bool {
		result.Add(member)
		return true
	})
	s.ForEach(func(member string) bool {
		result.Add(member)
		return true
	})
	return result
}

func (s *Set) Diff(another *Set) *Set {
	if s == nil {
		panic("set is nil")
	}

	result := MakeSet()
	s.ForEach(func(member string) bool {
		if !another.Has(member) {
			result.Add(member)
		}
		return true
	})
	return result
}

func (s *Set) RandomMembers(limit int) []string {
	return s.dict.RandomKeys(limit)
}

func (s *Set) RandomDistinctMembers(limit int) []string {
	return s.dict.RandomDistinctKeys(limit)
}
