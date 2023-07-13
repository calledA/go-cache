package list

/**
 * @Author: wanglei
 * @File: list
 * @Version: 1.0.0
 * @Description:
 * @Date: 2023/07/11 18:16
 */

// 判断val是否为期望的值
type Expected func(val interface{}) bool

// 通过i，v进行遍历，返回bool
type Consumer func(i int, v interface{}) bool

type List interface {
	Len() int
	Add(val interface{})
	Get(index int) (val interface{})
	Set(index int, val interface{})
	Insert(index int, val interface{})
	Remove(index int) (val interface{})
	RemoveLast() (val interface{})
	RemoveAllByValue(expected Expected) int
	RemoveByVal(expected Expected, count int) int
	ReverseRemoveByValue(expected Expected, count int) int
	ForEach(consumer Consumer)
	Contains(expected Expected) bool
	Range(start int, stop int) []interface{}
}
