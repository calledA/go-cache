package set

import (
	"strconv"
	"testing"
)

/**
 * @Author: wanglei
 * @File: set_test
 * @Version: 1.0.0
 * @Description:
 * @Date: 2023/07/19 12:04
 */

func TestSet(t *testing.T) {
	size := 10
	set := MakeSet()
	for i := 0; i < size; i++ {
		set.Add(strconv.Itoa(i))
	}
	for i := 0; i < size; i++ {
		ok := set.Has(strconv.Itoa(i))
		if !ok {
			t.Error("expected true actual false, key: " + strconv.Itoa(i))
		}
	}
	for i := 0; i < size; i++ {
		ok := set.Remove(strconv.Itoa(i))
		if ok != 1 {
			t.Error("expected true actual false, key: " + strconv.Itoa(i))
		}
	}
	for i := 0; i < size; i++ {
		ok := set.Has(strconv.Itoa(i))
		if ok {
			t.Error("expected false actual true, key: " + strconv.Itoa(i))
		}
	}
}
