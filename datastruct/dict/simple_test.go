package dict

import (
	"gmr/go-cache/lib/utils"
	"testing"
)

/**
 * @Author: wanglei
 * @File: simple_test
 * @Version: 1.0.0
 * @Description:
 * @Date: 2023/07/11 17:10
 */

func TestSimpleDict_Keys(t *testing.T) {
	d := MakeSimpleDict()
	size := 10
	for i := 0; i < size; i++ {
		d.Put(utils.RandString(5), utils.RandString(5))
	}
	if len(d.Keys()) != size {
		t.Errorf("expect %d keys, actual: %d", size, len(d.Keys()))
	}
}

func TestSimpleDict_PutIfExists(t *testing.T) {
	d := MakeSimpleDict()
	key := utils.RandString(5)
	val := key + "1"
	ret := d.PutIfExists(key, val)
	if ret != 0 {
		t.Error("expect 0")
		return
	}
	d.Put(key, val)
	val = key + "2"
	ret = d.PutIfExists(key, val)
	if ret != 1 {
		t.Error("expect 1")
		return
	}
	if v, _ := d.Get(key); v != val {
		t.Error("wrong value")
		return
	}
}
