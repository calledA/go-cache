package dict

/**
 * @Author: wanglei
 * @File: simple
 * @Version: 1.0.0
 * @Description: 非线程安全的dict
 * @Date: 2023/07/11 12:22
 */

type SimpleDict struct {
	m map[string]interface{}
}

func MakeSimpleDict() *SimpleDict {
	return &SimpleDict{
		m: make(map[string]interface{}),
	}
}

func (d *SimpleDict) Len() int {
	if d == nil {
		panic("dict is nil")
	}
	return len(d.m)
}

func (d *SimpleDict) Get(key string) (val interface{}, exists bool) {
	if d == nil {
		panic("dict is nil")
	}

	val, exists = d.m[key]
	return
}

func (d *SimpleDict) Put(key string, val interface{}) (result int) {
	if d == nil {
		panic("dict is nil")
	}
	_, ok := d.m[key]
	d.m[key] = val

	if ok {
		return 0
	}

	return 1
}

func (d *SimpleDict) PutIfAbsent(key string, val interface{}) (result int) {
	if d == nil {
		panic("dict is nil")
	}

	if _, ok := d.m[key]; ok {
		return 0
	}

	// 值不存在则向shard添加key
	d.m[key] = val
	return 1
}

func (d *SimpleDict) PutIfExists(key string, val interface{}) (result int) {
	if d == nil {
		panic("dict is nil")
	}

	if _, ok := d.m[key]; ok {
		d.m[key] = val
		return 1
	}

	// 值不存在则向shard添加key
	return 0
}

func (d *SimpleDict) Remove(key string) (result int) {
	if d == nil {
		panic("dict is nil")
	}

	if _, ok := d.m[key]; ok {
		delete(d.m, key)
		// 删除key之后减少count值
		return 1
	}
	return 0
}

func (d *SimpleDict) ForEach(consumer Consumer) {
	if d == nil {
		panic("dict is nil")
	}

	for key, value := range d.m {
		continues := consumer(key, value)
		if !continues {
			return
		}
	}
}

func (d *SimpleDict) Keys() []string {
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

func (d *SimpleDict) RandomKey() string {
	if d == nil {
		panic("dict is nil")
	}

	for key := range d.m {
		return key
	}
	return ""
}

// 随机获取keys
func (d *SimpleDict) RandomKeys(limit int) []string {
	size := d.Len()
	// limit超过dict长度，则返回dict全部keys
	if limit >= size {
		return d.Keys()
	}

	result := make([]string, limit)

	for i := 0; i < limit; {
		key := d.RandomKey()
		if key != "" {
			result[i] = key
			i++
		}
	}
	return result
}

func (d *SimpleDict) RandomDistinctKeys(limit int) []string {
	size := d.Len()
	if limit >= size {
		return d.Keys()
	}

	result := make(map[string]bool)
	for len(result) < limit {
		key := d.RandomKey()
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

func (d *SimpleDict) Clear() {
	d = MakeSimpleDict()
}
