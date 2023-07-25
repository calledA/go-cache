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

func (d *SimpleDict) Get(key string) (val interface{}, exist bool) {
	if d == nil {
		panic("dict is nil")
	}

	val, exist = d.m[key]
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

func (d *SimpleDict) PutIfExist(key string, val interface{}) (result int) {
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
			break
		}
	}
}

func (d *SimpleDict) Keys() []string {
	result := make([]string, d.Len())
	i := 0
	for key := range d.m {
		result[i] = key
	}
	return result
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
		for key := range d.m {
			result[i] = key
			break
		}
	}
	return result
}

func (d *SimpleDict) RandomDistinctKeys(limit int) []string {
	size := d.Len()
	if limit >= size {
		return d.Keys()
	}

	result := make([]string, size)

	i := 0
	for k := range d.m {
		if i == limit {
			break
		}
		result[i] = k
		i++
	}
	return result
}

func (d *SimpleDict) Clear() {
	*d = *MakeSimpleDict()
}
