package utils

/**
 * @Author: wanglei
 * @File: utils
 * @Version: 1.0.0
 * @Description: 工具包
 * @Date: 2023/07/05 17:20
 */

func BytesEquals(a, b []byte) bool {
	if (a == nil && b != nil) || (a != nil && b == nil) {
		return false
	}

	if len(a) != len(b) {
		return false
	}

	for i := 0; i < len(a); i++ {
		av := a[i]
		bv := b[i]
		if av != bv {
			return false
		}
	}
	return true
}
