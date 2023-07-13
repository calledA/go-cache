package utils

/**
 * @Author: wanglei
 * @File: utils
 * @Version: 1.0.0
 * @Description: 工具包
 * @Date: 2023/07/05 17:20
 */

// 将string转成[][]byte
func ToCmdLine(cmd ...string) [][]byte {
	args := make([][]byte, len(cmd))
	for i, s := range cmd {
		args[i] = []byte(s)
	}
	return args
}

// 将cmdName和args(string)转换成[][]byte
func ToCmdLineByParam(cmdName string, args ...string) [][]byte {
	result := make([][]byte, len(args)+1)
	result[0] = []byte(cmdName)
	for i, arg := range args {
		result[i+1] = []byte(arg)
	}
	return result
}

// 将cmdName和args([]byte)转换成[][]byte
func ToCmdLineByByte(cmdName string, args ...[]byte) [][]byte {
	result := make([][]byte, len(args)+1)
	result[0] = []byte(cmdName)
	for i, arg := range args {
		result[i+1] = arg
	}
	return result
}

// 两个interface值是否相等
func Equals(a interface{}, b interface{}) bool {
	s1, ok1 := a.([]byte)
	s2, ok2 := b.([]byte)
	if ok1 && ok2 {
		return BytesEquals(s1, s2)
	}
	return a == b
}

// 两个byte是否相等
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

// ConvertRange将redis索引转换为go切片索引
// 左包含右不包含，超过最大值是返回[-1,-1]
func ConvertRange(start int64, end int64, size int64) (int, int) {
	if start < -size {
		return -1, -1
	} else if start < 0 {
		start = size + start
	} else if start >= size {
		return -1, -1
	}

	if end < -size {
		return -1, -1
	} else if end < 0 {
		end = size + end + 1
	} else if end < size {
		end = end + 1
	} else {
		end = size
	}
	if start > end {
		return -1, -1
	}
	return int(start), int(end)
}
