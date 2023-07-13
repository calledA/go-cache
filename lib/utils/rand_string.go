package utils

import "math/rand"

/**
 * @Author: wanglei
 * @File: rand_string
 * @Version: 1.0.0
 * @Description:
 * @Date: 2023/07/11 16:49
 */

var letters = []rune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789")

// RandString 创建一个n个字符的随机字符串
func RandString(n int) string {
	b := make([]rune, n)
	for i := range b {
		b[i] = letters[rand.Intn(len(letters))]
	}
	return string(b)
}
