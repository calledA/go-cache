package utils

import "testing"

/**
 * @Author: wanglei
 * @File: rand_string_test
 * @Version: 1.0.0
 * @Description:
 * @Date: 2023/07/11 16:50
 */

func TestRandString(t *testing.T) {
	randString := RandString(6)
	t.Log(randString)
}
