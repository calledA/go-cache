package wait

import (
	"sync"
	"time"
)

/**
 * @Author: wanglei
 * @File: wait
 * @Version: 1.0.0
 * @Description: 锁方法封装
 * @Date: 2023/07/10 14:48
 */

type Wait struct {
	wg sync.WaitGroup
}

// wg添加delta个couter
func (w *Wait) Add(delta int) {
	w.wg.Add(delta)
}

// wg couter减一
func (w *Wait) Done() {
	w.wg.Done()
}

// wg阻塞直到couter到0
func (w *Wait) Wait() {
	w.wg.Wait()
}

// wg阻塞timeout时间或者couter到0
func (w *Wait) WaitWithTimeout(timeout time.Duration) bool {
	c := make(chan bool, 1)
	go func() {
		defer close(c)
		w.wg.Wait()
		c <- true
	}()

	select {
	case <-c:
		return false
	case <-time.After(timeout):
		return true
	}
}
