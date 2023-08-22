package timewheel

import "time"

/**
 * @Author: wanglei
 * @File: delay
 * @Version: 1.0.0
 * @Description:
 * @Date: 2023/08/14 12:25
 */

var tw = New(time.Second, 3600)

func init() {
	tw.Start()
}

func Delay(duration time.Duration, key string, job func()) {
	tw.AddJob(duration, key, job)
}

func At(at time.Time, key string, job func()) {
	tw.AddJob(at.Sub(time.Now()), key, job)
}

func Cancel(key string) {
	tw.RemoveJob(key)
}
