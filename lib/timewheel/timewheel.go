package timewheel

import (
	"container/list"
	"gmr/go-cache/lib/logger"
	"time"
)

/**
 * @Author: wanglei
 * @File: timewheel
 * @Version: 1.0.0
 * @Description:
 * @Date: 2023/08/14 12:25
 */

type location struct {
	slot  int
	etask *list.Element
}

type TimeWheel struct {
	interval time.Duration
	ticker   *time.Ticker
	slots    []*list.List

	timer             map[string]*location
	currentPos        int
	slotNum           int
	addTaskChannel    chan task
	removeTaskChannel chan string
	stopChannel       chan bool
}

type task struct {
	delay  time.Duration
	circle int
	key    string
	job    func()
}

func New(interval time.Duration, slotNum int) *TimeWheel {
	if interval <= 0 || slotNum <= 0 {
		return nil
	}

	tw := &TimeWheel{
		interval:          interval,
		slots:             make([]*list.List, slotNum),
		timer:             make(map[string]*location),
		currentPos:        0,
		slotNum:           slotNum,
		addTaskChannel:    make(chan task),
		removeTaskChannel: make(chan string),
		stopChannel:       make(chan bool),
	}
	tw.initSlots()

	return tw
}

func (tw *TimeWheel) initSlots() {
	for i := 0; i < tw.slotNum; i++ {
		tw.slots[i] = list.New()
	}
}

func (tw *TimeWheel) Start() {
	tw.ticker = time.NewTicker(tw.interval)
	go tw.start()
}

func (tw *TimeWheel) Stop() {
	tw.stopChannel <- true
}

func (tw *TimeWheel) AddJob(delay time.Duration, key string, job func()) {
	if delay < 0 {
		return
	}
	tw.addTaskChannel <- task{delay: delay, key: key, job: job}
}

func (tw *TimeWheel) RemoveJob(key string) {
	if key == "" {
		return
	}
	tw.removeTaskChannel <- key
}

func (tw *TimeWheel) start() {
	for {
		select {
		case <-tw.ticker.C:
			tw.tickHandler()
		case task := <-tw.addTaskChannel:
			tw.addTask(&task)
		case key := <-tw.removeTaskChannel:
			tw.removeTask(key)
		case <-tw.stopChannel:
			tw.ticker.Stop()
			return
		}
	}
}

func (tw *TimeWheel) tickHandler() {
	l := tw.slots[tw.currentPos]
	if tw.currentPos == tw.slotNum-1 {
		tw.currentPos = 0
	} else {
		tw.currentPos++
	}
	go tw.scanAndRunTask(l)
}

func (tw *TimeWheel) scanAndRunTask(l *list.List) {
	for e := l.Front(); e != nil; {
		task := e.Value.(*task)
		if task.circle > 0 {
			task.circle--
			e = e.Next()
			continue
		}

		go func() {
			defer func() {
				if err := recover(); err != nil {
					logger.Error(err)
				}
			}()
			job := task.job
			job()
		}()
		next := e.Next()
		l.Remove(e)
		if task.key != "" {
			delete(tw.timer, task.key)
		}
		e = next
	}
}

func (tw *TimeWheel) addTask(task *task) {
	pos, circle := tw.getPositionAndCircle(task.delay)
	task.circle = circle

	e := tw.slots[pos].PushBack(task)
	loc := &location{
		slot:  pos,
		etask: e,
	}
	if task.key != "" {
		_, ok := tw.timer[task.key]
		if ok {
			tw.removeTask(task.key)
		}
	}
	tw.timer[task.key] = loc
}

func (tw *TimeWheel) getPositionAndCircle(d time.Duration) (pos int, circle int) {
	delaySeconds := int(d.Seconds())
	intervalSeconds := int(tw.interval.Seconds())
	circle = int(delaySeconds / intervalSeconds / tw.slotNum)
	pos = int(tw.currentPos+delaySeconds/intervalSeconds) % tw.slotNum

	return
}

func (tw *TimeWheel) removeTask(key string) {
	pos, ok := tw.timer[key]
	if !ok {
		return
	}
	l := tw.slots[pos.slot]
	l.Remove(pos.etask)
	delete(tw.timer, key)
}
