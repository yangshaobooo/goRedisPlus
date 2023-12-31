package timewheel

import (
	"container/list"
	"goRedisPlus/lib/logger"
	"time"
)

type location struct { // 一个整形位置，一个指针
	slot  int
	etask *list.Element // 双向链表中的一个元素
}

// TimeWheel can execute job after waiting given duration
type TimeWheel struct {
	interval          time.Duration
	ticker            *time.Ticker
	slots             []*list.List // 双向链表头节点数组
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

// New creates a new time wheel
func New(interval time.Duration, slotNum int) *TimeWheel {
	if interval <= 0 || slotNum <= 0 {
		return nil
	}
	tw := &TimeWheel{
		interval:          interval,
		slots:             make([]*list.List, slotNum),
		timer:             make(map[string]*location),
		currentPos:        0,
		slotNum:           slotNum,         // 位置数量 3600 前面用new调用 参数3600
		addTaskChannel:    make(chan task), // 都是无缓冲的channel，阻塞
		removeTaskChannel: make(chan string),
		stopChannel:       make(chan bool),
	}
	tw.initSlots()

	return tw
}

func (tw *TimeWheel) initSlots() {
	for i := 0; i < tw.slotNum; i++ {
		tw.slots[i] = list.New() // 创建3600个新的链表
	}
}

// Start starts ticker for time wheel
func (tw *TimeWheel) Start() {
	tw.ticker = time.NewTicker(tw.interval) // 一个定时器，每隔internal:1s 时间，发送一个信号
	go tw.start()
}

// Stop stops the time wheel
func (tw *TimeWheel) Stop() {
	tw.stopChannel <- true
}

// AddJob add new job into pending queue
func (tw *TimeWheel) AddJob(delay time.Duration, key string, job func()) {
	if delay < 0 {
		return
	}
	tw.addTaskChannel <- task{delay: delay, key: key, job: job}
}

// RemoveJob add remove job from pending queue
// if job is done or not found, then nothing happened
func (tw *TimeWheel) RemoveJob(key string) {
	if key == "" {
		return
	}
	tw.removeTaskChannel <- key
}

func (tw *TimeWheel) start() {
	for {
		select {
		case <-tw.ticker.C: // 定时器发来的消息
			tw.tickHandler()
		case task := <-tw.addTaskChannel: // 添加任务通道的消息
			tw.addTask(&task)
		case key := <-tw.removeTaskChannel: // 移除任务通道的消息
			tw.removeTask(key)
		case <-tw.stopChannel: // 结束时间轮的消息
			tw.ticker.Stop()
			return
		}
	}
}

func (tw *TimeWheel) tickHandler() {
	l := tw.slots[tw.currentPos] // 获取一个双向链表
	if tw.currentPos == tw.slotNum-1 {
		tw.currentPos = 0
	} else {
		tw.currentPos++
	}
	go tw.scanAndRunTask(l)
}

func (tw *TimeWheel) scanAndRunTask(l *list.List) {
	for e := l.Front(); e != nil; { // 从头节点开始一直到nil
		task := e.Value.(*task) // 类型断言
		if task.circle > 0 {    // 没有circle++ 计数是因为这里我们对存的数进行--
			task.circle--
			e = e.Next() // 下一个节点
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
			delete(tw.timer, task.key) // 这里不清楚这个有什么用
		}
		e = next
	}
}

func (tw *TimeWheel) addTask(task *task) {
	pos, circle := tw.getPositionAndCircle(task.delay)
	task.circle = circle

	e := tw.slots[pos].PushBack(task) // 添加到链表结尾
	loc := &location{
		slot:  pos,
		etask: e,
	}
	if task.key != "" {
		_, ok := tw.timer[task.key] // 如果已经有这个key，删掉重新添加
		if ok {
			tw.removeTask(task.key)
		}
	}
	tw.timer[task.key] = loc
}

func (tw *TimeWheel) getPositionAndCircle(d time.Duration) (pos int, circle int) {
	delaySeconds := int(d.Seconds())
	intervalSeconds := int(tw.interval.Seconds())
	circle = int(delaySeconds / intervalSeconds / tw.slotNum)          // 计算圈数
	pos = int(tw.currentPos+delaySeconds/intervalSeconds) % tw.slotNum // 计算单圈位置

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
