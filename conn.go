package meetcoord

import "sync"

// maxPendingEvents 是单连接积压事件上限，超过后房间会用最新完整快照压缩积压。
const maxPendingEvents = 64

// conn 表示一次成功连接，负责把事件按序、不重叠地投递给接收方。
type conn struct {
	id   uint64
	recv Receiver

	mu          sync.Mutex
	cond        *sync.Cond
	queue       []Event
	lastSent    uint64 // 已投递状态的版本
	enqueuedVer uint64 // 已入队事件的最高版本
	closed      bool
	detached    bool // 已被替换：不再接收新事件，存量事件投递完后退出
}

func newConn(id uint64, recv Receiver) *conn {
	c := &conn{id: id, recv: recv}
	c.cond = sync.NewCond(&c.mu)
	return c
}

// enqueue 追加一个事件（由房间在持锁状态下调用）。
func (c *conn) enqueue(ev Event) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed || c.detached {
		return false
	}
	c.queue = append(c.queue, ev)
	c.enqueuedVer = eventVersion(ev)
	c.cond.Signal()
	return len(c.queue) >= maxPendingEvents
}

// compactTo 用更新的完整快照替换全部积压事件，允许跨过积压版本但不倒退。
func (c *conn) compactTo(snap Snapshot) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed || c.detached || snap.Version <= c.lastSent {
		return
	}
	c.queue = []Event{{Snapshot: &snap}}
	c.enqueuedVer = snap.Version
	c.cond.Signal()
}

// lastEnqueued 返回已入队事件的最高版本。
func (c *conn) lastEnqueued() uint64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.enqueuedVer
}

// close 清理连接：停止投递并退出投递循环。
func (c *conn) close() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.closed = true
	c.queue = nil
	c.cond.Signal()
}

// detach 标记连接已被替换：不再接收新事件，已在途的允许继续完成。
func (c *conn) detach() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.detached = true
	c.cond.Signal()
}

// run 是投递循环：保证同一连接的回调不重叠。
func (c *conn) run() {
	for {
		c.mu.Lock()
		for len(c.queue) == 0 && !c.closed && !c.detached {
			c.cond.Wait()
		}
		if c.closed || (len(c.queue) == 0 && c.detached) {
			c.mu.Unlock()
			return
		}
		ev := c.queue[0]
		c.queue = c.queue[1:]
		lastSent := c.lastSent
		c.mu.Unlock()

		// 增量必须紧接上一次的已交付版本；重复或过期事件直接丢弃。
		if ev.Delta != nil && ev.Delta.Version <= lastSent {
			continue
		}
		c.recv(ev)

		c.mu.Lock()
		c.lastSent = eventVersion(ev)
		c.mu.Unlock()
	}
}

func eventVersion(ev Event) uint64 {
	if ev.Snapshot != nil {
		return ev.Snapshot.Version
	}
	return ev.Delta.Version
}
