package meeting

// Connection 表示一次成功连接，具有可区分的新身份。
// 媒体修改与清理都归属于具体连接：旧连接的迟到操作不会影响新连接。
type Connection struct {
	id   uint64
	room *Room
	pid  string
	disp *dispatcher

	// closed 为 true 表示连接已失效（被替换或被清理），
	// 之后该连接上的操作返回 err。仅能在 room.mu 保护下访问。
	closed bool
	err    error
}

func newConnection(id uint64, room *Room, pid string, recv Receiver) *Connection {
	return &Connection{id: id, room: room, pid: pid, disp: newDispatcher(recv, room.State)}
}

// ID 返回本次连接的唯一身份。
func (c *Connection) ID() uint64 { return c.id }

// ParticipantID 返回该连接所属参会者。
func (c *Connection) ParticipantID() string { return c.pid }

func (c *Connection) attachSnapshot(s Snapshot) { c.disp.enqueueSnapshot(s) }

func (c *Connection) enqueueDelta(d Delta) { c.disp.enqueueDelta(d) }

// detach 在连接被替换或清理时调用，需在 room.mu 保护下使用。
func (c *Connection) detach() {
	c.closed = true
	if c.err == nil {
		c.err = ErrStaleConnection
	}
	c.disp.close()
}

// UpdateMedia 原子地修改当前连接的媒体状态：整体成功或整体失败。
// 超过共享上限的请求返回 ErrShareLimit，且不改变任何字段。
func (c *Connection) UpdateMedia(m MediaState) error {
	r := c.room
	r.mu.Lock()
	defer r.mu.Unlock()

	if err := c.checkCurrentLocked(); err != nil {
		return err
	}
	p := r.participants[c.pid]
	if m.Sharing && !p.media.Sharing && r.shareCountLocked() >= MaxScreenShares {
		return ErrShareLimit
	}
	if p.media == m {
		return nil // 无可见变化，不产生版本
	}
	p.media = m
	r.version++
	r.broadcastLocked(Delta{
		Version: r.version,
		Upsert:  []ParticipantState{{ID: p.id, Media: p.media}},
	})
	return nil
}

// Leave 清理当前连接，参会名额与共享名额一起释放。
// 旧连接上的迟到清理不会影响已替换的新连接。
func (c *Connection) Leave() {
	r := c.room
	r.mu.Lock()
	defer r.mu.Unlock()

	if c.closed {
		return
	}
	c.err = ErrConnectionClosed
	if p, ok := r.participants[c.pid]; ok && p.conn == c {
		delete(r.participants, c.pid)
		r.version++
		r.broadcastLocked(Delta{Version: r.version, Removed: []string{c.pid}})
	}
	c.detach()
}

// checkCurrentLocked 确认该连接仍是参会者的当前连接。
func (c *Connection) checkCurrentLocked() error {
	if c.closed {
		return c.err
	}
	p, ok := c.room.participants[c.pid]
	if !ok || p.conn != c {
		return ErrStaleConnection
	}
	return nil
}

func (r *Room) shareCountLocked() int {
	n := 0
	for _, p := range r.participants {
		if p.media.Sharing {
			n++
		}
	}
	return n
}
