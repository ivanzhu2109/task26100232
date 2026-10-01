package meetcoord

import (
	"sort"
	"sync"
)

// Room 是一个容量预先指定的会议房间，全部状态仅保存在进程内存中。
type Room struct {
	mu       sync.Mutex
	capacity int
	version  uint64
	nextConn uint64
	members  map[string]*member // participantID -> member
	conns    map[uint64]*conn   // 全部存活连接（含被替换待清理的）
}

type member struct {
	media MediaState
	conn  *conn // 当前有效连接
}

// NewRoom 创建一个指定容量的房间。
func NewRoom(capacity int) *Room {
	return &Room{
		capacity: capacity,
		members:  make(map[string]*member),
		conns:    make(map[uint64]*conn),
	}
}

// Join 加入或替换连接，返回本次连接的身份。
// 同一参会者重复连接时替换其当前连接，保留媒体状态与名额，满员时也允许替换；
// 新连接先收到完整快照，再收到后续增量。
func (r *Room) Join(participantID string, recv Receiver) (connID uint64, err error) {
	r.mu.Lock()
	r.nextConn++
	connID = r.nextConn
	c := newConn(connID, recv)

	isNew := false
	if m, ok := r.members[participantID]; ok {
		// 替换：保留媒体状态与名额，旧连接不再接收新事件。
		m.conn.detach()
		m.conn = c
	} else {
		if len(r.members) >= r.capacity {
			r.mu.Unlock()
			return 0, ErrRoomFull
		}
		r.members[participantID] = &member{
			media: MediaState{Muted: true}, // 首次加入默认静音、关摄像头、不共享
			conn:  c,
		}
		r.version++
		isNew = true
	}

	r.conns[connID] = c
	// 先入队快照再广播增量，保证新连接的队列以快照开头。
	snap := r.snapshotLocked()
	c.enqueue(Event{Snapshot: &snap})
	if isNew {
		r.broadcastLocked(Delta{
			Version: r.version,
			Kind:    DeltaJoin,
			Participant: Participant{
				ID:    participantID,
				Media: MediaState{Muted: true},
			},
		})
	}
	r.mu.Unlock()

	go c.run()
	return connID, nil
}

// UpdateMedia 修改指定连接的媒体状态，整体成功或整体失败。
// 迟到的旧连接操作返回 ErrStaleConnection，不影响当前连接。
func (r *Room) UpdateMedia(connID uint64, media MediaState) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	m := r.memberOfLocked(connID)
	if m == nil {
		return ErrStaleConnection
	}
	if media.ScreenShare && !m.media.ScreenShare && r.sharersLocked() >= MaxScreenSharers {
		// 超过共享上限：整体失败，不改变任何字段。
		return ErrScreenShareLimit
	}
	if media == m.media {
		return nil // 无可见变化，不产生版本
	}
	m.media = media
	r.version++
	r.broadcastLocked(Delta{
		Version:     r.version,
		Kind:        DeltaMedia,
		Participant: Participant{ID: r.participantOfLocked(connID), Media: media},
	})
	return nil
}

// Leave 清理指定连接；当前连接被清理时释放参会名额与共享名额。
// 旧连接的迟到清理不影响已替换的新连接。
func (r *Room) Leave(connID uint64) {
	r.mu.Lock()
	c, ok := r.conns[connID]
	if !ok {
		r.mu.Unlock()
		return
	}
	delete(r.conns, connID)
	var d *Delta
	if m := r.memberOfLocked(connID); m != nil {
		pid := r.participantOfLocked(connID)
		delete(r.members, pid)
		r.version++
		d = &Delta{
			Version:     r.version,
			Kind:        DeltaLeave,
			Participant: Participant{ID: pid},
		}
		r.broadcastLocked(*d)
	}
	r.mu.Unlock()

	c.close()
}

// State 读取房间当前完整状态。
func (r *Room) State() Snapshot {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.snapshotLocked()
}

// memberOfLocked 返回 connID 所属的当前成员；连接已失效时返回 nil。
func (r *Room) memberOfLocked(connID uint64) *member {
	for _, m := range r.members {
		if m.conn.id == connID {
			return m
		}
	}
	return nil
}

func (r *Room) participantOfLocked(connID uint64) string {
	for pid, m := range r.members {
		if m.conn.id == connID {
			return pid
		}
	}
	return ""
}

func (r *Room) sharersLocked() int {
	n := 0
	for _, m := range r.members {
		if m.media.ScreenShare {
			n++
		}
	}
	return n
}

func (r *Room) snapshotLocked() Snapshot {
	snap := Snapshot{Version: r.version}
	for pid, m := range r.members {
		snap.Participants = append(snap.Participants, Participant{ID: pid, Media: m.media})
	}
	sort.Slice(snap.Participants, func(i, j int) bool {
		return snap.Participants[i].ID < snap.Participants[j].ID
	})
	return snap
}

// broadcastLocked 把增量投递给所有需要它的当前连接；积压过久时用最新快照压缩。
func (r *Room) broadcastLocked(d Delta) {
	var snap *Snapshot
	for _, m := range r.members {
		c := m.conn
		if d.Version <= c.lastEnqueued() {
			continue
		}
		if c.enqueue(Event{Delta: &d}) {
			if snap == nil {
				s := r.snapshotLocked()
				snap = &s
			}
			c.compactTo(*snap)
		}
	}
}
