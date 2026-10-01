package meeting

import (
	"sort"
	"sync"
)

// Room 是一个会议房间，容量在创建时指定。
type Room struct {
	mu           sync.Mutex
	capacity     int
	version      uint64
	participants map[string]*participant
	nextConnID   uint64
}

type participant struct {
	id    string
	media MediaState
	conn  *Connection
}

// NewRoom 创建一个指定容量的房间。
func NewRoom(capacity int) *Room {
	return &Room{capacity: capacity, participants: make(map[string]*participant)}
}

// Join 加入房间并返回本次连接。
//
// 同一参会者再次连接时替换其当前连接，保留媒体状态与已占名额，
// 满员时也允许替换；若原参会者已被清理，则按首次加入处理。
// 首次加入默认静音、关闭摄像头且不共享屏幕。
func (r *Room) Join(participantID string, recv Receiver) (*Connection, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	p, exists := r.participants[participantID]
	if !exists {
		if len(r.participants) >= r.capacity {
			return nil, ErrRoomFull
		}
		p = &participant{id: participantID, media: MediaState{Muted: true}}
		r.participants[participantID] = p
	}

	r.nextConnID++
	conn := newConnection(r.nextConnID, r, participantID, recv)
	if old := p.conn; old != nil {
		old.detach()
	}
	p.conn = conn

	if !exists {
		// 可见变化：新参会者加入。
		r.version++
		conn.attachSnapshot(r.snapshotLocked())
		r.broadcastLocked(Delta{
			Version: r.version,
			Upsert:  []ParticipantState{{ID: p.id, Media: p.media}},
		})
	} else {
		conn.attachSnapshot(r.snapshotLocked())
	}
	return conn, nil
}

// State 读取房间当前完整状态。
func (r *Room) State() Snapshot {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.snapshotLocked()
}

func (r *Room) snapshotLocked() Snapshot {
	s := Snapshot{Version: r.version, Participants: make([]ParticipantState, 0, len(r.participants))}
	for _, p := range r.participants {
		s.Participants = append(s.Participants, ParticipantState{ID: p.id, Media: p.media})
	}
	sort.Slice(s.Participants, func(i, j int) bool {
		return s.Participants[i].ID < s.Participants[j].ID
	})
	return s
}

func (r *Room) broadcastLocked(d Delta) {
	for _, p := range r.participants {
		p.conn.enqueueDelta(d)
	}
}
