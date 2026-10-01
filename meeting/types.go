package meeting

import "errors"

var (
	ErrRoomFull         = errors.New("meeting: room is full")
	ErrShareLimit       = errors.New("meeting: screen share limit reached")
	ErrStaleConnection  = errors.New("meeting: connection has been replaced or closed")
	ErrConnectionClosed = errors.New("meeting: connection is closed")
)

const MaxScreenShares = 3

// MediaState 描述一个参会者的媒体状态。
type MediaState struct {
	Muted   bool
	Camera  bool
	Sharing bool
}

// ParticipantState 是某个参会者在某一版本的完整可见状态。
type ParticipantState struct {
	ID    string
	Media MediaState
}

// Snapshot 是房间在某个完整版本上的全量状态。
type Snapshot struct {
	Version      uint64
	Participants []ParticipantState
}

// Delta 是相邻两个版本之间的可见变化。
type Delta struct {
	Version uint64
	Upsert  []ParticipantState
	Removed []string
}

// EventKind 区分投递给接收方的事件类型。
type EventKind int

const (
	EventSnapshot EventKind = iota
	EventDelta
)

// Event 是投递给接收方的消息：要么是完整快照，要么是增量。
type Event struct {
	Kind     EventKind
	Version  uint64
	Snapshot *Snapshot
	Delta    *Delta
}

// Receiver 由调用方在连接时提供，用于接收快照与增量。
// 同一连接的回调保证串行，不会重叠。
type Receiver func(Event)
