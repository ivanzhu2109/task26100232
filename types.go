// Package meetcoord 提供进程内存中的会议参会状态协调能力。
package meetcoord

import "errors"

// MaxScreenSharers 是单个房间允许同时共享屏幕的最大人数。
const MaxScreenSharers = 3

var (
	// ErrRoomFull 表示房间已满且不是替换连接。
	ErrRoomFull = errors.New("meetcoord: room is full")
	// ErrStaleConnection 表示操作所属的连接已失效（被替换或已清理）。
	ErrStaleConnection = errors.New("meetcoord: stale connection")
	// ErrScreenShareLimit 表示共享屏幕人数已达上限。
	ErrScreenShareLimit = errors.New("meetcoord: screen share limit reached")
)

// MediaState 描述一个参会者的媒体状态。
type MediaState struct {
	Muted       bool
	CameraOn    bool
	ScreenShare bool
}

// Participant 是房间中一个参会者的可见状态。
type Participant struct {
	ID    string
	Media MediaState
}

// Snapshot 是房间在某个完整版本上的全部参会状态。
type Snapshot struct {
	Version      uint64
	Participants []Participant
}

// DeltaKind 标识增量变化的类型。
type DeltaKind int

const (
	DeltaJoin DeltaKind = iota + 1
	DeltaLeave
	DeltaMedia
)

// Delta 是房间的一次可见变化，版本号连续递增。
type Delta struct {
	Version     uint64
	Kind        DeltaKind
	Participant Participant // Join/Media 时为变化后的状态；Leave 时仅 ID 有效
}

// Event 是投递给接收方的一条消息：要么是完整快照，要么是增量。
type Event struct {
	Snapshot *Snapshot
	Delta    *Delta
}

// Receiver 是调用方在连接时提供的接收回调。
// 同一连接的回调保证不重叠执行。
type Receiver func(Event)
