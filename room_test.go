package meetcoord

import (
	"sync"
	"testing"
	"time"
)

// recorder 记录投递到接收方的全部事件，可选择阻塞首个快照的交付。
type recorder struct {
	mu     sync.Mutex
	events []Event

	blockFirstSnapshot bool
	started            chan struct{}
	release            chan struct{}
	blockOnce          sync.Once
}

func newRecorder() *recorder { return &recorder{} }

func newBlockingRecorder() *recorder {
	return &recorder{
		blockFirstSnapshot: true,
		started:            make(chan struct{}),
		release:            make(chan struct{}),
	}
}

func (r *recorder) recv(ev Event) {
	if r.blockFirstSnapshot && ev.Snapshot != nil {
		r.blockOnce.Do(func() {
			close(r.started)
			<-r.release
		})
	}
	r.mu.Lock()
	r.events = append(r.events, ev)
	r.mu.Unlock()
}

func (r *recorder) snapshot() []Event {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]Event, len(r.events))
	copy(out, r.events)
	return out
}

// waitFor 轮询直到条件满足或超时。
func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("timeout waiting for %s", what)
}

// applyDelta 在参会状态副本上应用一个增量。
func applyDelta(state []Participant, d Delta) []Participant {
	out := make([]Participant, 0, len(state)+1)
	switch d.Kind {
	case DeltaJoin:
		out = append(out, state...)
		out = append(out, d.Participant)
	case DeltaLeave:
		for _, p := range state {
			if p.ID != d.Participant.ID {
				out = append(out, p)
			}
		}
	case DeltaMedia:
		for _, p := range state {
			if p.ID == d.Participant.ID {
				p.Media = d.Participant.Media
			}
			out = append(out, p)
		}
	}
	return out
}

// participantsEqual 无序比较两个参会状态。
func participantsEqual(a, b []Participant) bool {
	if len(a) != len(b) {
		return false
	}
	ma := make(map[string]MediaState, len(a))
	for _, p := range a {
		ma[p.ID] = p.Media
	}
	for _, p := range b {
		m, ok := ma[p.ID]
		if !ok || m != p.Media {
			return false
		}
	}
	return true
}

// replay 从首个快照开始回放事件流，返回每个版本对应的完整状态。
func replay(t *testing.T, events []Event) map[uint64][]Participant {
	t.Helper()
	if len(events) == 0 || events[0].Snapshot == nil {
		t.Fatalf("event stream must start with a snapshot, got %+v", events)
	}
	states := make(map[uint64][]Participant)
	cur := events[0].Snapshot.Participants
	lastVer := events[0].Snapshot.Version
	states[lastVer] = cur
	for i, ev := range events[1:] {
		switch {
		case ev.Snapshot != nil:
			if ev.Snapshot.Version <= lastVer {
				t.Fatalf("event %d: snapshot version %d regresses from %d", i+1, ev.Snapshot.Version, lastVer)
			}
			cur = ev.Snapshot.Participants
			lastVer = ev.Snapshot.Version
		case ev.Delta != nil:
			if ev.Delta.Version != lastVer+1 {
				t.Fatalf("event %d: delta version %d does not follow %d", i+1, ev.Delta.Version, lastVer)
			}
			cur = applyDelta(cur, *ev.Delta)
			lastVer = ev.Delta.Version
		default:
			t.Fatalf("event %d: empty event", i+1)
		}
		states[lastVer] = cur
	}
	return states
}

func shareOn() MediaState  { return MediaState{Muted: false, CameraOn: true, ScreenShare: true} }
func shareOff() MediaState { return MediaState{Muted: false, CameraOn: true} }

// 满员且已有 3 路共享时替换连接，再对旧连接执行迟到的媒体修改及清理。
func TestReplaceThenStaleOpsDoNotCorruptState(t *testing.T) {
	room := NewRoom(3)
	recs := map[string]*recorder{"a": newRecorder(), "b": newRecorder(), "c": newRecorder()}
	conns := make(map[string]uint64)
	for _, id := range []string{"a", "b", "c"} {
		cid, err := room.Join(id, recs[id].recv)
		if err != nil {
			t.Fatalf("join %s: %v", id, err)
		}
		conns[id] = cid
	}
	// 三人全部开启共享，达到上限。
	for _, id := range []string{"a", "b", "c"} {
		if err := room.UpdateMedia(conns[id], shareOn()); err != nil {
			t.Fatalf("share %s: %v", id, err)
		}
	}
	// 满员：新参会者无法加入。
	if _, err := room.Join("d", newRecorder().recv); err != ErrRoomFull {
		t.Fatalf("expected ErrRoomFull, got %v", err)
	}
	before := room.State()

	// a 重新连接：满员也允许替换，媒体状态与名额保留。
	newRec := newRecorder()
	newConn, err := room.Join("a", newRec.recv)
	if err != nil {
		t.Fatalf("rejoin a: %v", err)
	}
	if newConn == conns["a"] {
		t.Fatal("new connection must have a distinct identity")
	}

	// 旧连接的迟到媒体修改：必须失败且不改变任何状态。
	if err := room.UpdateMedia(conns["a"], MediaState{}); err != ErrStaleConnection {
		t.Fatalf("stale update: expected ErrStaleConnection, got %v", err)
	}
	// 旧连接的迟到清理：不能释放 a 的名额与共享。
	room.Leave(conns["a"])

	after := room.State()
	if after.Version != before.Version {
		t.Fatalf("stale ops must not bump version: before=%d after=%d", before.Version, after.Version)
	}
	if !participantsEqual(after.Participants, before.Participants) {
		t.Fatalf("stale ops corrupted state: before=%+v after=%+v", before.Participants, after.Participants)
	}
	if len(after.Participants) != 3 {
		t.Fatalf("seat leaked: got %d participants", len(after.Participants))
	}

	// 新连接仍是当前连接：可以正常修改媒体。
	if err := room.UpdateMedia(newConn, shareOff()); err != nil {
		t.Fatalf("update on new conn: %v", err)
	}
	st := room.State()
	for _, p := range st.Participants {
		if p.ID == "a" && p.Media.ScreenShare {
			t.Fatal("media update on new conn did not take effect")
		}
	}
	// a 关闭共享后名额释放，d 可以加入。
	if _, err := room.Join("d", newRecorder().recv); err != ErrRoomFull {
		t.Fatalf("room should still be full (a keeps seat), got %v", err)
	}
	room.Leave(newConn)
	if _, err := room.Join("d", newRecorder().recv); err != nil {
		t.Fatalf("seat should be freed after leaving: %v", err)
	}
}

// 首个快照交付期间发生媒体修改和连接替换。
func TestSnapshotInFlightWithMediaChangeAndReplace(t *testing.T) {
	room := NewRoom(4)

	oldRec := newBlockingRecorder()
	oldConn, err := room.Join("a", oldRec.recv)
	if err != nil {
		t.Fatalf("join a: %v", err)
	}
	// 等待首个快照进入交付（回调被阻塞）。
	<-oldRec.started

	// 快照交付期间：b 加入并修改媒体。
	bRec := newRecorder()
	bConn, err := room.Join("b", bRec.recv)
	if err != nil {
		t.Fatalf("join b: %v", err)
	}
	bMedia := MediaState{Muted: false, CameraOn: true}
	if err := room.UpdateMedia(bConn, bMedia); err != nil {
		t.Fatalf("update b: %v", err)
	}
	versionBeforeReplace := room.State().Version

	// 快照交付期间：a 被新连接替换。
	newRec := newRecorder()
	newConn, err := room.Join("a", newRec.recv)
	if err != nil {
		t.Fatalf("replace a: %v", err)
	}

	// 替换后的变化只应投递给新接收方。
	if err := room.UpdateMedia(newConn, shareOn()); err != nil {
		t.Fatalf("update new a: %v", err)
	}

	// 放行旧接收方的首个快照，让在途数据继续完成。
	close(oldRec.release)
	waitFor(t, "old receiver to drain", func() bool { return len(oldRec.snapshot()) >= 3 })
	oldEvents := oldRec.snapshot()

	// 旧接收方：快照 + 替换前已在途的增量，可重建对应版本的完整状态。
	oldStates := replay(t, oldEvents)
	if _, ok := oldStates[1]; !ok {
		t.Fatal("old receiver must start with snapshot at version 1")
	}
	finalOld := oldStates[versionBeforeReplace]
	if finalOld == nil {
		t.Fatalf("old receiver missing state for version %d", versionBeforeReplace)
	}
	want := []Participant{
		{ID: "a", Media: MediaState{Muted: true}},
		{ID: "b", Media: bMedia},
	}
	if !participantsEqual(finalOld, want) {
		t.Fatalf("reconstructed state mismatch: got %+v want %+v", finalOld, want)
	}
	// 替换后的变化不得投递给旧接收方。
	for _, ev := range oldEvents {
		if ev.Delta != nil && ev.Delta.Version > versionBeforeReplace {
			t.Fatalf("old receiver got post-replace delta: %+v", ev.Delta)
		}
	}

	// 新接收方：先收到替换时刻的完整快照，再收到后续增量。
	waitFor(t, "new receiver events", func() bool { return len(newRec.snapshot()) >= 2 })
	newEvents := newRec.snapshot()
	if newEvents[0].Snapshot == nil {
		t.Fatal("new receiver must start with a snapshot")
	}
	if newEvents[0].Snapshot.Version != versionBeforeReplace {
		t.Fatalf("new receiver snapshot version = %d, want %d",
			newEvents[0].Snapshot.Version, versionBeforeReplace)
	}
	newStates := replay(t, newEvents)
	final := room.State()
	got, ok := newStates[final.Version]
	if !ok {
		t.Fatalf("new receiver cannot reconstruct version %d", final.Version)
	}
	if !participantsEqual(got, final.Participants) {
		t.Fatalf("new receiver state mismatch: got %+v want %+v", got, final.Participants)
	}

	// 旧连接的迟到清理不影响新连接。
	room.Leave(oldConn)
	if got := len(room.State().Participants); got != 2 {
		t.Fatalf("stale leave freed a seat: %d participants", got)
	}
}

// 媒体修改整体成功或整体失败；共享超限不得顺带改变其他字段。
func TestMediaUpdateAtomicity(t *testing.T) {
	room := NewRoom(4)
	conns := make(map[string]uint64)
	for _, id := range []string{"a", "b", "c", "d"} {
		cid, err := room.Join(id, newRecorder().recv)
		if err != nil {
			t.Fatalf("join %s: %v", id, err)
		}
		conns[id] = cid
	}
	for _, id := range []string{"a", "b", "c"} {
		if err := room.UpdateMedia(conns[id], shareOn()); err != nil {
			t.Fatalf("share %s: %v", id, err)
		}
	}
	before := room.State()

	// d 请求共享（超限）同时修改其他字段：必须整体失败。
	err := room.UpdateMedia(conns["d"], MediaState{Muted: false, CameraOn: true, ScreenShare: true})
	if err != ErrScreenShareLimit {
		t.Fatalf("expected ErrScreenShareLimit, got %v", err)
	}
	after := room.State()
	if after.Version != before.Version {
		t.Fatal("rejected update must not bump version")
	}
	for _, p := range after.Participants {
		if p.ID == "d" && p.Media != (MediaState{Muted: true}) {
			t.Fatalf("rejected update changed other fields: %+v", p.Media)
		}
	}

	// 有人关闭共享后，d 可以共享。
	if err := room.UpdateMedia(conns["a"], shareOff()); err != nil {
		t.Fatalf("update a: %v", err)
	}
	if err := room.UpdateMedia(conns["d"], shareOn()); err != nil {
		t.Fatalf("d should be able to share now: %v", err)
	}
}

// 接收方暂停不算退会；恢复后最终收敛到与 State 一致的结果。
func TestPausedReceiverEventuallyConsistent(t *testing.T) {
	room := NewRoom(3)
	slow := newBlockingRecorder()
	if _, err := room.Join("slow", slow.recv); err != nil {
		t.Fatalf("join slow: %v", err)
	}
	<-slow.started

	// 暂停期间制造大量变化（超过积压上限，触发快照压缩）。
	other := newRecorder()
	otherConn, err := room.Join("other", other.recv)
	if err != nil {
		t.Fatalf("join other: %v", err)
	}
	for i := 0; i < 100; i++ {
		m := MediaState{Muted: i%2 == 0, CameraOn: i%3 == 0}
		if err := room.UpdateMedia(otherConn, m); err != nil {
			t.Fatalf("update %d: %v", i, err)
		}
	}

	close(slow.release)
	final := room.State()
	waitFor(t, "slow receiver to converge", func() bool {
		evs := slow.snapshot()
		if len(evs) == 0 {
			return false
		}
		last := evs[len(evs)-1]
		v := uint64(0)
		if last.Snapshot != nil {
			v = last.Snapshot.Version
		} else {
			v = last.Delta.Version
		}
		return v == final.Version
	})

	states := replay(t, slow.snapshot())
	got, ok := states[final.Version]
	if !ok {
		t.Fatalf("slow receiver missing final version %d", final.Version)
	}
	if !participantsEqual(got, final.Participants) {
		t.Fatalf("slow receiver diverged: got %+v want %+v", got, final.Participants)
	}
}
