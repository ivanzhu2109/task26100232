package meeting

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// recorder 记录投递到接收方的全部事件；gate 非空时回调会被阻塞，用于模拟接收方暂停。
type recorder struct {
	mu     sync.Mutex
	events []Event
	gate   chan struct{}
}

func newRecorder() *recorder { return &recorder{} }

func newGatedRecorder() (*recorder, chan struct{}) {
	gate := make(chan struct{})
	return &recorder{gate: gate}, gate
}

func (r *recorder) recv(ev Event) {
	if r.gate != nil {
		<-r.gate
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	r.events = append(r.events, ev)
}

func (r *recorder) snapshot() []Event {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]Event, len(r.events))
	copy(out, r.events)
	return out
}

// waitEvents 等待至少 n 条事件后返回快照。
func (r *recorder) waitEvents(t *testing.T, n int) []Event {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		evs := r.snapshot()
		if len(evs) >= n {
			return evs
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %d events, got %d", n, len(r.snapshot()))
	return nil
}

// reconstruct 从首个快照开始依次应用增量，重建最终状态。
// 校验：首条必须是快照；增量版本必须紧接上一条已交付版本。
func reconstruct(t *testing.T, evs []Event) Snapshot {
	t.Helper()
	if len(evs) == 0 {
		t.Fatal("no events")
	}
	if evs[0].Kind != EventSnapshot {
		t.Fatalf("first event must be a snapshot, got %v", evs[0].Kind)
	}
	cur := *evs[0].Snapshot
	state := make(map[string]MediaState)
	for _, p := range cur.Participants {
		state[p.ID] = p.Media
	}
	for _, ev := range evs[1:] {
		switch ev.Kind {
		case EventSnapshot:
			if ev.Snapshot.Version <= cur.Version {
				t.Fatalf("snapshot version %d does not advance past %d", ev.Snapshot.Version, cur.Version)
			}
			cur = *ev.Snapshot
			state = make(map[string]MediaState)
			for _, p := range cur.Participants {
				state[p.ID] = p.Media
			}
		case EventDelta:
			if ev.Delta.Version != cur.Version+1 {
				t.Fatalf("delta version %d does not follow %d", ev.Delta.Version, cur.Version)
			}
			for _, u := range ev.Delta.Upsert {
				state[u.ID] = u.Media
			}
			for _, id := range ev.Delta.Removed {
				delete(state, id)
			}
			cur.Version = ev.Delta.Version
			cur.Participants = cur.Participants[:0]
			for id, m := range state {
				cur.Participants = append(cur.Participants, ParticipantState{ID: id, Media: m})
			}
		}
	}
	return cur
}

func mediaMap(s Snapshot) map[string]MediaState {
	m := make(map[string]MediaState)
	for _, p := range s.Participants {
		m[p.ID] = p.Media
	}
	return m
}

func equalMedia(a, b map[string]MediaState) bool {
	if len(a) != len(b) {
		return false
	}
	for k, v := range a {
		if bv, ok := b[k]; !ok || bv != v {
			return false
		}
	}
	return true
}

// TestLateOpsFromReplacedConnection 满员且已有3路共享时替换连接，
// 旧连接的迟到媒体修改与清理不得影响名额与媒体状态。
func TestLateOpsFromReplacedConnection(t *testing.T) {
	room := NewRoom(3)
	recA := newRecorder()
	connA1, err := room.Join("a", recA.recv)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := room.Join("b", newRecorder().recv); err != nil {
		t.Fatal(err)
	}
	if _, err := room.Join("c", newRecorder().recv); err != nil {
		t.Fatal(err)
	}
	sharing := MediaState{Muted: false, Camera: true, Sharing: true}
	if err := connA1.UpdateMedia(sharing); err != nil {
		t.Fatal(err)
	}
	// b、c 也开启共享，达到 3 路上限（重新加入拿连接句柄）。
	recB, recC := newRecorder(), newRecorder()
	connB, _ := room.Join("b", recB.recv)
	connC, _ := room.Join("c", recC.recv)
	if err := connB.UpdateMedia(sharing); err != nil {
		t.Fatal(err)
	}
	if err := connC.UpdateMedia(sharing); err != nil {
		t.Fatal(err)
	}

	base := room.State()
	if len(base.Participants) != 3 {
		t.Fatalf("expected 3 participants, got %d", len(base.Participants))
	}

	// a 再次连接：满员也允许替换，媒体状态与名额保留。
	recA2 := newRecorder()
	connA2, err := room.Join("a", recA2.recv)
	if err != nil {
		t.Fatalf("replace join must be allowed on full room: %v", err)
	}
	if connA2.ID() == connA1.ID() {
		t.Fatal("each successful connection must have a distinct identity")
	}
	if got := room.State(); got.Version != base.Version {
		t.Fatalf("replace without visible change must not bump version: %d -> %d", base.Version, got.Version)
	}
	if m := mediaMap(room.State())["a"]; m != sharing {
		t.Fatalf("media state must be preserved across replace, got %+v", m)
	}

	// 旧连接的迟到媒体修改：必须失败且不改任何状态。
	if err := connA1.UpdateMedia(MediaState{}); err != ErrStaleConnection {
		t.Fatalf("expected ErrStaleConnection, got %v", err)
	}
	// 旧连接的迟到清理：不得释放名额与共享名额。
	connA1.Leave()

	got := room.State()
	if got.Version != base.Version {
		t.Fatalf("stale ops must not produce versions: %d -> %d", base.Version, got.Version)
	}
	if !equalMedia(mediaMap(got), mediaMap(base)) {
		t.Fatalf("stale ops changed room state: %+v", got.Participants)
	}
	if m := mediaMap(got)["a"]; !m.Sharing {
		t.Fatal("stale leave must not release the screen share")
	}

	// 名额仍被占满，新参会者无法加入。
	if _, err := room.Join("d", newRecorder().recv); err != ErrRoomFull {
		t.Fatalf("expected ErrRoomFull, got %v", err)
	}

	// 新连接正常工作：关闭共享释放名额。
	if err := connA2.UpdateMedia(MediaState{Muted: true}); err != nil {
		t.Fatal(err)
	}
	if mediaMap(room.State())["a"].Sharing {
		t.Fatal("new connection must be able to update media")
	}
}

// TestSnapshotAndDeltaReconstruction 首个快照交付期间发生媒体修改和连接替换，
// 交付的快照与增量必须能重建对应版本的完整状态。
func TestSnapshotAndDeltaReconstruction(t *testing.T) {
	room := NewRoom(4)
	if _, err := room.Join("x", newRecorder().recv); err != nil {
		t.Fatal(err)
	}

	// a 的旧接收方在首个快照交付时被卡住。
	recA1, gate := newGatedRecorder()
	connA1, err := room.Join("a", recA1.recv)
	if err != nil {
		t.Fatal(err)
	}

	// 快照在途期间：媒体修改 + 连接替换。
	if err := connA1.UpdateMedia(MediaState{Muted: false, Camera: true}); err != nil {
		t.Fatal(err)
	}
	recA2 := newRecorder()
	connA2, err := room.Join("a", recA2.recv)
	if err != nil {
		t.Fatal(err)
	}
	// 替换后再产生一些变化。
	if err := connA2.UpdateMedia(MediaState{Muted: false, Camera: true, Sharing: true}); err != nil {
		t.Fatal(err)
	}
	if _, err := room.Join("y", newRecorder().recv); err != nil {
		t.Fatal(err)
	}
	close(gate) // 恢复旧接收方，允许在途快照完成。

	// 新接收方：先收到完整快照，再收到连续增量，可重建出与房间一致的状态。
	want := room.State()
	deadline := time.Now().Add(3 * time.Second)
	var got Snapshot
	for time.Now().Before(deadline) {
		evs := recA2.snapshot()
		if len(evs) > 0 {
			got = reconstruct(t, evs)
			if got.Version == want.Version {
				break
			}
		}
		time.Sleep(5 * time.Millisecond)
	}
	if got.Version != want.Version {
		t.Fatalf("reconstructed version %d, want %d", got.Version, want.Version)
	}
	if !equalMedia(mediaMap(got), mediaMap(want)) {
		t.Fatalf("reconstructed %+v, want %+v", got.Participants, want.Participants)
	}

	// 替换后旧接收方不再收到新的房间变化（只有在途数据允许完成）。
	time.Sleep(50 * time.Millisecond)
	n1 := len(recA1.snapshot())
	time.Sleep(50 * time.Millisecond)
	if n2 := len(recA1.snapshot()); n2 != n1 {
		t.Fatalf("old receiver kept receiving events after replace: %d -> %d", n1, n2)
	}
}

// TestShareLimitIsAtomic 超过共享上限的请求整体失败，不顺带改变其他字段。
func TestShareLimitIsAtomic(t *testing.T) {
	room := NewRoom(4)
	sharing := MediaState{Camera: true, Sharing: true}
	for _, id := range []string{"a", "b", "c"} {
		conn, err := room.Join(id, newRecorder().recv)
		if err != nil {
			t.Fatal(err)
		}
		if err := conn.UpdateMedia(sharing); err != nil {
			t.Fatal(err)
		}
	}
	connD, err := room.Join("d", newRecorder().recv)
	if err != nil {
		t.Fatal(err)
	}
	before := room.State()
	err = connD.UpdateMedia(MediaState{Muted: false, Camera: true, Sharing: true})
	if err != ErrShareLimit {
		t.Fatalf("expected ErrShareLimit, got %v", err)
	}
	after := room.State()
	if after.Version != before.Version {
		t.Fatal("rejected update must not produce a version")
	}
	if m := mediaMap(after)["d"]; m != (MediaState{Muted: true}) {
		t.Fatalf("rejected update must not change any field, got %+v", m)
	}
}

// TestLeaveReleasesSeatAndShare 清理当前连接时，参会名额与共享名额一起释放。
func TestLeaveReleasesSeatAndShare(t *testing.T) {
	room := NewRoom(2)
	recA := newRecorder()
	connA, _ := room.Join("a", recA.recv)
	connB, _ := room.Join("b", newRecorder().recv)
	if err := connA.UpdateMedia(MediaState{Sharing: true}); err != nil {
		t.Fatal(err)
	}
	connA.Leave()
	if got := room.State(); len(got.Participants) != 1 || got.Participants[0].ID != "b" {
		t.Fatalf("leave must release the seat, got %+v", got.Participants)
	}
	// 名额已释放，a 再次连接按首次加入处理（默认静音）。
	connA2, err := room.Join("a", newRecorder().recv)
	if err != nil {
		t.Fatal(err)
	}
	_ = connA2
	if m := mediaMap(room.State())["a"]; m != (MediaState{Muted: true}) {
		t.Fatalf("rejoin after cleanup must be treated as first join, got %+v", m)
	}
	// 重复清理幂等。
	connA.Leave()
	connB.Leave()
	if got := room.State(); len(got.Participants) != 1 {
		t.Fatalf("unexpected participants %+v", got.Participants)
	}
}

// TestPauseAndResume 接收方暂停不算退会；恢复后最终与读取房间状态一致。
func TestPauseAndResume(t *testing.T) {
	room := NewRoom(3)
	recSlow, gate := newGatedRecorder()
	if _, err := room.Join("slow", recSlow.recv); err != nil {
		t.Fatal(err)
	}
	connFast, err := room.Join("fast", newRecorder().recv)
	if err != nil {
		t.Fatal(err)
	}
	// 暂停期间产生一系列变化。
	if err := connFast.UpdateMedia(MediaState{Camera: true, Sharing: true}); err != nil {
		t.Fatal(err)
	}
	connTmp, err := room.Join("tmp", newRecorder().recv)
	if err != nil {
		t.Fatal(err)
	}
	connTmp.Leave()
	if err := connFast.UpdateMedia(MediaState{Muted: true, Camera: true}); err != nil {
		t.Fatal(err)
	}
	close(gate) // 消费恢复。

	want := room.State()
	deadline := time.Now().Add(3 * time.Second)
	var got Snapshot
	for time.Now().Before(deadline) {
		evs := recSlow.snapshot()
		if len(evs) > 0 {
			got = reconstruct(t, evs)
			if got.Version == want.Version {
				break
			}
		}
		time.Sleep(5 * time.Millisecond)
	}
	if got.Version != want.Version || !equalMedia(mediaMap(got), mediaMap(want)) {
		t.Fatalf("final state mismatch: got v%d %+v, want v%d %+v",
			got.Version, got.Participants, want.Version, want.Participants)
	}
	// 暂停期间名额未被释放：房间仍能容纳 slow/fast 之外的一人。
	if _, err := room.Join("third", newRecorder().recv); err != nil {
		t.Fatalf("paused receiver must keep its seat: %v", err)
	}
}

// TestCallbacksNotOverlapping 同一连接的回调不得重叠。
func TestCallbacksNotOverlapping(t *testing.T) {
	room := NewRoom(2)
	var inFlight int32
	var maxSeen int32
	rec := func(ev Event) {
		n := atomic.AddInt32(&inFlight, 1)
		for {
			m := atomic.LoadInt32(&maxSeen)
			if n <= m || atomic.CompareAndSwapInt32(&maxSeen, m, n) {
				break
			}
		}
		time.Sleep(time.Millisecond)
		atomic.AddInt32(&inFlight, -1)
	}
	connA, _ := room.Join("a", rec)
	connB, _ := room.Join("b", newRecorder().recv)
	var wg sync.WaitGroup
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			_ = connB.UpdateMedia(MediaState{Muted: i%2 == 0, Camera: i%3 == 0})
		}(i)
	}
	wg.Wait()
	_ = connA
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if atomic.LoadInt32(&inFlight) == 0 {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	if m := atomic.LoadInt32(&maxSeen); m > 1 {
		t.Fatalf("callbacks overlapped: max concurrency %d", m)
	}
}
