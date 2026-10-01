package meeting

import "sync"

// dispatcher 负责向单个连接的接收方串行投递事件。
// 保证：先快照后增量；增量版本紧接上一条已交付状态；
// 版本缺口用更新的完整快照跨过，不倒退、不伪造连续流。
type dispatcher struct {
	recv       Receiver
	snapshotFn func() Snapshot

	mu        sync.Mutex
	cond      *sync.Cond
	queue     []queuedEvent
	enqueued  uint64 // 已入队事件的最新版本
	delivered uint64 // 已交付事件的最新版本
	started   bool   // 是否已交付过任何事件
	closed    bool
}

type eventKind int

const (
	kindSnapshot eventKind = iota
	kindDelta
	kindSnapshotRequest // 版本缺口，投递时物化为最新完整快照
)

type queuedEvent struct {
	kind eventKind
	snap Snapshot
	del  Delta
}

func newDispatcher(recv Receiver, snapshotFn func() Snapshot) *dispatcher {
	d := &dispatcher{recv: recv, snapshotFn: snapshotFn}
	d.cond = sync.NewCond(&d.mu)
	go d.run()
	return d
}

// enqueueSnapshot 用一份完整快照替换所有积压事件。
func (d *dispatcher) enqueueSnapshot(s Snapshot) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.closed {
		return
	}
	d.queue = []queuedEvent{{kind: kindSnapshot, snap: s}}
	d.enqueued = s.Version
	d.cond.Signal()
}

func (d *dispatcher) enqueueDelta(del Delta) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.closed {
		return
	}
	switch {
	case del.Version <= d.enqueued:
		// 已覆盖的版本（例如加入时快照已包含自身变化），忽略。
	case del.Version == d.enqueued+1:
		d.queue = append(d.queue, queuedEvent{kind: kindDelta, del: del})
		d.enqueued = del.Version
	default:
		// 版本缺口：用更新的完整快照跨过积压版本。
		d.queue = []queuedEvent{{kind: kindSnapshotRequest}}
		d.enqueued = del.Version
	}
	d.cond.Signal()
}

func (d *dispatcher) close() {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.closed = true
	d.cond.Signal()
}

func (d *dispatcher) run() {
	for {
		d.mu.Lock()
		for len(d.queue) == 0 && !d.closed {
			d.cond.Wait()
		}
		if len(d.queue) == 0 && d.closed {
			d.mu.Unlock()
			return
		}
		ev := d.queue[0]
		d.queue = d.queue[1:]
		d.mu.Unlock()

		switch ev.kind {
		case kindSnapshot:
			d.deliverSnapshot(ev.snap)
		case kindDelta:
			if d.started && ev.del.Version == d.delivered+1 {
				del := ev.del
				d.recv(Event{Kind: EventDelta, Version: del.Version, Delta: &del})
				d.delivered = del.Version
			} else {
				// 无法连续交付，退化为最新完整快照。
				d.deliverFreshSnapshot()
			}
		case kindSnapshotRequest:
			d.deliverFreshSnapshot()
		}
	}
}

func (d *dispatcher) deliverSnapshot(s Snapshot) {
	snap := s
	d.recv(Event{Kind: EventSnapshot, Version: snap.Version, Snapshot: &snap})
	d.delivered = snap.Version
	d.started = true
}

// deliverFreshSnapshot 取当前最新完整快照投递，用于跨过积压或缺失的版本。
func (d *dispatcher) deliverFreshSnapshot() {
	snap := d.snapshotFn()
	if d.started && snap.Version <= d.delivered {
		return // 不倒退
	}
	d.deliverSnapshot(snap)
}
