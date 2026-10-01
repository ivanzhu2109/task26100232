# meeting — 会议参会状态协调库

纯进程内存的 Go 库，由上层直接调用，负责：加入或替换连接、修改媒体状态、
清理连接、读取房间状态，并向各连接的接收方同步一致的参会状态。

## 核心语义

- 房间容量预先指定；同一参会者在同一房间只占一个名额。
- 首次加入默认静音、关摄像头、不共享屏幕；每房间最多 3 路屏幕共享。
- 每次成功连接都有新身份（`Connection.ID`）。同人再次连接会替换旧连接，
  保留媒体状态与名额，满员也可替换；旧连接上的迟到修改/清理返回
  `ErrStaleConnection` 或不生效，绝不影响新连接。
- `UpdateMedia` 整体成功或整体失败；超过共享上限返回 `ErrShareLimit`，
  不改变任何字段。`Leave` 同时释放参会名额与共享名额，断连时机由上层决定。
- 每次可见变化产生连续递增的版本；被拒绝或无变化的操作不产生版本。
- 接收方先收到一份完整快照，之后收到增量；增量版本严格接续上一条已交付
  状态，出现缺口时自动改用更新的完整快照跨过，不倒退。同一连接的回调
  串行执行，绝不重叠。替换后变化只投向新接收方。
- 接收方暂停（回调阻塞）不算退会、不释放名额；消费恢复后，所有当前连接
  最终都能重建出与 `Room.State()` 完全一致的状态。

## 使用示例

```go
room := meeting.NewRoom(10)

// 连接时提供接收回调，先收快照，再收增量。
conn, err := room.Join("alice", func(ev meeting.Event) {
    switch ev.Kind {
    case meeting.EventSnapshot:
        // ev.Snapshot: 某一完整版本的全量状态
    case meeting.EventDelta:
        // ev.Delta: 紧接上一条已交付版本的增量（Upsert / Removed）
    }
})
if err != nil { /* meeting.ErrRoomFull */ }

// 原子修改媒体状态：整体成功或整体失败。
err = conn.UpdateMedia(meeting.MediaState{Muted: false, Camera: true, Sharing: true})
// 超过 3 路共享上限时返回 meeting.ErrShareLimit，且任何字段都不会被改。

// 读取房间当前完整状态（含版本号）。
snap := room.State()

// 断连清理由上层决定；名额与共享名额一起释放。
conn.Leave()

// 网络闪断后重连：替换旧连接，保留媒体状态与名额。
conn2, _ := room.Join("alice", newReceiver)
_ = conn2
```

## 运行测试

```sh
cd meeting
go test ./...        # 功能测试
go test -race ./...  # 并发竞态检测
```

测试覆盖：满员且 3 路共享时替换连接后旧连接的迟到修改/清理（名额与媒体
不被误改）、首个快照交付期间发生媒体修改与连接替换（快照+增量可重建
对应版本的完整状态）、共享上限的原子性、清理释放名额、暂停/恢复后的
最终一致性、回调不重叠等。
