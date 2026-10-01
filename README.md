# meetcoord

进程内存中的会议参会状态协调库（Go），由上层直接调用，不依赖任何外部服务。

## 能力

- `NewRoom(capacity)`：创建容量预先指定的房间。
- `Join(participantID, recv) (connID, err)`：加入或替换连接。同一参会者只占一个名额；
  重复连接会替换旧连接并保留媒体状态与名额（满员也允许替换）；首次加入默认静音、
  关摄像头、不共享屏幕。返回的 `connID` 是本次连接的唯一身份。
- `UpdateMedia(connID, media)`：修改指定连接的摄像头/静音/共享状态，整体成功或整体失败；
  每房间最多 3 人同时共享屏幕，超限返回 `ErrScreenShareLimit` 且不改任何字段。
- `Leave(connID)`：清理指定连接；清理当前连接时同时释放参会名额与共享名额。
  旧连接的迟到修改/清理返回 `ErrStaleConnection` 或静默忽略，不影响新连接。
- `State() Snapshot`：读取房间当前完整状态。

## 状态同步

`Join` 时传入的 `Receiver` 回调先收到完整快照（`Event.Snapshot`），再收到增量
（`Event.Delta`）。同一连接的回调不重叠执行。每次可见变化产生连续递增的版本号；
拒绝或失效操作不产生版本。增量严格紧接该接收方上一次已交付的版本；接收方积压过
久时会用更新的完整快照压缩积压（允许跨版本前进，绝不倒退）。接收方暂停不算退会，
恢复消费后最终收敛到与 `State()` 完全一致的结果。

## 示例

```go
room := meetcoord.NewRoom(10)

recv := func(ev meetcoord.Event) {
    if ev.Snapshot != nil {
        // 全量替换本地视图
    } else {
        // 按 ev.Delta.Kind 应用 Join / Leave / Media 变化
    }
}

connID, err := room.Join("alice", recv)          // 默认静音、关摄像头、不共享
if err != nil { /* ErrRoomFull */ }

err = room.UpdateMedia(connID, meetcoord.MediaState{
    Muted: false, CameraOn: true, ScreenShare: true,
})                                                // 超限返回 ErrScreenShareLimit

connID2, _ := room.Join("alice", recv2)           // 网络恢复后的新连接：替换旧连接
_ = room.UpdateMedia(connID, meetcoord.MediaState{}) // ErrStaleConnection，不影响新连接
room.Leave(connID)                                // 旧连接的迟到清理：静默忽略

room.Leave(connID2)                               // 释放参会名额与共享名额
snap := room.State()                              // 读取房间当前完整状态
```

## 测试

```sh
go test -race ./...
```
