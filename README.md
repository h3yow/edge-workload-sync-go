# edge-workload-sync-go

云端与边缘节点之间的**声明式工作负载同步机制**。云端与边缘两端分别以本地
JSON 文件持久化（`edgesync.Store`）模拟，保证断连、乱序下发与并发变更时
最终状态可预期、可解释。

## 架构

```
┌────────────┐   SubmitDesired / UpdateDesired   ┌────────────┐
│   调用方    │ ───────────────────────────────▶ │   Cloud    │──┐
└────────────┘                                   │ (权威期望状态) │  │ DesiredState
                                                 └────────────┘  │ (可乱序/重复)
                                                        ▲        ▼
                                          LocalChange   │   ┌────────────┐
                                          (断连暂存,    └───│    Edge    │
                                           恢复后重放)      │ (已应用状态) │
                                                           └────────────┘
```

- `edgesync/cloud.go` — 云端控制器：权威期望状态、授权校验、配额、并发收敛、变更去重。
- `edgesync/edge.go` — 边缘节点：按序应用期望状态、断连暂存、恢复后按序重放。
- `edgesync/link.go` — 模拟链路：可断连，上行投递失败由边缘暂存。
- `edgesync/store.go` — JSON 文件持久化，写穿透落盘，重启后状态可恢复。

## 同步协议

### 下行：期望状态（Cloud → Edge）

- 每个工作负载的期望状态携带单调递增的 `Version`（uint64，可直接比较）。
- `Cloud.SubmitDesired`：版本必须严格大于当前版本，**允许跨越中间版本**；
  内容完全相同的重复提交幂等成功；同版本不同内容按冲突拒绝。
- `Edge.ApplyDesired` 的判定规则：

  | 到达版本 vs 已应用版本 | 结果 |
  |---|---|
  | 更高（含跳版） | 应用 |
  | 相等 | 幂等忽略（`Duplicate`） |
  | 更低（回退/乱序旧版） | 拒绝（`VersionRegression`） |

### 上行：本地变更（Edge → Cloud）

- 每条 `LocalChange` 携带幂等键 `ID` 与 `BaseVersion`（变更所基于的期望版本）。
- 断连时边缘以最后已确认的期望状态继续运行，变更持久化暂存；
  `Edge.Reconnect` 恢复后**按序重放**。
- `Cloud.ReceiveChange`：
  - `ID` 已生效 → 幂等忽略，不重复生效；
  - `BaseVersion` 低于当前版本 → 拒绝（`StaleChange`），重放时从队列丢弃，
    绝不静默应用。

### 并发冲突收敛

- `Cloud.UpdateDesired(workload, baseVersion, token, mutate)` 提供乐观锁：
  仅当当前版本等于 `baseVersion` 时生效，否则返回 `Conflict`（可重试）。
  并发提交同一基线时恰好一个成功，最终状态唯一。

### 授权与配额

- `Spec.Protected = true` 的工作负载变更必须携带有效管理员令牌，
  否则拒绝（`Unauthorized`）。
- 副本数超过配额上限时拒绝（`QuotaExceeded`）。
- 被拒绝的变更不污染权威状态。

## 错误分类

所有拒绝均以 `*RejectError` 返回，`Reason` 字段可区分：

| Reason | 含义 |
|---|---|
| `VersionRegression` | 期望版本回退 |
| `StaleChange` | 基于已失效版本的边缘变更 |
| `Duplicate` | 重复到达，幂等忽略 |
| `Unauthorized` | 受保护变更未授权 |
| `QuotaExceeded` | 超出副本配额 |
| `Conflict` | 并发冲突（可重试）/ 同版本不同内容 |

## 验证方法

```sh
go build ./...
go test ./... -v        # 日志打印每个用例的输入与判定依据
go test ./... -race     # 并发安全验证
```

测试覆盖（`edgesync/sync_test.go`）：

- `TestDisconnectedReplay` — 断连暂存、按序重放、重复投递幂等；
- `TestStaleChangeRejectedOnReplay` — 失效变更被拒绝而非静默应用；
- `TestOutOfOrderAndVersionJump` — 乱序到达、跨版本跳跃、版本回退拒绝；
- `TestConcurrentConflict` — 并发变更收敛为唯一最终状态；
- `TestUnauthorizedAndQuota` — 未授权与超配额拒绝且原因可区分；
- `TestPersistenceAcrossRestart` — 重启后状态恢复，回退仍被拒绝。
