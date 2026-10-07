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
- `Cloud.ReceiveChange` / `Cloud.ApplyChange`：
  - `ID` 已生效 → 幂等忽略，不重复生效；
  - `BaseVersion` 低于当前版本 → 拒绝（`StaleChange`），重放时从队列丢弃，
    绝不静默应用。

### 断连恢复的逐条结局（重放审计）

- `Edge.Reconnect() ([]ReplayOutcome, error)` 按暂存顺序返回每条变更的结局，
  三类可区分：`Applied`（正常生效）、`Duplicate`（重复忽略）、
  `Rejected`（拒绝，`Reason` 给出原因）。
- 每条结局携带全局单调 `Seq` 与变更 `ID`，重放期间云端并发推进版本时
  结局仍与各自变更一一对应（云端版本单调推进，失效判定只增不减）。
- 结局追加到边缘本地持久化的重放日志，`Edge.ReplayLog()` 可随时查阅，
  **重启后依然保留**。

### 边缘变更的策略约束（与云端下发同一套规矩）

- `LocalChange` 新增可选字段：`Token`（授权令牌）与 `Spec`
  （边缘请求的规格变更，如扩缩容）。
- 无论**在线直投**（`ReportLocal` 直连）还是**断连重放**（`Reconnect`），
  都汇入同一个判定入口 `Cloud.ApplyChange`，判定结果无差别：
  - 落在受保护工作负载上（或携带受保护规格）而无有效令牌 → `Unauthorized`；
  - 规格变更副本数超配额 → `QuotaExceeded`。
- 被拒绝的变更不污染权威状态；合规的规格变更提交为新的权威期望版本
  （版本号 +1）。

### 同版本内容漂移识别与一致性查询

- `Edge.ApplyDesired` 收到**版本相同但内容不同**的期望状态时，不再当作
  重复忽略，而是以 `ContentMismatch` 明确拒绝：边缘不被错误内容覆盖，
  也不静默继续，等待云端以更高版本重新下发。
- `Edge.Consistency(workload)` 返回 `ConsistencyReport`：云端权威期望与
  边缘实际运行内容是否一致；不一致时给出逐字段差异
  （`version` / `spec.image` / `spec.replicas` / `spec.protected` / `existence`）。

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
| `Unauthorized` | 受保护变更未授权（云端下发与边缘上报均适用） |
| `QuotaExceeded` | 超出副本配额（云端下发与边缘上报均适用） |
| `Conflict` | 并发冲突（可重试）/ 同版本不同内容 |
| `ContentMismatch` | 边缘侧识别到同版本但内容不一致的下发 |

## 兼容边界

- `Edge.Reconnect` 签名由 `error` 变更为 `([]ReplayOutcome, error)`，
  调用方需接收返回值（结局同时持久化，可经 `ReplayLog` 补查）。
- `LocalChange` 新增的 `Token` / `Spec` 为可选字段，旧数据与旧报文
  反序列化行为不变；但注意：**对受保护工作负载的状态上报现在也需要
  令牌**，此前不受约束的边缘变更若落在受保护对象上将被拒绝。
- `Edge.ApplyDesired` 对"同版本不同内容"的下发，原先按重复幂等忽略，
  现改为 `ContentMismatch` 明确拒绝（不覆盖、不静默）。
- 边缘合规规格变更会使权威期望版本 +1，随后基于旧版本的变更将按
  `StaleChange` 拒绝，与云端下发推进版本的行为一致。

## 验证方法

```sh
go build ./...
go test ./... -v            # 日志打印每个用例的输入与判定依据
go test ./... -race -count=3 # 并发安全验证（竞态检测，多轮稳定）
```

测试覆盖：

- `edgesync/sync_test.go`（既有回归）
  - `TestDisconnectedReplay` — 断连暂存、按序重放、重复投递幂等；
  - `TestStaleChangeRejectedOnReplay` — 失效变更被拒绝而非静默应用；
  - `TestOutOfOrderAndVersionJump` — 乱序到达、跨版本跳跃、版本回退拒绝；
  - `TestConcurrentConflict` — 并发变更收敛为唯一最终状态；
  - `TestUnauthorizedAndQuota` — 未授权与超配额拒绝且原因可区分；
  - `TestPersistenceAcrossRestart` — 重启后状态恢复，回退仍被拒绝。
- `edgesync/replay_test.go`（重放逐条结局）
  - `TestReplayOutcomesPerItem` — 生效/重复/拒绝三类结局逐条可区分；
  - `TestReplayLogPersistedAcrossRestart` — 结局落盘，重启后可查；
  - `TestReplayConcurrentCloudAdvance` — 重放期间云端并发推进版本，
    结局与变更一一对应、序号单调、失效判定只增不减（竞态检测）。
- `edgesync/policy_test.go`（边缘变更策略约束）
  - `TestEdgeChangePolicyOnline` — 在线直投的未授权/超配额拒绝，
    权威状态不被污染，合规变更放行；
  - `TestEdgeChangePolicyReplay` — 断连重放与在线直投判定一致。
- `edgesync/consistency_test.go`（内容漂移识别与一致性查询）
  - `TestContentMismatchDetected` — 同版本不同内容以 `ContentMismatch`
    拒绝，与重复到达可区分，边缘不被覆盖；
  - `TestConsistencyQuery` — 一致/版本落后/同版本内容漂移/单边缺失
    四种场景的逐字段差异报告。
