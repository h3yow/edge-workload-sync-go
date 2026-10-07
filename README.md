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
- `edgesync/edge.go` — 边缘节点：按序应用期望状态、断连暂存、恢复后按序重放并记录逐条结局。
- `edgesync/link.go` — 模拟链路：可断连，上行投递失败由边缘暂存。
- `edgesync/consistency.go` — 云端权威内容与边缘运行内容的一致性查询。
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
  | 相等且内容一致 | 幂等忽略（`Duplicate`） |
  | 相等但内容不一致 | 拒绝（`SpecDrift`），边缘不静默按老内容运行 |
  | 更低（回退/乱序旧版） | 拒绝（`VersionRegression`） |

### 上行：本地变更（Edge → Cloud）

- 每条 `LocalChange` 携带幂等键 `ID` 与 `BaseVersion`（变更所基于的期望版本）。
- 断连时边缘以最后已确认的期望状态继续运行，变更持久化暂存；
  `Edge.Reconnect` 恢复后**按序重放**。
- `Cloud.ReceiveChange` / `Cloud.ReceiveChangeResult` 的判定顺序
  （云端锁内原子完成，并发推进版本不会张冠李戴）：
  1. 工作负载未知 → 拒绝（`StaleChange`）；
  2. `ID` 已生效过 → 幂等忽略（`Duplicate`），不重复生效；
  3. `BaseVersion` 低于当前版本 → 拒绝（`StaleChange`），重放时从队列丢弃，
     绝不静默应用；
  4. 策略校验（见下节）；
  5. 全部通过才写入状态并记录幂等键。

### 重放逐条结局（Replay Outcomes）

断连恢复后，`Edge.Reconnect` 返回每条暂存变更的结局，并持久化到边缘本地
（`replay/history`），重启后经 `Edge.ReplayHistory` 仍可查询：

| Outcome | 含义 |
|---|---|
| `Applied` | 被云端正常生效 |
| `Duplicate` | 此前已生效，被幂等忽略 |
| `Rejected` | 被云端明确拒绝（`Reason` 字段给出可区分原因） |

每条 `ReplayRecord` 携带判定时的云端版本（`CloudVersion`）与单调递增的
序号（`Seq`）：重放期间云端并发推进版本时，每条记录仍唯一对应一条暂存
变更，不张冠李戴。链路在重放中途再次断开时，已产生的结局先落盘，
未重放的变更保留在队列中等待下次恢复。

### 边缘变更的策略约束

边缘本地产生的变更与云端下发的期望状态受**同等**策略约束，
在线直投（`ReportLocal` 直连）与断连重放（`Reconnect`）两条链路
都经过云端同一判定入口，结果一致：

- 落在 `Spec.Protected = true` 的工作负载上的变更必须携带有效令牌
  （`LocalChange.Token`），否则拒绝（`Unauthorized`）；
- `Key` 为 `"replicas"` 的变更按副本数解析，超过配额拒绝（`QuotaExceeded`）；
- 被拒绝的变更不写入状态、不占用幂等键（修正后重投仍可生效），
  权威状态不被污染。

### 内容一致性校验

- 同版本不同内容的下发被边缘识别为 `SpecDrift` 拒绝，与幂等重复、
  版本回退可区分，边缘不会被静默覆盖。
- `VerifyConsistency(cloud, edge, workload)` 返回某工作负载的一致性结论：
  两端内容摘要（版本+规格的 SHA-256 截断）、是否一致，不一致时
  `Diffs` 给出逐字段差异（version/image/replicas/protected），
  单端缺失也会体现为不一致。

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
| `Unauthorized` | 受保护变更未授权（云端下发与边缘上报同等适用） |
| `QuotaExceeded` | 超出副本配额（云端下发与边缘上报同等适用） |
| `Conflict` | 并发冲突（可重试）/ 同版本不同内容 |
| `SpecDrift` | 同版本不同内容下发到边缘，两端可能跑偏 |

## 兼容边界

- `Edge.Reconnect` 的签名由 `error` 变为 `([]ReplayRecord, error)`：
  返回值新增逐条结局，错误语义不变（仅持久化失败等非判定错误）。
- `Cloud.ReceiveChange` 签名不变；新增 `ReceiveChangeResult` 返回结构化
  判定结果。判定顺序调整为**去重先于失效检查**：已生效过的变更即使
  基准版本已失效也判定为 `Duplicate` 而非 `StaleChange`（语义上它确实
  生效过）；未生效过的失效变更判定不变。
- `LocalChange` 新增 `Token` 字段（`omitempty`）：旧数据反序列化不受影响；
  但落在受保护工作负载上的边缘变更现在必须携带令牌，此前会被放行的
  此类变更将被拒绝（`Unauthorized`）。
- `Edge.ApplyDesired` 对"同版本不同内容"由幂等忽略改为 `SpecDrift` 拒绝：
  依赖旧行为（静默忽略内容漂移）的调用方需要适配。
- 边缘本地新增持久化键 `replay/history`，与既有键不冲突，旧存储文件
  可直接打开。

## 验证方法

```sh
go build ./...
go test ./... -v        # 日志打印每个用例的输入与判定依据
go test ./... -race     # 并发安全验证
```

测试覆盖：

- `edgesync/sync_test.go`（既有，无回归）
  - `TestDisconnectedReplay` — 断连暂存、按序重放、重复投递幂等；
  - `TestStaleChangeRejectedOnReplay` — 失效变更被拒绝而非静默应用；
  - `TestOutOfOrderAndVersionJump` — 乱序到达、跨版本跳跃、版本回退拒绝；
  - `TestConcurrentConflict` — 并发变更收敛为唯一最终状态；
  - `TestUnauthorizedAndQuota` — 未授权与超配额拒绝且原因可区分；
  - `TestPersistenceAcrossRestart` — 重启后状态恢复，回退仍被拒绝。
- `edgesync/replay_test.go`（重放逐条结局）
  - `TestReplayOutcomesRecorded` — 生效/重复/拒绝三类结局逐条可区分；
  - `TestReplayHistoryPersistedAcrossRestart` — 结局持久化，重启后可查，序号单调；
  - `TestReplayConcurrentCloudAdvance` — 重放与云端版本推进并发，结局不张冠李戴（`-race` 验证）。
- `edgesync/policy_test.go`（边缘变更策略约束）
  - `TestEdgeChangePolicyOnline` — 在线直投：未授权/超配额被拒，权威状态不污染；
  - `TestEdgeChangePolicyReplay` — 断连重放：同一批用例判定结果与在线完全一致。
- `edgesync/consistency_test.go`（内容一致性）
  - `TestSpecDriftDetected` — 同版本不同内容识别为 `SpecDrift`，边缘不被覆盖；
  - `TestVerifyConsistency` — 一致/字段级差异/单端缺失三种结论与差异明细。
