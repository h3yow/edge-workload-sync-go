# edge-workload-sync-go

云端与边缘节点之间的**声明式工作负载同步机制**。云端与边缘两端分别以本地
JSON 文件持久化（`edgesync.Store`，写临时文件 + rename 原子落盘），用于模拟
断连、乱序下发与并发变更场景，并保证最终状态可预期、可解释。

## 同步协议

```
Cloud (权威期望状态)                Edge (边缘节点)
  |-- SetDesired(DesiredState) ---->|  ReceiveDesired: 按版本应用
  |                                 |  断连: 以最后确认的期望状态继续运行,
  |                                 |        本地变更按序暂存 (staged)
  |<-- Replay(Change 按序重放) -----|  恢复连接: 先拉取最新期望状态, 再按序重放
  |-- SubmitChange(直接提交) -------|  连接状态下本地变更直送云端
```

- **DesiredState**：`{WorkloadID, Version, Spec}`，`Version` 为单调可比较的
  整数（`Version.Compare`）。
- **Change**：边缘产生的本地变更，携带幂等键 `ID`、基准版本 `BaseVersion`、
  提出者 `Principal` 与是否受保护 `Protected`。

## 版本规则

| 情形 | 判定 | 原因码 |
|---|---|---|
| 更高版本（允许跨越中间版本） | 应用，最新版本胜出 | — |
| 同版本同内容重复到达 | 幂等空操作 | `duplicate` |
| 更低版本，或同版本不同内容 | 明确拒绝 | `version_regression` |
| 变更的 `BaseVersion` 落后于当前版本 | 拒绝，不静默应用 | `stale_base` |
| 变更 ID 已生效（含重启后） | 幂等拒绝，不重复生效 | `duplicate` |

## 冲突与授权规则

- **并发收敛**：`Cloud` 对所有变更串行化裁决。同一基准版本上的并发变更
  恰好一个胜出（版本 +1），其余被归类为 `stale_base`——无论经哪条路径
  （直连 `SubmitChange` 或边缘 `Replay`），最终状态唯一且可解释。
- **受保护变更**：`Protected=true` 的变更需通过 `Authorizer` 校验，否则
  拒绝为 `unauthorized`。
- **配额**：`Spec.Replicas` 超出配额拒绝为 `quota_exceeded`。
- 校验顺序：幂等 → 基准版本新鲜度 → 授权 → 配额。被拒绝的变更**不消耗**
  其变更 ID（重提时按当时状态重新裁决，不会被误判为 `duplicate`）。
- **断连重放**：暂存变更按提出顺序链式预测基准版本（第 n 个暂存变更基于
  前序变更将产生的版本）；重放时若云端已独立推进，过期变更被 `stale_base`
  拒绝而非静默应用。重放队列中每条变更都有确定结局后才出队，重放可安全
  重复执行。

## 验证方法

```sh
go test ./... -v        # 日志打印每条输入与判定依据
go test -race ./...     # 并发路径竞态检测
```

测试覆盖：

- `TestOutOfOrderAndSkipVersion`：乱序到达、跳版、重复下发、同版本异内容。
- `TestCloudVersionRegression`：云端发布路径的版本回退拒绝。
- `TestDisconnectReplay`：断连暂存、按序重放、重放幂等、过期变更拒绝。
- `TestConcurrentConflict`：16 路并发变更收敛为唯一胜者。
- `TestUnauthorizedAndQuota`：未授权与超配额在两条路径上的一致拒绝。
- `TestPersistence`：期望状态与已应用变更 ID 跨重启持久。

## 已知简化

- 边缘暂存队列保存在内存中（期望状态与已应用变更 ID 均持久化）；生产实现
  应同样持久化暂存队列。
