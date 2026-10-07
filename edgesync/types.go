// Package edgesync 实现云端与边缘节点之间的声明式工作负载同步机制。
package edgesync

import "fmt"

// Version 是工作负载期望状态的单调递增版本号，可直接比较。
type Version uint64

// Compare 比较两个版本：-1 表示小于，0 表示相等，1 表示大于。
func (v Version) Compare(o Version) int {
	switch {
	case v < o:
		return -1
	case v > o:
		return 1
	default:
		return 0
	}
}

// Reason 是变更被拒绝的可区分原因。
type Reason string

const (
	ReasonVersionRegression Reason = "VersionRegression" // 版本回退
	ReasonStaleChange       Reason = "StaleChange"       // 基于已失效版本的变更
	ReasonDuplicate         Reason = "Duplicate"         // 重复到达，幂等忽略
	ReasonUnauthorized      Reason = "Unauthorized"      // 未授权的受保护变更
	ReasonQuotaExceeded     Reason = "QuotaExceeded"     // 超出配额
	ReasonConflict          Reason = "Conflict"          // 并发冲突，可重试
	ReasonContentMismatch   Reason = "ContentMismatch"   // 同版本但内容不一致
)

// RejectError 携带可区分的拒绝原因。
type RejectError struct {
	Reason   Reason
	Workload string
	Detail   string
}

func (e *RejectError) Error() string {
	return fmt.Sprintf("%s: workload=%q reason=%s", e.Detail, e.Workload, e.Reason)
}

// WorkloadSpec 是工作负载的声明式规格。
type WorkloadSpec struct {
	Image     string `json:"image"`
	Replicas  int    `json:"replicas"`
	Protected bool   `json:"protected"` // 受保护的工作负载变更需要授权令牌
}

// DesiredState 是云端下发的期望状态。
type DesiredState struct {
	Workload string       `json:"workload"`
	Version  Version      `json:"version"`
	Spec     WorkloadSpec `json:"spec"`
	Token    string       `json:"token,omitempty"` // 受保护变更的授权令牌
}

// LocalChange 是边缘节点本地产生的变更（如状态上报）。
// ID 为幂等键，BaseVersion 为该变更所基于的期望版本。
// Token 用于受保护工作负载变更的授权；Spec 非空时表示边缘请求
// 修改期望规格（如扩缩容），与云端下发一样受授权与配额策略约束。
type LocalChange struct {
	ID          string        `json:"id"`
	Workload    string        `json:"workload"`
	BaseVersion Version       `json:"base_version"`
	Key         string        `json:"key"`
	Value       string        `json:"value"`
	Token       string        `json:"token,omitempty"`
	Spec        *WorkloadSpec `json:"spec,omitempty"`
}

// ApplyResult 描述一次期望状态应用的判定结果。
type ApplyResult struct {
	Applied  bool
	Rejected bool
	Reason   Reason
	Detail   string
}

// Verdict 是单条变更的终局类别，彼此可区分。
type Verdict string

const (
	VerdictApplied   Verdict = "Applied"   // 正常生效
	VerdictDuplicate Verdict = "Duplicate" // 重复到达，幂等忽略
	VerdictRejected  Verdict = "Rejected"  // 被明确拒绝（Reason 给出原因）
)

// ChangeResult 是云端对一条边缘变更的判定结果。
type ChangeResult struct {
	Verdict Verdict
	Reason  Reason // 仅 VerdictRejected 时有意义
	Detail  string
}

// ReplayOutcome 是断连恢复重放中一条暂存变更的逐条结局。
// Seq 为本次重放内的单调序号，保证并发推进版本时结局与变更一一对应。
type ReplayOutcome struct {
	Seq      int     `json:"seq"`
	ChangeID string  `json:"change_id"`
	Workload string  `json:"workload"`
	Verdict  Verdict `json:"verdict"`
	Reason   Reason  `json:"reason,omitempty"`
	Detail   string  `json:"detail,omitempty"`
}

// FieldDiff 描述云端权威内容与边缘实际运行内容的一个字段差异。
type FieldDiff struct {
	Field string `json:"field"`
	Cloud string `json:"cloud"`
	Edge  string `json:"edge"`
}

// ConsistencyReport 是某工作负载云端权威期望与边缘已应用状态的一致性报告。
type ConsistencyReport struct {
	Workload   string      `json:"workload"`
	Consistent bool        `json:"consistent"`
	CloudFound bool        `json:"cloud_found"`
	EdgeFound  bool        `json:"edge_found"`
	Diffs      []FieldDiff `json:"diffs,omitempty"`
}
