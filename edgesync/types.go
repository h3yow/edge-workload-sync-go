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
	ReasonSpecDrift         Reason = "SpecDrift"         // 同版本不同内容，两端可能跑偏
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
// 落在受保护工作负载上的变更必须携带有效授权令牌；
// Key 为 "replicas" 时 Value 按副本数解析并受配额约束。
type LocalChange struct {
	ID          string  `json:"id"`
	Workload    string  `json:"workload"`
	BaseVersion Version `json:"base_version"`
	Key         string  `json:"key"`
	Value       string  `json:"value"`
	Token       string  `json:"token,omitempty"` // 受保护工作负载变更的授权令牌
}

// ReceiveResult 描述云端对一条边缘变更的判定结果，
// 使调用方能区分"正常生效"与"重复忽略"。
type ReceiveResult struct {
	Applied   bool    `json:"applied"`   // 正常生效
	Duplicate bool    `json:"duplicate"` // ID 已生效过，幂等忽略
	Version   Version `json:"version"`   // 判定时云端该工作负载的当前版本
}

// ReplayOutcome 是断连恢复重放中单条变更的结局类别。
type ReplayOutcome string

const (
	OutcomeApplied   ReplayOutcome = "Applied"   // 被云端正常生效
	OutcomeDuplicate ReplayOutcome = "Duplicate" // 此前已生效，被幂等忽略
	OutcomeRejected  ReplayOutcome = "Rejected"  // 被云端明确拒绝
)

// ReplayRecord 是重放过程中一条暂存变更的逐条结局，持久化在边缘本地，
// 重启后仍可查询。记录携带判定时的云端版本，保证并发推进下不张冠李戴。
type ReplayRecord struct {
	Seq          uint64        `json:"seq"`              // 单调递增的记录序号
	Change       LocalChange   `json:"change"`           // 被重放的变更
	Outcome      ReplayOutcome `json:"outcome"`          // 结局类别
	Reason       Reason        `json:"reason,omitempty"` // Outcome 为 Rejected 时的可区分原因
	Detail       string        `json:"detail,omitempty"` // 判定细节
	CloudVersion Version       `json:"cloud_version"`    // 判定时云端当前版本
}

// ApplyResult 描述一次期望状态应用的判定结果。
type ApplyResult struct {
	Applied  bool
	Rejected bool
	Reason   Reason
	Detail   string
}
