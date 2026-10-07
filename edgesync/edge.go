package edgesync

import (
	"encoding/json"
	"fmt"
	"sync"
)

// Edge 模拟边缘节点：按序应用期望状态，断连期间以最后已确认的
// 期望状态继续运行并暂存本地变更，恢复后按序幂等重放。
type Edge struct {
	mu    sync.Mutex
	store *Store
	link  *Link
}

// NewEdge 创建边缘节点。
func NewEdge(store *Store, link *Link) *Edge {
	return &Edge{store: store, link: link}
}

func appliedKey(w string) string { return "applied/" + w }

const pendingKey = "pending"
const replayLogKey = "replaylog"

// ApplyDesired 应用云端下发的期望状态。重复到达幂等忽略，
// 允许跨越中间版本，版本回退以 ReasonVersionRegression 明确拒绝。
func (e *Edge) ApplyDesired(d DesiredState) ApplyResult {
	e.mu.Lock()
	defer e.mu.Unlock()

	cur, ok := e.currentLocked(d.Workload)
	if ok {
		switch d.Version.Compare(cur.Version) {
		case 0:
			if d.Spec == cur.Spec {
				return ApplyResult{Applied: false, Rejected: false, Reason: ReasonDuplicate,
					Detail: "duplicate desired state ignored"}
			}
			// 同版本但内容不一致：明确识别，不静默继续按老内容运行，
			// 也不贸然覆盖，等待云端以更高版本重新下发
			return ApplyResult{Rejected: true, Reason: ReasonContentMismatch,
				Detail: fmt.Sprintf("version %d already applied with different spec (running=%+v incoming=%+v)",
					d.Version, cur.Spec, d.Spec)}
		case -1:
			return ApplyResult{Rejected: true, Reason: ReasonVersionRegression,
				Detail: fmt.Sprintf("incoming version %d < applied version %d", d.Version, cur.Version)}
		}
		// case 1：乱序到达时只接受更高版本，允许跨版本跳跃
	}
	if err := e.putJSONLocked(appliedKey(d.Workload), d); err != nil {
		return ApplyResult{Rejected: true, Reason: ReasonConflict, Detail: err.Error()}
	}
	return ApplyResult{Applied: true}
}

// ReportLocal 产生一条本地变更：连接正常时立即上报；
// 断连时暂存到持久化队列，待恢复后按序重放。
func (e *Edge) ReportLocal(ch LocalChange) error {
	e.mu.Lock()
	defer e.mu.Unlock()

	if e.link.Connected() {
		// 在线：直接上报（含被云端明确拒绝的情况，错误透传给调用方）
		return e.link.Deliver(ch)
	}
	// 断连：暂存到持久化队列
	pending := e.pendingLocked()
	pending = append(pending, ch)
	return e.putJSONLocked(pendingKey, pending)
}

// Reconnect 恢复连接后按序重放暂存的本地变更，返回每条变更的逐条结局
// （生效/重复/拒绝及原因），并将结局追加到持久化的重放日志，重启后仍可查询。
// 重放幂等：重复变更被云端去重；已失效或违规变更被拒绝后从队列移除，
// 绝不静默应用。重放期间云端可能并发推进版本，每条结局记录的是云端对该条
// 变更 ID 的实际判定，按全局单调序号落盘，不会张冠李戴。
func (e *Edge) Reconnect() ([]ReplayOutcome, error) {
	e.mu.Lock()
	defer e.mu.Unlock()

	pending := e.pendingLocked()
	log := e.replayLogLocked()
	outcomes := []ReplayOutcome{}
	remaining := pending
	for i, ch := range pending {
		res, err := e.link.DeliverWithResult(ch)
		if err != nil {
			// 链路仍不可用或内部失败：保留当前及后续变更，等待下次重放
			remaining = pending[i:]
			break
		}
		outcome := ReplayOutcome{
			Seq:      len(log) + len(outcomes) + 1,
			ChangeID: ch.ID,
			Workload: ch.Workload,
			Verdict:  res.Verdict,
			Reason:   res.Reason,
			Detail:   res.Detail,
		}
		outcomes = append(outcomes, outcome)
		// 被明确拒绝（如 StaleChange/Unauthorized/QuotaExceeded）：
		// 丢弃该变更，继续重放后续
		remaining = pending[i+1:]
	}
	if len(outcomes) > 0 {
		log = append(log, outcomes...)
		if err := e.putJSONLocked(replayLogKey, log); err != nil {
			return outcomes, err
		}
	}
	if err := e.putJSONLocked(pendingKey, remaining); err != nil {
		return outcomes, err
	}
	return outcomes, nil
}

// ReplayLog 返回持久化的重放结局日志（含历史重放，重启后仍可查阅）。
func (e *Edge) ReplayLog() []ReplayOutcome {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.replayLogLocked()
}

// Consistency 查询某工作负载云端权威期望与边缘实际运行内容是否一致，
// 不一致时给出逐字段差异（版本、镜像、副本数、保护标记）。
func (e *Edge) Consistency(workload string) ConsistencyReport {
	e.mu.Lock()
	defer e.mu.Unlock()

	rep := ConsistencyReport{Workload: workload}
	cd, cok := e.link.Cloud().Desired(workload)
	ed, eok := e.currentLocked(workload)
	rep.CloudFound = cok
	rep.EdgeFound = eok
	if !cok || !eok {
		if cok != eok {
			rep.Diffs = append(rep.Diffs, FieldDiff{
				Field: "existence",
				Cloud: presence(cok),
				Edge:  presence(eok),
			})
		}
		rep.Consistent = len(rep.Diffs) == 0
		return rep
	}
	if cd.Version != ed.Version {
		rep.Diffs = append(rep.Diffs, FieldDiff{
			Field: "version",
			Cloud: fmt.Sprintf("%d", cd.Version),
			Edge:  fmt.Sprintf("%d", ed.Version),
		})
	}
	if cd.Spec.Image != ed.Spec.Image {
		rep.Diffs = append(rep.Diffs, FieldDiff{
			Field: "spec.image", Cloud: cd.Spec.Image, Edge: ed.Spec.Image})
	}
	if cd.Spec.Replicas != ed.Spec.Replicas {
		rep.Diffs = append(rep.Diffs, FieldDiff{
			Field: "spec.replicas",
			Cloud: fmt.Sprintf("%d", cd.Spec.Replicas),
			Edge:  fmt.Sprintf("%d", ed.Spec.Replicas),
		})
	}
	if cd.Spec.Protected != ed.Spec.Protected {
		rep.Diffs = append(rep.Diffs, FieldDiff{
			Field: "spec.protected",
			Cloud: fmt.Sprintf("%t", cd.Spec.Protected),
			Edge:  fmt.Sprintf("%t", ed.Spec.Protected),
		})
	}
	rep.Consistent = len(rep.Diffs) == 0
	return rep
}

func presence(ok bool) string {
	if ok {
		return "present"
	}
	return "missing"
}

// Current 返回当前已应用的期望状态（断连期间边缘以此继续运行）。
func (e *Edge) Current(workload string) (DesiredState, bool) {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.currentLocked(workload)
}

// Pending 返回暂存待重放的本地变更。
func (e *Edge) Pending() []LocalChange {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.pendingLocked()
}

func (e *Edge) currentLocked(w string) (DesiredState, bool) {
	var d DesiredState
	raw, ok := e.store.Get(appliedKey(w))
	if !ok {
		return DesiredState{}, false
	}
	if err := json.Unmarshal(raw, &d); err != nil {
		return DesiredState{}, false
	}
	return d, true
}

func (e *Edge) pendingLocked() []LocalChange {
	var pending []LocalChange
	if raw, ok := e.store.Get(pendingKey); ok {
		_ = json.Unmarshal(raw, &pending)
	}
	return pending
}

func (e *Edge) replayLogLocked() []ReplayOutcome {
	var log []ReplayOutcome
	if raw, ok := e.store.Get(replayLogKey); ok {
		_ = json.Unmarshal(raw, &log)
	}
	return log
}

func (e *Edge) putJSONLocked(key string, v any) error {
	raw, err := json.Marshal(v)
	if err != nil {
		return err
	}
	return e.store.Put(key, raw)
}
