package edgesync

import (
	"encoding/json"
	"errors"
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
const historyKey = "replay/history"

// ApplyDesired 应用云端下发的期望状态。重复到达幂等忽略，
// 允许跨越中间版本，版本回退以 ReasonVersionRegression 明确拒绝；
// 同版本但内容不一致时以 ReasonSpecDrift 拒绝，边缘不静默按老内容运行。
func (e *Edge) ApplyDesired(d DesiredState) ApplyResult {
	e.mu.Lock()
	defer e.mu.Unlock()

	cur, ok := e.currentLocked(d.Workload)
	if ok {
		switch d.Version.Compare(cur.Version) {
		case 0:
			if d.Spec != cur.Spec {
				return ApplyResult{Rejected: true, Reason: ReasonSpecDrift,
					Detail: fmt.Sprintf("version %d arrives with different spec (running image=%q replicas=%d, incoming image=%q replicas=%d)",
						d.Version, cur.Spec.Image, cur.Spec.Replicas, d.Spec.Image, d.Spec.Replicas)}
			}
			return ApplyResult{Applied: false, Rejected: false, Reason: ReasonDuplicate,
				Detail: "duplicate desired state ignored"}
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

// Reconnect 恢复连接后按序重放暂存的本地变更，返回每条变更的逐条结局。
// 重放幂等：重复变更被云端去重并记为 OutcomeDuplicate；被明确拒绝的变更
// 记为 OutcomeRejected（含可区分原因）后从队列移除，绝不静默应用。
// 每条结局在云端锁内判定后立即落盘到本地历史，重启后可通过 ReplayHistory
// 查询；记录携带判定时的云端版本，重放期间云端并发推进也不会张冠李戴。
func (e *Edge) Reconnect() ([]ReplayRecord, error) {
	e.mu.Lock()
	defer e.mu.Unlock()

	pending := e.pendingLocked()
	var records []ReplayRecord
	remaining := pending
	for i, ch := range pending {
		res, err := e.link.DeliverResult(ch)
		var rej *RejectError
		switch {
		case err == nil:
			// 云端明确判定：生效或幂等忽略
			rec := ReplayRecord{Change: ch, CloudVersion: res.Version}
			if res.Duplicate {
				rec.Outcome = OutcomeDuplicate
				rec.Detail = "change id already applied, ignored idempotently"
			} else {
				rec.Outcome = OutcomeApplied
			}
			records = append(records, rec)
		case errors.As(err, &rej):
			// 被云端明确拒绝（如 StaleChange/Unauthorized/QuotaExceeded）：
			// 记录原因与判定时的云端版本，丢弃该变更，继续重放后续
			records = append(records, ReplayRecord{
				Change:       ch,
				Outcome:      OutcomeRejected,
				Reason:       rej.Reason,
				Detail:       rej.Detail,
				CloudVersion: res.Version,
			})
		default:
			// 链路仍不可用：已产生的结局先落盘，保留当前及后续变更待下次重放
			remaining = pending[i:]
			if perr := e.putJSONLocked(pendingKey, remaining); perr != nil {
				return records, perr
			}
			return records, e.appendHistoryLocked(records)
		}
		remaining = pending[i+1:]
	}
	if err := e.putJSONLocked(pendingKey, remaining); err != nil {
		return records, err
	}
	return records, e.appendHistoryLocked(records)
}

// ReplayHistory 返回历次重放的逐条结局（按记录序号升序），
// 数据持久化在边缘本地，重启后仍可查询。
func (e *Edge) ReplayHistory() []ReplayRecord {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.historyLocked()
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

// historyLocked 读取已持久化的重放结局历史。
func (e *Edge) historyLocked() []ReplayRecord {
	var history []ReplayRecord
	if raw, ok := e.store.Get(historyKey); ok {
		_ = json.Unmarshal(raw, &history)
	}
	return history
}

// appendHistoryLocked 将本次重放产生的结局追加到持久化历史，
// 序号在既有历史基础上单调递增，保证每条记录可唯一指认。
func (e *Edge) appendHistoryLocked(records []ReplayRecord) error {
	if len(records) == 0 {
		return nil
	}
	history := e.historyLocked()
	next := uint64(0)
	if n := len(history); n > 0 {
		next = history[n-1].Seq + 1
	}
	for i := range records {
		records[i].Seq = next
		next++
	}
	return e.putJSONLocked(historyKey, append(history, records...))
}

func (e *Edge) putJSONLocked(key string, v any) error {
	raw, err := json.Marshal(v)
	if err != nil {
		return err
	}
	return e.store.Put(key, raw)
}
