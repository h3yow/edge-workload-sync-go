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

// ApplyDesired 应用云端下发的期望状态。重复到达幂等忽略，
// 允许跨越中间版本，版本回退以 ReasonVersionRegression 明确拒绝。
func (e *Edge) ApplyDesired(d DesiredState) ApplyResult {
	e.mu.Lock()
	defer e.mu.Unlock()

	cur, ok := e.currentLocked(d.Workload)
	if ok {
		switch d.Version.Compare(cur.Version) {
		case 0:
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

// Reconnect 恢复连接后按序重放暂存的本地变更。重放幂等：
// 重复变更被云端去重；已失效变更被拒绝后从队列移除，绝不静默应用。
func (e *Edge) Reconnect() error {
	e.mu.Lock()
	defer e.mu.Unlock()

	pending := e.pendingLocked()
	remaining := pending
	for i, ch := range pending {
		err := e.link.Deliver(ch)
		if err != nil {
			var rej *RejectError
			if !errors.As(err, &rej) {
				// 链路仍不可用：保留当前及后续变更，等待下次重放
				remaining = pending[i:]
				return e.putJSONLocked(pendingKey, remaining)
			}
			// 被明确拒绝（如 StaleChange）：丢弃该变更，继续重放后续
		}
		remaining = pending[i+1:]
	}
	return e.putJSONLocked(pendingKey, remaining)
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

func (e *Edge) putJSONLocked(key string, v any) error {
	raw, err := json.Marshal(v)
	if err != nil {
		return err
	}
	return e.store.Put(key, raw)
}
