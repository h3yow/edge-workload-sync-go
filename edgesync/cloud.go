package edgesync

import (
	"encoding/json"
	"sync"
)

// Cloud 模拟云端控制器：持有期望状态的权威副本，负责授权校验、
// 配额检查、并发冲突收敛以及边缘变更的接收与去重。
type Cloud struct {
	mu       sync.Mutex
	store    *Store
	adminTok string
	maxRepl  int // 副本数配额上限
}

// NewCloud 创建云端控制器。adminToken 为受保护变更的授权令牌，
// maxReplicas 为副本配额上限（<=0 表示不限制）。
func NewCloud(store *Store, adminToken string, maxReplicas int) *Cloud {
	return &Cloud{store: store, adminTok: adminToken, maxRepl: maxReplicas}
}

func desiredKey(w string) string { return "desired/" + w }
func statusKey(w string) string  { return "status/" + w }
func changesKey(w string) string { return "changes/" + w }

// SubmitDesired 提交新的期望状态。版本必须严格大于当前版本（允许跨版本跳跃）；
// 完全相同的重复提交幂等成功；版本回退被拒绝；受保护工作负载的变更必须携带
// 有效令牌；副本数不得超过配额。
func (c *Cloud) SubmitDesired(d DesiredState) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	cur, ok := c.desiredLocked(d.Workload)
	if ok {
		switch d.Version.Compare(cur.Version) {
		case 0:
			if d.Spec == cur.Spec {
				return nil // 重复到达：幂等成功，不产生错误状态
			}
			return &RejectError{Reason: ReasonConflict, Workload: d.Workload,
				Detail: "same version with different spec"}
		case -1:
			return &RejectError{Reason: ReasonVersionRegression, Workload: d.Workload,
				Detail: "version regresses below current"}
		}
		// case 1：允许跨越中间版本
	}
	if err := c.checkPolicyLocked(d); err != nil {
		return err
	}
	return c.putDesiredLocked(d)
}

// UpdateDesired 以乐观锁方式更新期望状态：仅当当前版本等于 baseVersion 时
// 变更才生效，否则返回 ReasonConflict（可重试），用于并发变更的收敛。
// 新版本取 baseVersion+1。
func (c *Cloud) UpdateDesired(workload string, baseVersion Version, token string, mutate func(*WorkloadSpec)) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	cur, ok := c.desiredLocked(workload)
	if !ok {
		return &RejectError{Reason: ReasonConflict, Workload: workload,
			Detail: "workload does not exist"}
	}
	if cur.Version != baseVersion {
		return &RejectError{Reason: ReasonConflict, Workload: workload,
			Detail: "concurrent modification detected"}
	}
	next := DesiredState{
		Workload: workload,
		Version:  cur.Version + 1,
		Spec:     cur.Spec,
		Token:    token,
	}
	mutate(&next.Spec)
	if err := c.checkPolicyLocked(next); err != nil {
		return err
	}
	return c.putDesiredLocked(next)
}

// ReceiveChange 接收边缘上报的本地变更。重复 ID 幂等忽略（返回 nil）；
// 基于已失效版本的变更以 ReasonStaleChange 拒绝，绝不静默应用。
// 策略违规（未授权/超配额）同样拒绝。判定逻辑见 ApplyChange。
func (c *Cloud) ReceiveChange(ch LocalChange) error {
	res, err := c.ApplyChange(ch)
	if err != nil {
		return err
	}
	if res.Verdict == VerdictRejected {
		return &RejectError{Reason: res.Reason, Workload: ch.Workload, Detail: res.Detail}
	}
	return nil
}

// ApplyChange 对一条边缘变更给出可区分的判定结果（生效/重复/拒绝及原因）。
// 边缘本地变更与云端下发遵循同一套授权与配额策略：落在受保护工作负载上
// 或携带受保护规格的变更必须携带有效令牌；规格变更的副本数不得超配额。
// 返回的 error 仅表示内部持久化失败（调用方可重试），策略拒绝体现在
// ChangeResult 中。无论在线直投还是断连重放，判定路径完全一致。
func (c *Cloud) ApplyChange(ch LocalChange) (ChangeResult, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	d, ok := c.desiredLocked(ch.Workload)
	if !ok {
		return ChangeResult{Verdict: VerdictRejected, Reason: ReasonStaleChange,
			Detail: "unknown workload"}, nil
	}
	if ch.BaseVersion < d.Version {
		return ChangeResult{Verdict: VerdictRejected, Reason: ReasonStaleChange,
			Detail: "change based on superseded version"}, nil
	}
	applied := c.appliedChangesLocked(ch.Workload)
	if _, dup := applied[ch.ID]; dup {
		return ChangeResult{Verdict: VerdictDuplicate,
			Detail: "duplicate change id, idempotently ignored"}, nil
	}
	if res, bad := c.checkChangePolicyLocked(d, ch); bad {
		return res, nil
	}

	// 通过全部校验：状态键值生效
	if ch.Key != "" {
		status := c.statusLocked(ch.Workload)
		status[ch.Key] = ch.Value
		if err := c.putJSONLocked(statusKey(ch.Workload), status); err != nil {
			return ChangeResult{}, err
		}
	}
	// 合规的规格变更提交为新的权威期望版本
	if ch.Spec != nil {
		next := DesiredState{
			Workload: ch.Workload,
			Version:  d.Version + 1,
			Spec:     *ch.Spec,
		}
		if err := c.putDesiredLocked(next); err != nil {
			return ChangeResult{}, err
		}
	}
	applied[ch.ID] = struct{}{}
	if err := c.putJSONLocked(changesKey(ch.Workload), applied); err != nil {
		return ChangeResult{}, err
	}
	return ChangeResult{Verdict: VerdictApplied}, nil
}

// checkChangePolicyLocked 对边缘变更执行与云端下发一致的授权与配额检查。
func (c *Cloud) checkChangePolicyLocked(cur DesiredState, ch LocalChange) (ChangeResult, bool) {
	protected := cur.Spec.Protected || (ch.Spec != nil && ch.Spec.Protected)
	if protected && ch.Token != c.adminTok {
		return ChangeResult{Verdict: VerdictRejected, Reason: ReasonUnauthorized,
			Detail: "change on protected workload requires valid admin token"}, true
	}
	if ch.Spec != nil && c.maxRepl > 0 && ch.Spec.Replicas > c.maxRepl {
		return ChangeResult{Verdict: VerdictRejected, Reason: ReasonQuotaExceeded,
			Detail: "replicas exceed quota"}, true
	}
	return ChangeResult{}, false
}

// Desired 返回某工作负载当前的权威期望状态。
func (c *Cloud) Desired(workload string) (DesiredState, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.desiredLocked(workload)
}

// Status 返回云端汇总的某工作负载状态视图（已生效的本地变更键值）。
func (c *Cloud) Status(workload string) map[string]string {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.statusLocked(workload)
}

func (c *Cloud) checkPolicyLocked(d DesiredState) error {
	if d.Spec.Protected && d.Token != c.adminTok {
		return &RejectError{Reason: ReasonUnauthorized, Workload: d.Workload,
			Detail: "protected workload requires valid admin token"}
	}
	if c.maxRepl > 0 && d.Spec.Replicas > c.maxRepl {
		return &RejectError{Reason: ReasonQuotaExceeded, Workload: d.Workload,
			Detail: "replicas exceed quota"}
	}
	return nil
}

func (c *Cloud) desiredLocked(w string) (DesiredState, bool) {
	var d DesiredState
	raw, ok := c.store.Get(desiredKey(w))
	if !ok {
		return DesiredState{}, false
	}
	if err := json.Unmarshal(raw, &d); err != nil {
		return DesiredState{}, false
	}
	return d, true
}

func (c *Cloud) putDesiredLocked(d DesiredState) error {
	return c.putJSONLocked(desiredKey(d.Workload), d)
}

func (c *Cloud) statusLocked(w string) map[string]string {
	m := map[string]string{}
	if raw, ok := c.store.Get(statusKey(w)); ok {
		_ = json.Unmarshal(raw, &m)
	}
	return m
}

func (c *Cloud) appliedChangesLocked(w string) map[string]struct{} {
	m := map[string]struct{}{}
	if raw, ok := c.store.Get(changesKey(w)); ok {
		_ = json.Unmarshal(raw, &m)
	}
	return m
}

func (c *Cloud) putJSONLocked(key string, v any) error {
	raw, err := json.Marshal(v)
	if err != nil {
		return err
	}
	return c.store.Put(key, raw)
}
