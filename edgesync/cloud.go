package edgesync

import (
	"encoding/json"
	"fmt"
	"strconv"
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
// 策略约束（授权/配额）与期望状态变更同等适用，见 ReceiveChangeResult。
func (c *Cloud) ReceiveChange(ch LocalChange) error {
	_, err := c.ReceiveChangeResult(ch)
	return err
}

// ReceiveChangeResult 与 ReceiveChange 判定规则相同，但返回结构化结果，
// 使调用方能区分"正常生效"与"重复忽略"，并获知判定时的云端版本。
//
// 判定顺序（云端锁内原子完成，并发推进版本不会张冠李戴）：
//  1. 工作负载未知 → ReasonStaleChange；
//  2. ID 已生效过 → 幂等忽略（Duplicate），无论其基准版本是否已失效；
//  3. BaseVersion 低于当前版本 → ReasonStaleChange，绝不静默应用；
//  4. 策略校验：受保护工作负载必须携带有效令牌（ReasonUnauthorized），
//     Key 为 "replicas" 时不得超过配额（ReasonQuotaExceeded）；
//  5. 全部通过才写入状态并记录幂等键，被拒绝的变更不污染权威状态。
func (c *Cloud) ReceiveChangeResult(ch LocalChange) (ReceiveResult, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	d, ok := c.desiredLocked(ch.Workload)
	if !ok {
		return ReceiveResult{}, &RejectError{Reason: ReasonStaleChange, Workload: ch.Workload,
			Detail: "unknown workload"}
	}
	applied := c.appliedChangesLocked(ch.Workload)
	if _, dup := applied[ch.ID]; dup {
		return ReceiveResult{Duplicate: true, Version: d.Version}, nil // 重复变更：幂等忽略
	}
	if ch.BaseVersion < d.Version {
		return ReceiveResult{Version: d.Version}, &RejectError{Reason: ReasonStaleChange, Workload: ch.Workload,
			Detail: "change based on superseded version"}
	}
	if err := c.checkChangePolicyLocked(d, ch); err != nil {
		return ReceiveResult{Version: d.Version}, err
	}
	status := c.statusLocked(ch.Workload)
	status[ch.Key] = ch.Value
	if err := c.putJSONLocked(statusKey(ch.Workload), status); err != nil {
		return ReceiveResult{}, err
	}
	applied[ch.ID] = struct{}{}
	if err := c.putJSONLocked(changesKey(ch.Workload), applied); err != nil {
		return ReceiveResult{}, err
	}
	return ReceiveResult{Applied: true, Version: d.Version}, nil
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

// checkChangePolicyLocked 对边缘本地变更施加与期望状态变更同等的策略约束：
// 受保护工作负载的变更必须携带有效令牌；Key 为 "replicas" 的变更按副本数
// 解析并受配额约束。在线直投与断连重放都经过此判定，结果一致。
func (c *Cloud) checkChangePolicyLocked(d DesiredState, ch LocalChange) error {
	if d.Spec.Protected && ch.Token != c.adminTok {
		return &RejectError{Reason: ReasonUnauthorized, Workload: ch.Workload,
			Detail: "protected workload requires valid admin token"}
	}
	if ch.Key == "replicas" && c.maxRepl > 0 {
		n, err := strconv.Atoi(ch.Value)
		if err != nil {
			return &RejectError{Reason: ReasonQuotaExceeded, Workload: ch.Workload,
				Detail: fmt.Sprintf("replicas value %q is not a number", ch.Value)}
		}
		if n > c.maxRepl {
			return &RejectError{Reason: ReasonQuotaExceeded, Workload: ch.Workload,
				Detail: "replicas exceed quota"}
		}
	}
	return nil
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
