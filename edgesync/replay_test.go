package edgesync

import (
	"fmt"
	"path/filepath"
	"sync"
	"testing"
)

// TestReplayOutcomesRecorded 验证断连恢复重放的逐条结局：
// 生效、重复忽略、明确拒绝三类结局可区分且逐条对应。
func TestReplayOutcomesRecorded(t *testing.T) {
	cloud, edge, link := newPair(t)

	v1 := DesiredState{Workload: "app", Version: 1, Spec: WorkloadSpec{Image: "app:v1", Replicas: 1}}
	if err := cloud.SubmitDesired(v1); err != nil {
		t.Fatal(err)
	}
	edge.ApplyDesired(v1)

	// 在线时先让 dup1 生效，之后断连期间再暂存同 ID 变更
	dup := LocalChange{ID: "dup1", Workload: "app", BaseVersion: 1, Key: "phase", Value: "Running"}
	t.Logf("输入: 在线投递变更 %s; 判定依据: 首次到达，应正常生效", dup.ID)
	if err := edge.ReportLocal(dup); err != nil {
		t.Fatal(err)
	}

	link.Disconnect()
	stale := LocalChange{ID: "stale1", Workload: "app", BaseVersion: 1, Key: "phase", Value: "Done"}
	for _, ch := range []LocalChange{dup, stale} { // dup 与已生效变更同 ID；stale 基于 v1
		if err := edge.ReportLocal(ch); err != nil {
			t.Fatal(err)
		}
	}
	// 断连期间云端推进到 v2，边缘也收到 v2，使 stale1 失效
	v2 := DesiredState{Workload: "app", Version: 2, Spec: WorkloadSpec{Image: "app:v2", Replicas: 1}}
	if err := cloud.SubmitDesired(v2); err != nil {
		t.Fatal(err)
	}
	edge.ApplyDesired(v2)
	// 边缘基于最新的 v2 再产生一条变更，重放时应正常生效
	ok := LocalChange{ID: "ok1", Workload: "app", BaseVersion: 2, Key: "ready", Value: "1/1"}
	if err := edge.ReportLocal(ok); err != nil {
		t.Fatal(err)
	}

	link.Connect()
	records, err := edge.Reconnect()
	if err != nil {
		t.Fatal(err)
	}
	for _, r := range records {
		t.Logf("输入: 重放变更 %s (base=v%d); 判定结果: outcome=%s reason=%q cloudVersion=%d; 判定依据: %s",
			r.Change.ID, r.Change.BaseVersion, r.Outcome, r.Reason, r.CloudVersion, r.Detail)
	}
	if len(records) != 3 {
		t.Fatalf("expected 3 records, got %d", len(records))
	}

	// 逐条核对：结局类别、原因、判定时云端版本（队列顺序：dup, stale1, ok1）
	if r := records[0]; r.Outcome != OutcomeDuplicate || r.Change.ID != "dup1" || r.CloudVersion != 2 {
		t.Fatalf("dup1: want Duplicate@cloudV2, got %+v", r)
	}
	if r := records[1]; r.Outcome != OutcomeRejected || r.Reason != ReasonStaleChange || r.Change.ID != "stale1" {
		t.Fatalf("stale1: want Rejected/StaleChange, got %+v", r)
	}
	if r := records[2]; r.Outcome != OutcomeApplied || r.Change.ID != "ok1" || r.CloudVersion != 2 {
		t.Fatalf("ok1: want Applied@cloudV2, got %+v", r)
	}

	// 权威状态只被合规变更改写
	st := cloud.Status("app")
	t.Logf("判定依据: 重放后云端状态 %v，仅 ok1 与首次 dup1 生效", st)
	if st["ready"] != "1/1" || st["phase"] != "Running" || len(st) != 2 {
		t.Fatalf("unexpected status: %v", st)
	}
	if got := len(edge.Pending()); got != 0 {
		t.Fatalf("expected empty pending, got %d", got)
	}
}

// TestReplayHistoryPersistedAcrossRestart 验证逐条结局持久化在边缘本地，
// 重启后仍可查询，且多次重放的记录序号单调递增。
func TestReplayHistoryPersistedAcrossRestart(t *testing.T) {
	dir := t.TempDir()
	cpath, epath := filepath.Join(dir, "cloud.json"), filepath.Join(dir, "edge.json")

	cs, _ := OpenStore(cpath)
	es, _ := OpenStore(epath)
	cloud := NewCloud(cs, "admin-token", 8)
	link := NewLink(cloud)
	edge := NewEdge(es, link)

	v1 := DesiredState{Workload: "app", Version: 1, Spec: WorkloadSpec{Image: "app:v1", Replicas: 1}}
	if err := cloud.SubmitDesired(v1); err != nil {
		t.Fatal(err)
	}
	edge.ApplyDesired(v1)

	// 第一次断连-重放
	link.Disconnect()
	if err := edge.ReportLocal(LocalChange{ID: "r1", Workload: "app", BaseVersion: 1, Key: "a", Value: "1"}); err != nil {
		t.Fatal(err)
	}
	link.Connect()
	if _, err := edge.Reconnect(); err != nil {
		t.Fatal(err)
	}
	// 第二次断连-重放
	link.Disconnect()
	if err := edge.ReportLocal(LocalChange{ID: "r2", Workload: "app", BaseVersion: 1, Key: "b", Value: "2"}); err != nil {
		t.Fatal(err)
	}
	link.Connect()
	if _, err := edge.Reconnect(); err != nil {
		t.Fatal(err)
	}

	// 模拟重启：重新打开同一边缘存储
	es2, _ := OpenStore(epath)
	edge2 := NewEdge(es2, link)
	history := edge2.ReplayHistory()
	for _, r := range history {
		t.Logf("输入: 重启后查询重放历史; 记录: seq=%d change=%s outcome=%s", r.Seq, r.Change.ID, r.Outcome)
	}
	if len(history) != 2 {
		t.Fatalf("expected 2 persisted records after restart, got %d", len(history))
	}
	if history[0].Change.ID != "r1" || history[0].Outcome != OutcomeApplied {
		t.Fatalf("record r1 mismatch: %+v", history[0])
	}
	if history[1].Change.ID != "r2" || history[1].Outcome != OutcomeApplied {
		t.Fatalf("record r2 mismatch: %+v", history[1])
	}
	if !(history[0].Seq < history[1].Seq) {
		t.Fatalf("seq must be monotonic: %+v", history)
	}
}

// TestReplayConcurrentCloudAdvance 验证重放期间云端并发推进版本时，
// 逐条结局仍然准确：每条记录唯一对应一条暂存变更，不张冠李戴。
func TestReplayConcurrentCloudAdvance(t *testing.T) {
	cloud, edge, link := newPair(t)

	v1 := DesiredState{Workload: "app", Version: 1, Spec: WorkloadSpec{Image: "app:v1", Replicas: 1}}
	if err := cloud.SubmitDesired(v1); err != nil {
		t.Fatal(err)
	}
	edge.ApplyDesired(v1)

	// 断连期间暂存一批基于 v1 的变更
	link.Disconnect()
	const n = 20
	for i := 0; i < n; i++ {
		ch := LocalChange{ID: fmt.Sprintf("c%02d", i), Workload: "app",
			BaseVersion: 1, Key: fmt.Sprintf("k%02d", i), Value: "v"}
		if err := edge.ReportLocal(ch); err != nil {
			t.Fatal(err)
		}
	}
	link.Connect()

	// 重放与云端版本推进并发进行
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for v := Version(2); v <= 10; v++ {
			_ = cloud.SubmitDesired(DesiredState{Workload: "app", Version: v,
				Spec: WorkloadSpec{Image: fmt.Sprintf("app:v%d", v), Replicas: 1}})
		}
	}()
	var records []ReplayRecord
	var err error
	wg.Add(1)
	go func() {
		defer wg.Done()
		records, err = edge.Reconnect()
	}()
	wg.Wait()
	if err != nil {
		t.Fatal(err)
	}
	// 若首轮重放因竞态未清空队列（理论上不会，链路一直连通），补一轮
	if len(edge.Pending()) > 0 {
		more, err := edge.Reconnect()
		if err != nil {
			t.Fatal(err)
		}
		records = append(records, more...)
	}

	// 每条暂存变更恰好有一条结局记录，ID 不重不漏
	if len(records) != n {
		t.Fatalf("expected %d records, got %d", n, len(records))
	}
	seen := map[string]ReplayRecord{}
	for _, r := range records {
		if _, dup := seen[r.Change.ID]; dup {
			t.Fatalf("change %s recorded twice", r.Change.ID)
		}
		seen[r.Change.ID] = r
		t.Logf("记录: change=%s outcome=%s reason=%q cloudVersion=%d",
			r.Change.ID, r.Outcome, r.Reason, r.CloudVersion)
	}
	for i := 0; i < n; i++ {
		id := fmt.Sprintf("c%02d", i)
		r, ok := seen[id]
		if !ok {
			t.Fatalf("change %s has no outcome record", id)
		}
		// 结局必须与实际判定一致：生效/拒绝取决于判定时云端版本
		switch r.Outcome {
		case OutcomeApplied:
			if r.CloudVersion != 1 {
				t.Fatalf("%s applied against cloud v%d, want v1", id, r.CloudVersion)
			}
		case OutcomeRejected:
			// 拒绝记录的原因必须是 StaleChange（基于已失效版本），
			// 且判定时云端版本确实高于变更的基准版本
			if r.Reason != ReasonStaleChange {
				t.Fatalf("%s rejected with unexpected reason %s", id, r.Reason)
			}
			if r.CloudVersion <= r.Change.BaseVersion {
				t.Fatalf("%s rejected at cloud v%d but base v%d is not stale",
					id, r.CloudVersion, r.Change.BaseVersion)
			}
		default:
			t.Fatalf("%s unexpected outcome %s", id, r.Outcome)
		}
	}
	// 云端最终状态与记录一致：所有 Applied 的键都应在状态中
	st := cloud.Status("app")
	for _, r := range records {
		if r.Outcome == OutcomeApplied && st[r.Change.Key] != "v" {
			t.Fatalf("applied change %s missing from cloud status", r.Change.ID)
		}
	}
	t.Logf("判定依据: %d 条暂存变更各有唯一结局，Applied 键全部体现在云端状态 %v", n, st)
}
