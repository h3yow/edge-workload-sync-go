package edgesync

import (
	"fmt"
	"path/filepath"
	"sync"
	"testing"
)

// TestReplayOutcomesPerItem 验证断连恢复重放给出逐条可区分的结局：
// 正常生效 / 重复忽略 / 明确拒绝（含原因），且结局持久化到本地。
func TestReplayOutcomesPerItem(t *testing.T) {
	cloud, edge, link := newPair(t)

	d := DesiredState{Workload: "app", Version: 1, Spec: WorkloadSpec{Image: "app:v1", Replicas: 1}}
	if err := cloud.SubmitDesired(d); err != nil {
		t.Fatal(err)
	}
	edge.ApplyDesired(d)

	// 在线先投递一条变更（ID=dup1），使其在云端已生效
	online := LocalChange{ID: "dup1", Workload: "app", BaseVersion: 1, Key: "phase", Value: "Running"}
	t.Logf("输入: 在线投递变更 %+v; 判定依据: 链路正常，应直接生效", online)
	if err := edge.ReportLocal(online); err != nil {
		t.Fatal(err)
	}

	// 断连，暂存三条结局各不相同的变更
	link.Disconnect()
	staged := []LocalChange{
		{ID: "ok1", Workload: "app", BaseVersion: 1, Key: "ready", Value: "1/1"},   // 应生效
		{ID: "dup1", Workload: "app", BaseVersion: 1, Key: "phase", Value: "Running"}, // 重复 ID，应幂等忽略
		{ID: "bad1", Workload: "app", BaseVersion: 1, Key: "x", Value: "y",
			Spec: &WorkloadSpec{Image: "app:v1", Replicas: 100}}, // 超配额，应拒绝
	}
	for _, ch := range staged {
		t.Logf("输入: 断连期间暂存变更 %+v", ch)
		if err := edge.ReportLocal(ch); err != nil {
			t.Fatal(err)
		}
	}

	link.Connect()
	outcomes, err := edge.Reconnect()
	if err != nil {
		t.Fatal(err)
	}
	for _, o := range outcomes {
		t.Logf("重放结局: seq=%d change=%s verdict=%s reason=%s detail=%s",
			o.Seq, o.ChangeID, o.Verdict, o.Reason, o.Detail)
	}
	if len(outcomes) != 3 {
		t.Fatalf("expected 3 outcomes, got %d", len(outcomes))
	}

	// 逐条核对：顺序与暂存顺序一致，结局类别可区分
	want := []struct {
		id      string
		verdict Verdict
		reason  Reason
	}{
		{"ok1", VerdictApplied, ""},
		{"dup1", VerdictDuplicate, ""},
		{"bad1", VerdictRejected, ReasonQuotaExceeded},
	}
	for i, w := range want {
		o := outcomes[i]
		t.Logf("判定依据: 变更 %s 期望 verdict=%s reason=%s, 实际 verdict=%s reason=%s",
			w.id, w.verdict, w.reason, o.Verdict, o.Reason)
		if o.ChangeID != w.id || o.Verdict != w.verdict || o.Reason != w.reason {
			t.Fatalf("outcome[%d] = %+v, want id=%s verdict=%s reason=%s", i, o, w.id, w.verdict, w.reason)
		}
		if o.Seq != i+1 {
			t.Fatalf("outcome[%d] seq = %d, want %d", i, o.Seq, i+1)
		}
	}

	// 被拒绝与重复的变更不污染权威状态；合规变更已生效
	if st := cloud.Status("app"); st["ready"] != "1/1" || st["phase"] != "Running" {
		t.Fatalf("applied change missing from status: %v", st)
	}
	if got := len(edge.Pending()); got != 0 {
		t.Fatalf("expected empty pending, got %d", got)
	}

	// 结局已持久化，与返回值一致
	if log := edge.ReplayLog(); len(log) != 3 {
		t.Fatalf("persisted replay log len = %d, want 3", len(log))
	}
}

// TestReplayLogPersistedAcrossRestart 验证重放结局落盘，重启后仍可查阅。
func TestReplayLogPersistedAcrossRestart(t *testing.T) {
	dir := t.TempDir()
	cs, _ := OpenStore(filepath.Join(dir, "cloud.json"))
	es, _ := OpenStore(filepath.Join(dir, "edge.json"))
	cloud := NewCloud(cs, "admin-token", 8)
	link := NewLink(cloud)
	edge := NewEdge(es, link)

	d := DesiredState{Workload: "app", Version: 1, Spec: WorkloadSpec{Image: "app:v1", Replicas: 1}}
	if err := cloud.SubmitDesired(d); err != nil {
		t.Fatal(err)
	}
	edge.ApplyDesired(d)

	link.Disconnect()
	ch := LocalChange{ID: "r1", Workload: "app", BaseVersion: 1, Key: "phase", Value: "Running"}
	if err := edge.ReportLocal(ch); err != nil {
		t.Fatal(err)
	}
	link.Connect()
	outcomes, err := edge.Reconnect()
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("输入: 重放变更 %s; 判定结果: %+v", ch.ID, outcomes)

	// 模拟重启：重新打开同一边缘存储
	es2, _ := OpenStore(filepath.Join(dir, "edge.json"))
	edge2 := NewEdge(es2, link)
	log := edge2.ReplayLog()
	t.Logf("输入: 重启后读取重放日志; 判定结果: %+v", log)
	if len(log) != 1 || log[0].ChangeID != "r1" || log[0].Verdict != VerdictApplied {
		t.Fatalf("replay log not persisted across restart: %+v", log)
	}
}

// TestReplayConcurrentCloudAdvance 验证重放期间云端并发推进版本时，
// 逐条结局仍然准确：每条结局对应自己的变更 ID，序号单调，且一旦云端
// 版本越过 BaseVersion，后续变更的结局必须全部是 StaleChange 拒绝。
func TestReplayConcurrentCloudAdvance(t *testing.T) {
	cloud, edge, link := newPair(t)

	d := DesiredState{Workload: "app", Version: 1, Spec: WorkloadSpec{Image: "app:v1", Replicas: 1}}
	if err := cloud.SubmitDesired(d); err != nil {
		t.Fatal(err)
	}
	edge.ApplyDesired(d)

	const n = 20
	link.Disconnect()
	for i := 0; i < n; i++ {
		ch := LocalChange{ID: fmt.Sprintf("cc-%d", i), Workload: "app",
			BaseVersion: 1, Key: fmt.Sprintf("k%d", i), Value: "v"}
		if err := edge.ReportLocal(ch); err != nil {
			t.Fatal(err)
		}
	}
	link.Connect()

	// 重放的同时，云端并发推进版本
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		_ = cloud.SubmitDesired(DesiredState{Workload: "app", Version: 2,
			Spec: WorkloadSpec{Image: "app:v2", Replicas: 1}})
	}()
	outcomes, err := edge.Reconnect()
	wg.Wait()
	if err != nil {
		t.Fatal(err)
	}
	for _, o := range outcomes {
		t.Logf("重放结局: seq=%d change=%s verdict=%s reason=%s", o.Seq, o.ChangeID, o.Verdict, o.Reason)
	}

	// 每条暂存变更都有且仅有一条结局
	if len(outcomes) != n {
		t.Fatalf("expected %d outcomes, got %d", n, len(outcomes))
	}
	seen := map[string]bool{}
	staleSeen := false
	for i, o := range outcomes {
		// 结局与变更一一对应，不张冠李戴
		wantID := fmt.Sprintf("cc-%d", i)
		if o.ChangeID != wantID {
			t.Fatalf("outcome[%d] change = %s, want %s", i, o.ChangeID, wantID)
		}
		if seen[o.ChangeID] {
			t.Fatalf("duplicate outcome for %s", o.ChangeID)
		}
		seen[o.ChangeID] = true
		if o.Seq != i+1 {
			t.Fatalf("outcome[%d] seq = %d, want %d", i, o.Seq, i+1)
		}
		// 云端版本单调推进：一旦出现 StaleChange，后续必须全部是 StaleChange
		if staleSeen && o.Verdict != VerdictRejected {
			t.Fatalf("outcome[%d] = %+v after a stale rejection, must also be rejected", i, o)
		}
		if o.Verdict == VerdictRejected {
			if o.Reason != ReasonStaleChange {
				t.Fatalf("outcome[%d] rejected with %s, want StaleChange", i, o.Reason)
			}
			staleSeen = true
		}
	}
	t.Logf("判定依据: 云端版本单调推进，%d 条结局中生效前缀与失效后缀严格分界", n)

	// 重放日志与返回值一致且已持久化
	if log := edge.ReplayLog(); len(log) != n {
		t.Fatalf("persisted replay log len = %d, want %d", len(log), n)
	}
	if got := len(edge.Pending()); got != 0 {
		t.Fatalf("expected empty pending, got %d", got)
	}
}
