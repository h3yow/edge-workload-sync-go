package edgesync

import (
	"errors"
	"fmt"
	"path/filepath"
	"sync"
	"testing"
)

// newPair 在临时目录中创建一对云端/边缘（各自独立持久化）及模拟链路。
func newPair(t *testing.T) (*Cloud, *Edge, *Link) {
	t.Helper()
	dir := t.TempDir()
	cs, err := OpenStore(filepath.Join(dir, "cloud.json"))
	if err != nil {
		t.Fatal(err)
	}
	es, err := OpenStore(filepath.Join(dir, "edge.json"))
	if err != nil {
		t.Fatal(err)
	}
	cloud := NewCloud(cs, "admin-token", 8)
	link := NewLink(cloud)
	edge := NewEdge(es, link)
	return cloud, edge, link
}

// reasonOf 提取错误的可区分原因。
func reasonOf(err error) Reason {
	var rej *RejectError
	if errors.As(err, &rej) {
		return rej.Reason
	}
	return ""
}

func TestDisconnectedReplay(t *testing.T) {
	cloud, edge, link := newPair(t)

	d := DesiredState{Workload: "app", Version: 1, Spec: WorkloadSpec{Image: "app:v1", Replicas: 2}}
	if err := cloud.SubmitDesired(d); err != nil {
		t.Fatal(err)
	}
	if r := edge.ApplyDesired(d); !r.Applied {
		t.Fatalf("expected applied, got %+v", r)
	}

	// 断连：边缘以最后确认的期望状态继续运行，并暂存本地变更
	link.Disconnect()
	changes := []LocalChange{
		{ID: "c1", Workload: "app", BaseVersion: 1, Key: "phase", Value: "Running"},
		{ID: "c2", Workload: "app", BaseVersion: 1, Key: "ready", Value: "2/2"},
	}
	for _, ch := range changes {
		t.Logf("输入: 断连期间本地变更 %+v; 判定依据: 链路断开，应暂存而非丢失", ch)
		if err := edge.ReportLocal(ch); err != nil {
			t.Fatalf("stage change: %v", err)
		}
	}
	if got := len(edge.Pending()); got != 2 {
		t.Fatalf("expected 2 pending, got %d", got)
	}
	cur, _ := edge.Current("app")
	t.Logf("判定依据: 断连期间边缘仍以 v%d 运行", cur.Version)

	// 恢复连接，按序重放
	link.Connect()
	if err := edge.Reconnect(); err != nil {
		t.Fatal(err)
	}
	st := cloud.Status("app")
	t.Logf("输入: 重放后云端状态 %v; 判定依据: 两条变更应按序生效", st)
	if st["phase"] != "Running" || st["ready"] != "2/2" {
		t.Fatalf("unexpected status: %v", st)
	}
	if got := len(edge.Pending()); got != 0 {
		t.Fatalf("expected empty pending, got %d", got)
	}

	// 幂等：再次重放同一批变更不得重复生效
	for _, ch := range changes {
		t.Logf("输入: 重复投递变更 %s; 判定依据: 云端按 ID 去重，应幂等忽略", ch.ID)
		if err := cloud.ReceiveChange(ch); err != nil {
			t.Fatalf("duplicate must be idempotent, got %v", err)
		}
	}
	if st := cloud.Status("app"); st["phase"] != "Running" || len(st) != 2 {
		t.Fatalf("duplicate replay changed state: %v", st)
	}
}

func TestStaleChangeRejectedOnReplay(t *testing.T) {
	cloud, edge, link := newPair(t)

	v1 := DesiredState{Workload: "app", Version: 1, Spec: WorkloadSpec{Image: "app:v1", Replicas: 1}}
	v2 := DesiredState{Workload: "app", Version: 2, Spec: WorkloadSpec{Image: "app:v2", Replicas: 1}}
	for _, d := range []DesiredState{v1, v2} {
		if err := cloud.SubmitDesired(d); err != nil {
			t.Fatal(err)
		}
	}
	edge.ApplyDesired(v1)

	// 断连期间产生基于 v1 的变更；期间云端已推进到 v2
	link.Disconnect()
	stale := LocalChange{ID: "s1", Workload: "app", BaseVersion: 1, Key: "phase", Value: "Running"}
	if err := edge.ReportLocal(stale); err != nil {
		t.Fatal(err)
	}
	edge.ApplyDesired(v2)

	link.Connect()
	t.Logf("输入: 重放基于 v1 的变更 %+v，云端当前版本 v2; 判定依据: BaseVersion < 当前版本，应以 StaleChange 拒绝", stale)
	if err := edge.Reconnect(); err != nil {
		t.Fatal(err)
	}
	if st := cloud.Status("app"); len(st) != 0 {
		t.Fatalf("stale change must not be applied, got %v", st)
	}
	if got := len(edge.Pending()); got != 0 {
		t.Fatalf("rejected change should be dropped from queue, got %d pending", got)
	}

	// 直接投递失效变更，验证原因可区分
	err := cloud.ReceiveChange(stale)
	t.Logf("输入: 直接投递失效变更; 判定结果: %v", err)
	if reasonOf(err) != ReasonStaleChange {
		t.Fatalf("expected StaleChange, got %v", err)
	}
}

func TestOutOfOrderAndVersionJump(t *testing.T) {
	cloud, edge, _ := newPair(t)

	mk := func(v Version) DesiredState {
		return DesiredState{Workload: "app", Version: v,
			Spec: WorkloadSpec{Image: fmt.Sprintf("app:v%d", v), Replicas: 1}}
	}
	// 云端：允许跨版本跳跃，拒绝回退，相同内容重复提交幂等
	for _, d := range []DesiredState{mk(1), mk(5)} {
		if err := cloud.SubmitDesired(d); err != nil {
			t.Fatalf("jump to v%d must be allowed: %v", d.Version, err)
		}
	}
	t.Logf("输入: 重复提交 v5（内容相同）; 判定依据: 幂等成功")
	if err := cloud.SubmitDesired(mk(5)); err != nil {
		t.Fatalf("identical resubmit must succeed: %v", err)
	}
	err := cloud.SubmitDesired(mk(3))
	t.Logf("输入: 提交 v3（当前 v5）; 判定结果: %v", err)
	if reasonOf(err) != ReasonVersionRegression {
		t.Fatalf("expected VersionRegression, got %v", err)
	}

	// 边缘：乱序到达
	r5 := edge.ApplyDesired(mk(5))
	t.Logf("输入: 边缘先收到 v5; 判定结果: %+v", r5)
	if !r5.Applied {
		t.Fatal("v5 should be applied")
	}
	r3 := edge.ApplyDesired(mk(3))
	t.Logf("输入: 乱序到达 v3（已应用 v5）; 判定结果: %+v", r3)
	if !r3.Rejected || r3.Reason != ReasonVersionRegression {
		t.Fatalf("expected regression rejection, got %+v", r3)
	}
	r5dup := edge.ApplyDesired(mk(5))
	t.Logf("输入: 重复到达 v5; 判定结果: %+v", r5dup)
	if r5dup.Applied || r5dup.Rejected || r5dup.Reason != ReasonDuplicate {
		t.Fatalf("expected idempotent duplicate, got %+v", r5dup)
	}
	r9 := edge.ApplyDesired(mk(9))
	t.Logf("输入: 跨越中间版本跳到 v9; 判定结果: %+v", r9)
	if !r9.Applied {
		t.Fatal("version jump must be allowed")
	}
	cur, _ := edge.Current("app")
	if cur.Version != 9 {
		t.Fatalf("final version = %d, want 9", cur.Version)
	}
}

func TestConcurrentConflict(t *testing.T) {
	cloud, _, _ := newPair(t)
	base := DesiredState{Workload: "app", Version: 1, Spec: WorkloadSpec{Image: "app:v1", Replicas: 1}}
	if err := cloud.SubmitDesired(base); err != nil {
		t.Fatal(err)
	}

	// 两个并发更新基于同一 baseVersion：恰好一个成功，另一个 Conflict
	var wg sync.WaitGroup
	results := make([]error, 2)
	for i := 0; i < 2; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			results[i] = cloud.UpdateDesired("app", 1, "", func(s *WorkloadSpec) {
				s.Replicas = 2 + i
			})
		}(i)
	}
	wg.Wait()
	t.Logf("输入: 两个并发 UpdateDesired(baseVersion=1); 判定结果: %v / %v", results[0], results[1])

	var ok, conflict int
	for _, err := range results {
		switch {
		case err == nil:
			ok++
		case reasonOf(err) == ReasonConflict:
			conflict++
		default:
			t.Fatalf("unexpected error: %v", err)
		}
	}
	if ok != 1 || conflict != 1 {
		t.Fatalf("expected 1 success + 1 conflict, got ok=%d conflict=%d", ok, conflict)
	}
	d, _ := cloud.Desired("app")
	t.Logf("判定依据: 最终状态唯一，version=%d replicas=%d", d.Version, d.Spec.Replicas)
	if d.Version != 2 {
		t.Fatalf("final version = %d, want 2", d.Version)
	}
}

func TestUnauthorizedAndQuota(t *testing.T) {
	cloud, _, _ := newPair(t)

	protected := DesiredState{Workload: "db", Version: 1,
		Spec: WorkloadSpec{Image: "db:v1", Replicas: 1, Protected: true}}
	err := cloud.SubmitDesired(protected)
	t.Logf("输入: 无令牌变更受保护工作负载; 判定结果: %v", err)
	if reasonOf(err) != ReasonUnauthorized {
		t.Fatalf("expected Unauthorized, got %v", err)
	}
	protected.Token = "wrong"
	err = cloud.SubmitDesired(protected)
	t.Logf("输入: 错误令牌变更受保护工作负载; 判定结果: %v", err)
	if reasonOf(err) != ReasonUnauthorized {
		t.Fatalf("expected Unauthorized, got %v", err)
	}
	protected.Token = "admin-token"
	if err := cloud.SubmitDesired(protected); err != nil {
		t.Fatalf("valid token must succeed: %v", err)
	}

	over := DesiredState{Workload: "db", Version: 2, Token: "admin-token",
		Spec: WorkloadSpec{Image: "db:v1", Replicas: 100, Protected: true}}
	err = cloud.SubmitDesired(over)
	t.Logf("输入: 副本数 100（配额 8）; 判定结果: %v", err)
	if reasonOf(err) != ReasonQuotaExceeded {
		t.Fatalf("expected QuotaExceeded, got %v", err)
	}
	// 被拒绝后权威状态仍为 v1
	d, _ := cloud.Desired("db")
	t.Logf("判定依据: 未授权/超配额被拒绝后状态不变，version=%d replicas=%d", d.Version, d.Spec.Replicas)
	if d.Version != 1 || d.Spec.Replicas != 1 {
		t.Fatalf("state mutated by rejected change: %+v", d)
	}
}

func TestPersistenceAcrossRestart(t *testing.T) {
	dir := t.TempDir()
	cpath := filepath.Join(dir, "cloud.json")
	cs, _ := OpenStore(cpath)
	cloud := NewCloud(cs, "admin-token", 8)
	if err := cloud.SubmitDesired(DesiredState{Workload: "app", Version: 7,
		Spec: WorkloadSpec{Image: "app:v7", Replicas: 3}}); err != nil {
		t.Fatal(err)
	}

	// 模拟重启：重新打开同一存储
	cs2, _ := OpenStore(cpath)
	cloud2 := NewCloud(cs2, "admin-token", 8)
	d, ok := cloud2.Desired("app")
	t.Logf("输入: 重启后读取期望状态; 判定结果: %+v ok=%v", d, ok)
	if !ok || d.Version != 7 {
		t.Fatalf("state not persisted: %+v", d)
	}
	// 重启后回退仍被拒绝
	err := cloud2.SubmitDesired(DesiredState{Workload: "app", Version: 6,
		Spec: WorkloadSpec{Image: "app:v6", Replicas: 1}})
	if reasonOf(err) != ReasonVersionRegression {
		t.Fatalf("expected VersionRegression after restart, got %v", err)
	}
}
