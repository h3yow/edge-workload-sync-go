package edgesync

import (
	"testing"
)

// TestEdgeChangePolicyOnline 验证在线直投的边缘本地变更与云端下发遵循
// 同一套授权与配额策略：违规被挡住、原因可区分、权威状态不被污染。
func TestEdgeChangePolicyOnline(t *testing.T) {
	cloud, edge, _ := newPair(t)

	d := DesiredState{Workload: "db", Version: 1, Token: "admin-token",
		Spec: WorkloadSpec{Image: "db:v1", Replicas: 1, Protected: true}}
	if err := cloud.SubmitDesired(d); err != nil {
		t.Fatal(err)
	}
	edge.ApplyDesired(d)

	cases := []struct {
		name string
		ch   LocalChange
		want Reason // "" 表示应生效
	}{
		{"受保护对象无令牌", LocalChange{ID: "p1", Workload: "db", BaseVersion: 1, Key: "phase", Value: "Running"}, ReasonUnauthorized},
		{"受保护对象错误令牌", LocalChange{ID: "p2", Workload: "db", BaseVersion: 1, Key: "phase", Value: "Running", Token: "wrong"}, ReasonUnauthorized},
		{"超配额规格变更", LocalChange{ID: "q1", Workload: "db", BaseVersion: 1, Token: "admin-token",
			Spec: &WorkloadSpec{Image: "db:v1", Replicas: 100, Protected: true}}, ReasonQuotaExceeded},
		{"合规状态上报", LocalChange{ID: "ok1", Workload: "db", BaseVersion: 1, Token: "admin-token", Key: "phase", Value: "Running"}, ""},
		{"合规规格变更", LocalChange{ID: "ok2", Workload: "db", BaseVersion: 1, Token: "admin-token",
			Spec: &WorkloadSpec{Image: "db:v1", Replicas: 2, Protected: true}}, ""},
	}
	for _, tc := range cases {
		err := edge.ReportLocal(tc.ch) // 在线：直投
		got := reasonOf(err)
		t.Logf("输入: %s %+v; 判定结果: reason=%q err=%v", tc.name, tc.ch, got, err)
		if got != tc.want {
			t.Fatalf("%s: reason = %q, want %q", tc.name, got, tc.want)
		}
	}

	// 权威状态只被合规变更推进：v1 -> v2（合规规格变更），违规未造成污染
	cur, _ := cloud.Desired("db")
	t.Logf("判定依据: 违规变更被拒绝后权威状态 version=%d spec=%+v", cur.Version, cur.Spec)
	if cur.Version != 2 || cur.Spec.Replicas != 2 {
		t.Fatalf("authoritative state polluted or compliant change lost: %+v", cur)
	}
	if st := cloud.Status("db"); st["phase"] != "Running" {
		t.Fatalf("compliant status report not applied: %v", st)
	}
}

// TestEdgeChangePolicyReplay 验证断连重放链路的策略判定与在线直投完全一致：
// 同样的违规变更，两条链路给出同样的拒绝原因，且只放行合规部分。
func TestEdgeChangePolicyReplay(t *testing.T) {
	cloud, edge, link := newPair(t)

	d := DesiredState{Workload: "db", Version: 1, Token: "admin-token",
		Spec: WorkloadSpec{Image: "db:v1", Replicas: 1, Protected: true}}
	if err := cloud.SubmitDesired(d); err != nil {
		t.Fatal(err)
	}
	edge.ApplyDesired(d)

	// 断连期间暂存：违规（无令牌）、违规（超配额）、合规
	link.Disconnect()
	staged := []LocalChange{
		{ID: "r-noauth", Workload: "db", BaseVersion: 1, Key: "phase", Value: "Running"},
		{ID: "r-quota", Workload: "db", BaseVersion: 1, Token: "admin-token",
			Spec: &WorkloadSpec{Image: "db:v1", Replicas: 100, Protected: true}},
		{ID: "r-ok", Workload: "db", BaseVersion: 1, Token: "admin-token", Key: "phase", Value: "Running"},
	}
	for _, ch := range staged {
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
		t.Logf("重放结局: change=%s verdict=%s reason=%s", o.ChangeID, o.Verdict, o.Reason)
	}
	if len(outcomes) != 3 {
		t.Fatalf("expected 3 outcomes, got %d", len(outcomes))
	}

	// 与在线直投的判定逐一比对：同类违规在两条链路上原因必须一致
	onlineErrs := map[string]Reason{}
	for _, ch := range staged[:2] {
		ch.ID += "-online" // 换 ID 避免被去重为 Duplicate
		onlineErrs[ch.ID] = reasonOf(edge.ReportLocal(ch))
	}
	t.Logf("判定依据: 在线直投同类违规的原因 %v", onlineErrs)

	if outcomes[0].Verdict != VerdictRejected || outcomes[0].Reason != ReasonUnauthorized {
		t.Fatalf("replay no-token outcome = %+v, want Rejected/Unauthorized", outcomes[0])
	}
	if outcomes[1].Verdict != VerdictRejected || outcomes[1].Reason != ReasonQuotaExceeded {
		t.Fatalf("replay over-quota outcome = %+v, want Rejected/QuotaExceeded", outcomes[1])
	}
	if outcomes[2].Verdict != VerdictApplied {
		t.Fatalf("replay compliant outcome = %+v, want Applied", outcomes[2])
	}
	if onlineErrs["r-noauth-online"] != outcomes[0].Reason ||
		onlineErrs["r-quota-online"] != outcomes[1].Reason {
		t.Fatalf("online/replay verdict mismatch: online=%v replay=%v", onlineErrs, outcomes[:2])
	}

	// 只放行合规部分：违规变更不污染权威状态
	cur, _ := cloud.Desired("db")
	t.Logf("判定依据: 重放后权威状态 version=%d spec=%+v（应仍为 v1/replicas=1）", cur.Version, cur.Spec)
	if cur.Version != 1 || cur.Spec.Replicas != 1 {
		t.Fatalf("authoritative state polluted by rejected replay: %+v", cur)
	}
	if st := cloud.Status("db"); st["phase"] != "Running" {
		t.Fatalf("compliant replayed change not applied: %v", st)
	}
	if got := len(edge.Pending()); got != 0 {
		t.Fatalf("rejected changes should be dropped from queue, got %d pending", got)
	}
}
