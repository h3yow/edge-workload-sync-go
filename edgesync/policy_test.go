package edgesync

import (
	"fmt"
	"testing"
)

// edgeChangeCases 构造一组边缘本地变更用例：覆盖受保护对象未授权、
// 超配额、以及合规变更。同一批用例会分别走在线直投与断连重放两条链路，
// 判定结果必须一致。
func edgeChangeCases() []struct {
	name       string
	ch         LocalChange
	wantReject Reason // 空串表示应放行
} {
	return []struct {
		name       string
		ch         LocalChange
		wantReject Reason
	}{
		{"受保护对象无令牌", LocalChange{ID: "p1", Workload: "db", BaseVersion: 1, Key: "phase", Value: "Running"}, ReasonUnauthorized},
		{"受保护对象错误令牌", LocalChange{ID: "p2", Workload: "db", BaseVersion: 1, Key: "phase", Value: "Running", Token: "wrong"}, ReasonUnauthorized},
		{"受保护对象合法令牌", LocalChange{ID: "p3", Workload: "db", BaseVersion: 1, Key: "phase", Value: "Running", Token: "admin-token"}, ""},
		{"副本数超配额", LocalChange{ID: "p4", Workload: "db", BaseVersion: 1, Key: "replicas", Value: "100", Token: "admin-token"}, ReasonQuotaExceeded},
		{"副本数配额内", LocalChange{ID: "p5", Workload: "db", BaseVersion: 1, Key: "replicas", Value: "3", Token: "admin-token"}, ""},
		{"非受保护对象普通变更", LocalChange{ID: "p6", Workload: "app", BaseVersion: 1, Key: "phase", Value: "Running"}, ""},
	}
}

// setupProtectedPair 创建一对云端/边缘，包含受保护工作负载 db（v1）
// 与普通工作负载 app（v1），边缘已应用两者。
func setupProtectedPair(t *testing.T) (*Cloud, *Edge, *Link) {
	t.Helper()
	cloud, edge, link := newPair(t)
	db := DesiredState{Workload: "db", Version: 1, Token: "admin-token",
		Spec: WorkloadSpec{Image: "db:v1", Replicas: 1, Protected: true}}
	app := DesiredState{Workload: "app", Version: 1,
		Spec: WorkloadSpec{Image: "app:v1", Replicas: 2}}
	for _, d := range []DesiredState{db, app} {
		if err := cloud.SubmitDesired(d); err != nil {
			t.Fatal(err)
		}
		edge.ApplyDesired(d)
	}
	return cloud, edge, link
}

// TestEdgeChangePolicyOnline 验证在线直投链路：边缘本地变更与云端下发
// 受同样的授权/配额约束，被拒变更不污染权威状态。
func TestEdgeChangePolicyOnline(t *testing.T) {
	cloud, edge, _ := setupProtectedPair(t)

	for _, tc := range edgeChangeCases() {
		err := edge.ReportLocal(tc.ch)
		got := reasonOf(err)
		t.Logf("输入: 在线直投 %s (%s=%q token=%q); 判定结果: %v; 判定依据: want=%q",
			tc.name, tc.ch.Key, tc.ch.Value, tc.ch.Token, err, tc.wantReject)
		if got != tc.wantReject {
			t.Fatalf("%s: want reason %q, got %q (err=%v)", tc.name, tc.wantReject, got, err)
		}
	}

	// 权威状态只包含合规变更（p3、p5、p6）
	stDB, stApp := cloud.Status("db"), cloud.Status("app")
	t.Logf("判定依据: 合规变更生效后 db=%v app=%v", stDB, stApp)
	if stDB["phase"] != "Running" || stDB["replicas"] != "3" || len(stDB) != 2 {
		t.Fatalf("db status polluted or incomplete: %v", stDB)
	}
	if stApp["phase"] != "Running" || len(stApp) != 1 {
		t.Fatalf("app status unexpected: %v", stApp)
	}
	// 被拒的变更 ID 未记录为已生效：修正令牌后重投应能生效
	retry := LocalChange{ID: "p1", Workload: "db", BaseVersion: 1, Key: "note", Value: "retried", Token: "admin-token"}
	if err := edge.ReportLocal(retry); err != nil {
		t.Fatalf("rejected change id must not be burned, retry failed: %v", err)
	}
	t.Logf("判定依据: 被拒变更 p1 修正令牌后重投成功，权威状态未被污染")
}

// TestEdgeChangePolicyReplay 验证断连重放链路：同一批变更的判定结果
// 与在线直投完全一致，且逐条结局中原因可区分。
func TestEdgeChangePolicyReplay(t *testing.T) {
	cloud, edge, link := setupProtectedPair(t)

	link.Disconnect()
	cases := edgeChangeCases()
	for _, tc := range cases {
		if err := edge.ReportLocal(tc.ch); err != nil {
			t.Fatal(err)
		}
	}
	link.Connect()
	records, err := edge.Reconnect()
	if err != nil {
		t.Fatal(err)
	}
	if len(records) != len(cases) {
		t.Fatalf("expected %d records, got %d", len(cases), len(records))
	}
	for i, tc := range cases {
		r := records[i]
		t.Logf("输入: 断连重放 %s (%s=%q token=%q); 判定结果: outcome=%s reason=%q; 判定依据: want=%q",
			tc.name, tc.ch.Key, tc.ch.Value, tc.ch.Token, r.Outcome, r.Reason, tc.wantReject)
		if r.Change.ID != fmt.Sprintf("p%d", i+1) {
			t.Fatalf("record %d mismatched change: %+v", i, r)
		}
		if tc.wantReject == "" {
			if r.Outcome != OutcomeApplied {
				t.Fatalf("%s: want Applied, got %+v", tc.name, r)
			}
		} else {
			if r.Outcome != OutcomeRejected || r.Reason != tc.wantReject {
				t.Fatalf("%s: want Rejected/%s, got %+v", tc.name, tc.wantReject, r)
			}
		}
	}

	// 与在线链路相同：权威状态只被合规变更改写
	stDB, stApp := cloud.Status("db"), cloud.Status("app")
	t.Logf("判定依据: 重放后 db=%v app=%v，仅合规变更生效", stDB, stApp)
	if stDB["phase"] != "Running" || stDB["replicas"] != "3" || len(stDB) != 2 {
		t.Fatalf("db status polluted or incomplete: %v", stDB)
	}
	if stApp["phase"] != "Running" || len(stApp) != 1 {
		t.Fatalf("app status unexpected: %v", stApp)
	}
	if got := len(edge.Pending()); got != 0 {
		t.Fatalf("rejected changes must be dropped from queue, got %d pending", got)
	}
}
