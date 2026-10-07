package edgesync

import (
	"testing"
)

// TestSpecDriftDetected 验证同版本不同内容的下发被识别为 SpecDrift，
// 与幂等重复、版本回退可区分，且边缘不静默按老内容运行。
func TestSpecDriftDetected(t *testing.T) {
	cloud, edge, _ := newPair(t)

	v1 := DesiredState{Workload: "app", Version: 1, Spec: WorkloadSpec{Image: "app:v1", Replicas: 2}}
	if err := cloud.SubmitDesired(v1); err != nil {
		t.Fatal(err)
	}
	if r := edge.ApplyDesired(v1); !r.Applied {
		t.Fatalf("expected applied, got %+v", r)
	}

	// 同版本同内容：幂等忽略
	dup := edge.ApplyDesired(v1)
	t.Logf("输入: 同版本同内容 v1; 判定结果: %+v; 判定依据: 幂等重复", dup)
	if dup.Reason != ReasonDuplicate || dup.Applied || dup.Rejected {
		t.Fatalf("want idempotent duplicate, got %+v", dup)
	}

	// 同版本不同内容：可区分的 SpecDrift，且边缘不被覆盖
	drifted := DesiredState{Workload: "app", Version: 1, Spec: WorkloadSpec{Image: "app:v1-evil", Replicas: 99}}
	r := edge.ApplyDesired(drifted)
	t.Logf("输入: 同版本不同内容 v1 (image=%q replicas=%d); 判定结果: %+v; 判定依据: 内容摘要不一致，应拒绝而非静默",
		drifted.Spec.Image, drifted.Spec.Replicas, r)
	if !r.Rejected || r.Reason != ReasonSpecDrift {
		t.Fatalf("want Rejected/SpecDrift, got %+v", r)
	}
	cur, _ := edge.Current("app")
	if cur.Spec.Image != "app:v1" || cur.Spec.Replicas != 2 {
		t.Fatalf("edge state overwritten by drifted content: %+v", cur)
	}
	t.Logf("判定依据: 漂移被拒绝后边缘仍运行 v1 原内容 image=%q replicas=%d", cur.Spec.Image, cur.Spec.Replicas)

	// 与版本回退可区分
	older := edge.ApplyDesired(DesiredState{Workload: "app", Version: 0, Spec: v1.Spec})
	if older.Reason != ReasonVersionRegression {
		t.Fatalf("regression must stay distinguishable, got %+v", older)
	}
}

// TestVerifyConsistency 验证云端权威内容与边缘运行内容的一致性查询：
// 一致、字段级不一致、单端缺失三种结论可区分，且能看到差在哪里。
func TestVerifyConsistency(t *testing.T) {
	cloud, edge, _ := newPair(t)

	// 场景一：两端一致
	v1 := DesiredState{Workload: "app", Version: 3, Spec: WorkloadSpec{Image: "app:v3", Replicas: 2, Protected: true}, Token: "admin-token"}
	if err := cloud.SubmitDesired(v1); err != nil {
		t.Fatal(err)
	}
	edge.ApplyDesired(v1)
	rep := VerifyConsistency(cloud, edge, "app")
	t.Logf("输入: 两端均为 v3 同内容; 判定结果: consistent=%v cloudDigest=%s edgeDigest=%s",
		rep.Consistent, rep.CloudDigest, rep.EdgeDigest)
	if !rep.Consistent || !rep.CloudFound || !rep.EdgeFound || len(rep.Diffs) != 0 {
		t.Fatalf("want consistent, got %+v", rep)
	}
	if rep.CloudDigest != rep.EdgeDigest {
		t.Fatalf("digests must match when consistent: %+v", rep)
	}

	// 场景二：云端推进到 v4 但边缘仍运行 v3（含字段级差异）
	v4 := v1
	v4.Version = 4
	v4.Spec.Image = "app:v4"
	v4.Spec.Replicas = 5
	if err := cloud.SubmitDesired(v4); err != nil {
		t.Fatal(err)
	}
	rep = VerifyConsistency(cloud, edge, "app")
	for _, d := range rep.Diffs {
		t.Logf("输入: 云端 v4/边缘 v3; 差异: field=%s cloud=%q edge=%q", d.Field, d.Cloud, d.Edge)
	}
	if rep.Consistent {
		t.Fatalf("want inconsistent, got %+v", rep)
	}
	if rep.CloudDigest == rep.EdgeDigest {
		t.Fatalf("digests must differ when inconsistent: %+v", rep)
	}
	want := map[string][2]string{
		"version":  {"4", "3"},
		"image":    {"app:v4", "app:v3"},
		"replicas": {"5", "2"},
	}
	if len(rep.Diffs) != len(want) {
		t.Fatalf("want %d diffs, got %+v", len(want), rep.Diffs)
	}
	for _, d := range rep.Diffs {
		w, ok := want[d.Field]
		if !ok || w[0] != d.Cloud || w[1] != d.Edge {
			t.Fatalf("unexpected diff %+v", d)
		}
	}
	t.Logf("判定依据: 差异字段与期望值逐一对应 %v", rep.Diffs)

	// 边缘追平后恢复一致
	edge.ApplyDesired(v4)
	if rep := VerifyConsistency(cloud, edge, "app"); !rep.Consistent {
		t.Fatalf("want consistent after catch-up, got %+v", rep)
	}

	// 场景三：单端缺失
	rep = VerifyConsistency(cloud, edge, "ghost")
	t.Logf("输入: 两端均无 ghost; 判定结果: %+v", rep)
	if rep.Consistent || rep.CloudFound || rep.EdgeFound {
		t.Fatalf("ghost workload must be inconsistent with both missing, got %+v", rep)
	}
	only := DesiredState{Workload: "only-cloud", Version: 1, Spec: WorkloadSpec{Image: "x:v1", Replicas: 1}}
	if err := cloud.SubmitDesired(only); err != nil {
		t.Fatal(err)
	}
	rep = VerifyConsistency(cloud, edge, "only-cloud")
	t.Logf("输入: 仅云端存在 only-cloud; 判定结果: consistent=%v cloudFound=%v edgeFound=%v",
		rep.Consistent, rep.CloudFound, rep.EdgeFound)
	if rep.Consistent || !rep.CloudFound || rep.EdgeFound {
		t.Fatalf("want cloud-only inconsistent, got %+v", rep)
	}
}
