package edgesync

import (
	"testing"
)

// TestContentMismatchDetected 验证同版本不同内容的下发被明确识别为
// ContentMismatch，与重复到达（Duplicate）可区分，且边缘不静默继续、
// 也不被错误内容覆盖。
func TestContentMismatchDetected(t *testing.T) {
	cloud, edge, _ := newPair(t)

	v1 := DesiredState{Workload: "app", Version: 1, Spec: WorkloadSpec{Image: "app:v1", Replicas: 2}}
	if err := cloud.SubmitDesired(v1); err != nil {
		t.Fatal(err)
	}
	if r := edge.ApplyDesired(v1); !r.Applied {
		t.Fatalf("expected applied, got %+v", r)
	}

	// 同版本、内容被改动：必须给出可区分的结论
	tampered := v1
	tampered.Spec.Image = "app:v1-evil"
	r := edge.ApplyDesired(tampered)
	t.Logf("输入: 同版本 v1 但镜像被改为 %s; 判定结果: %+v", tampered.Spec.Image, r)
	if !r.Rejected || r.Reason != ReasonContentMismatch {
		t.Fatalf("expected ContentMismatch rejection, got %+v", r)
	}

	// 边缘不被覆盖，也不静默：仍运行原内容
	cur, _ := edge.Current("app")
	t.Logf("判定依据: 拒绝后边缘仍运行 image=%s replicas=%d", cur.Spec.Image, cur.Spec.Replicas)
	if cur.Spec.Image != "app:v1" {
		t.Fatalf("edge overwritten by mismatched content: %+v", cur)
	}

	// 同版本同内容仍是幂等重复，与 ContentMismatch 可区分
	rdup := edge.ApplyDesired(v1)
	t.Logf("输入: 同版本 v1 内容相同; 判定结果: %+v", rdup)
	if rdup.Rejected || rdup.Reason != ReasonDuplicate {
		t.Fatalf("expected idempotent duplicate, got %+v", rdup)
	}

	// 云端以更高版本重新下发后恢复正常
	v2 := DesiredState{Workload: "app", Version: 2, Spec: WorkloadSpec{Image: "app:v2", Replicas: 2}}
	if err := cloud.SubmitDesired(v2); err != nil {
		t.Fatal(err)
	}
	if r := edge.ApplyDesired(v2); !r.Applied {
		t.Fatalf("higher version must recover, got %+v", r)
	}
	if rep := edge.Consistency("app"); !rep.Consistent {
		t.Fatalf("expected consistent after recovery, got %+v", rep)
	}
}

// TestConsistencyQuery 验证一致性查询：一致时通过；云端版本领先、
// 同版本内容漂移、单边缺失时给出逐字段差异。
func TestConsistencyQuery(t *testing.T) {
	cloud, edge, _ := newPair(t)

	v1 := DesiredState{Workload: "app", Version: 1, Spec: WorkloadSpec{Image: "app:v1", Replicas: 2}}
	if err := cloud.SubmitDesired(v1); err != nil {
		t.Fatal(err)
	}
	edge.ApplyDesired(v1)

	// 一致
	rep := edge.Consistency("app")
	t.Logf("输入: 两端均为 v1 同内容; 判定结果: %+v", rep)
	if !rep.Consistent || len(rep.Diffs) != 0 {
		t.Fatalf("expected consistent, got %+v", rep)
	}

	// 云端推进到 v2，边缘尚未应用：版本与镜像差异可见
	v2 := DesiredState{Workload: "app", Version: 2, Spec: WorkloadSpec{Image: "app:v2", Replicas: 3}}
	if err := cloud.SubmitDesired(v2); err != nil {
		t.Fatal(err)
	}
	rep = edge.Consistency("app")
	for _, d := range rep.Diffs {
		t.Logf("差异: field=%s cloud=%s edge=%s", d.Field, d.Cloud, d.Edge)
	}
	if rep.Consistent {
		t.Fatal("expected inconsistent while edge lags")
	}
	assertDiffFields(t, rep, "version", "spec.image", "spec.replicas")

	// 边缘追平后恢复一致
	edge.ApplyDesired(v2)
	if rep := edge.Consistency("app"); !rep.Consistent {
		t.Fatalf("expected consistent after catch-up, got %+v", rep)
	}

	// 同版本内容漂移（模拟权威副本被带外改写）：版本相同但内容对不上
	drifted := v2
	drifted.Spec.Image = "app:v2-tampered"
	if err := cloud.putDesiredLocked(drifted); err != nil {
		t.Fatal(err)
	}
	rep = edge.Consistency("app")
	for _, d := range rep.Diffs {
		t.Logf("差异: field=%s cloud=%s edge=%s", d.Field, d.Cloud, d.Edge)
	}
	t.Logf("输入: 云端 v2 内容被带外改写; 判定结果: consistent=%v diffs=%v", rep.Consistent, rep.Diffs)
	if rep.Consistent {
		t.Fatal("same-version content drift must be reported inconsistent")
	}
	assertDiffFields(t, rep, "spec.image") // 版本相同，只有内容差异

	// 单边缺失
	rep = edge.Consistency("ghost")
	t.Logf("输入: 查询不存在的工作负载; 判定结果: %+v", rep)
	if rep.CloudFound || rep.EdgeFound {
		t.Fatalf("ghost workload should not be found: %+v", rep)
	}
}

func assertDiffFields(t *testing.T, rep ConsistencyReport, want ...string) {
	t.Helper()
	got := map[string]bool{}
	for _, d := range rep.Diffs {
		got[d.Field] = true
	}
	for _, w := range want {
		if !got[w] {
			t.Fatalf("expected diff on field %q, got diffs %+v", w, rep.Diffs)
		}
	}
}
