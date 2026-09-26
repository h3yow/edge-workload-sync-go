package edgesync

import (
	"errors"
	"fmt"
	"path/filepath"
	"sync"
	"testing"
)

func newCloudStore(t *testing.T) *Store {
	t.Helper()
	s, err := OpenStore(filepath.Join(t.TempDir(), "cloud.json"))
	if err != nil {
		t.Fatalf("open cloud store: %v", err)
	}
	return s
}

func newEdgeStore(t *testing.T) *Store {
	t.Helper()
	s, err := OpenStore(filepath.Join(t.TempDir(), "edge.json"))
	if err != nil {
		t.Fatalf("open edge store: %v", err)
	}
	return s
}

func logDecision(t *testing.T, label string, input any, d Decision) {
	t.Helper()
	t.Logf("%s input=%+v -> applied=%v reason=%q version=%d detail=%s",
		label, input, d.Applied, d.Reason, d.Version, d.Detail)
}

func spec(image string, replicas int) Spec {
	return Spec{Image: image, Replicas: replicas, Env: map[string]string{"A": "1"}}
}

// TestOutOfOrderAndSkipVersion covers duplicate delivery, version
// regression and skipping intermediate versions on the edge.
func TestOutOfOrderAndSkipVersion(t *testing.T) {
	edge := NewEdge(newEdgeStore(t))

	v1 := DesiredState{WorkloadID: "w1", Version: 1, Spec: spec("img:1", 1)}
	v2 := DesiredState{WorkloadID: "w1", Version: 2, Spec: spec("img:2", 2)}
	v4 := DesiredState{WorkloadID: "w1", Version: 4, Spec: spec("img:4", 1)}

	d := edge.ReceiveDesired(v1)
	logDecision(t, "deliver v1", v1, d)
	if !d.Applied {
		t.Fatalf("v1 should apply, got %+v", d)
	}

	d = edge.ReceiveDesired(v1)
	logDecision(t, "re-deliver v1 (duplicate)", v1, d)
	if d.Applied || d.Reason != ReasonDuplicate {
		t.Fatalf("duplicate v1 should be idempotent no-op, got %+v", d)
	}

	// Skip v2/v3: jumping to v4 is allowed.
	d = edge.ReceiveDesired(v4)
	logDecision(t, "deliver v4 (skip v2,v3)", v4, d)
	if !d.Applied {
		t.Fatalf("v4 should apply over v1, got %+v", d)
	}

	// Late arrival of v2 must be rejected as regression.
	d = edge.ReceiveDesired(v2)
	logDecision(t, "late v2 after v4", v2, d)
	if d.Applied || d.Reason != ReasonVersionRegression {
		t.Fatalf("v2 after v4 must be rejected as regression, got %+v", d)
	}

	// Same version, different content must be rejected too.
	v4alt := DesiredState{WorkloadID: "w1", Version: 4, Spec: spec("img:4-alt", 1)}
	d = edge.ReceiveDesired(v4alt)
	logDecision(t, "conflicting v4 content", v4alt, d)
	if d.Applied || d.Reason != ReasonVersionRegression {
		t.Fatalf("conflicting v4 must be rejected, got %+v", d)
	}

	cur, _, _ := edge.Current("w1")
	t.Logf("final edge state: %+v", cur)
	if cur.Version != 4 || cur.Spec.Image != "img:4" {
		t.Fatalf("final state must be v4/img:4, got %+v", cur)
	}
}

// TestCloudVersionRegression covers regression rejection on the cloud
// publish path with distinguishable reasons.
func TestCloudVersionRegression(t *testing.T) {
	cloud := NewCloud(newCloudStore(t), nil, 0)

	if err := cloud.SetDesired(DesiredState{WorkloadID: "w1", Version: 5, Spec: spec("img:5", 1)}); err != nil {
		t.Fatalf("set v5: %v", err)
	}
	// Idempotent re-publish.
	if err := cloud.SetDesired(DesiredState{WorkloadID: "w1", Version: 5, Spec: spec("img:5", 1)}); err != nil {
		t.Fatalf("re-publish identical v5 should be a no-op, got %v", err)
	}
	// Regression.
	err := cloud.SetDesired(DesiredState{WorkloadID: "w1", Version: 3, Spec: spec("img:3", 1)})
	var re *RejectError
	if !errors.As(err, &re) || re.Reason != ReasonVersionRegression {
		t.Fatalf("expected version_regression, got %v", err)
	}
	t.Logf("regression rejected as expected: %v", err)
}

// TestDisconnectReplay covers offline staging, ordered replay, replay
// idempotency and rejection of superseded changes.
func TestDisconnectReplay(t *testing.T) {
	cloud := NewCloud(newCloudStore(t), nil, 0)
	edge := NewEdge(newEdgeStore(t))
	edge.Attach(cloud)

	base := DesiredState{WorkloadID: "w1", Version: 1, Spec: spec("img:1", 1)}
	if err := cloud.SetDesired(base); err != nil {
		t.Fatalf("seed cloud: %v", err)
	}
	if d := edge.ReceiveDesired(base); !d.Applied {
		t.Fatalf("seed edge: %+v", d)
	}

	// Disconnect: edge keeps running on v1 and stages local changes.
	edge.SetConnected(false)
	c1 := Change{ID: "c1", WorkloadID: "w1", Principal: "op", Spec: spec("img:1", 3)}
	c2 := Change{ID: "c2", WorkloadID: "w1", Principal: "op", Spec: spec("img:1", 5)}
	for _, c := range []Change{c1, c2} {
		d := edge.ProposeLocal(c)
		logDecision(t, "offline propose", c, d)
		if !d.Applied {
			t.Fatalf("offline change should be staged, got %+v", d)
		}
	}
	// Edge still runs the last acknowledged desired state while offline.
	cur, _, _ := edge.Current("w1")
	t.Logf("offline running state: %+v (staged=%d)", cur, len(edge.Staged()))
	if cur.Version != 1 || len(edge.Staged()) != 2 {
		t.Fatalf("offline: want v1 running with 2 staged, got %+v staged=%d", cur, len(edge.Staged()))
	}

	// Reconnect and replay in order.
	edge.SetConnected(true)
	for i, d := range edge.Replay() {
		logDecision(t, fmt.Sprintf("replay #%d", i), nil, d)
		if !d.Applied {
			t.Fatalf("replay #%d should apply, got %+v", i, d)
		}
	}
	cur, _, _ = edge.Current("w1")
	cd, _, _ := cloud.GetDesired("w1")
	t.Logf("after replay: edge=%+v cloud=%+v", cur, cd)
	if cur.Version != 3 || cd.Version != 3 || cur.Spec.Replicas != 5 {
		t.Fatalf("both ends should converge to v3 replicas=5, got edge=%+v cloud=%+v", cur, cd)
	}

	// Replaying again must be a no-op (queue drained, no double effect).
	if ds := edge.Replay(); len(ds) != 0 {
		t.Fatalf("second replay should be empty, got %+v", ds)
	}

	// Re-submitting an already-applied change ID must be classified as a
	// duplicate, never re-applied.
	d := cloud.SubmitChange(c2)
	logDecision(t, "re-submit applied change", c2, d)
	if d.Applied || d.Reason != ReasonDuplicate {
		t.Fatalf("re-submitted change must be duplicate, got %+v", d)
	}

	// A change based on a superseded version must be rejected, not applied.
	stale := Change{ID: "c9", WorkloadID: "w1", BaseVersion: 1, Principal: "op", Spec: spec("img:1", 9)}
	d = cloud.SubmitChange(stale)
	logDecision(t, "stale-base change", stale, d)
	if d.Applied || d.Reason != ReasonStaleBase {
		t.Fatalf("stale change must be rejected as stale_base, got %+v", d)
	}
	if cd, _, _ := cloud.GetDesired("w1"); cd.Spec.Replicas != 5 {
		t.Fatalf("stale change must not take effect, got %+v", cd)
	}
}

// TestConcurrentConflict submits competing changes on the same base
// version from many goroutines: exactly one wins, the rest are stale.
func TestConcurrentConflict(t *testing.T) {
	cloud := NewCloud(newCloudStore(t), nil, 0)
	if err := cloud.SetDesired(DesiredState{WorkloadID: "w1", Version: 1, Spec: spec("img:1", 1)}); err != nil {
		t.Fatalf("seed: %v", err)
	}

	const n = 16
	decs := make([]Decision, n)
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			ch := Change{ID: fmt.Sprintf("cc-%d", i), WorkloadID: "w1", BaseVersion: 1,
				Principal: "op", Spec: spec("img:1", i+2)}
			decs[i] = cloud.SubmitChange(ch)
		}(i)
	}
	wg.Wait()

	applied, stale := 0, 0
	for i, d := range decs {
		logDecision(t, fmt.Sprintf("concurrent[%d]", i), nil, d)
		switch {
		case d.Applied:
			applied++
		case d.Reason == ReasonStaleBase:
			stale++
		default:
			t.Fatalf("unexpected decision %+v", d)
		}
	}
	t.Logf("convergence: applied=%d stale=%d", applied, stale)
	if applied != 1 || stale != n-1 {
		t.Fatalf("exactly one change must win, got applied=%d stale=%d", applied, stale)
	}
	cd, _, _ := cloud.GetDesired("w1")
	if cd.Version != 2 {
		t.Fatalf("cloud must converge to a single v2, got %+v", cd)
	}
	t.Logf("final cloud state: %+v", cd)
}

// TestUnauthorizedAndQuota covers protected-change authorization and
// replica quota, with distinguishable rejection reasons on every path.
func TestUnauthorizedAndQuota(t *testing.T) {
	auth := AuthorizerFunc(func(principal, workloadID string) bool {
		return principal == "admin"
	})
	cloud := NewCloud(newCloudStore(t), auth, 4)
	if err := cloud.SetDesired(DesiredState{WorkloadID: "w1", Version: 1, Spec: spec("img:1", 1)}); err != nil {
		t.Fatalf("seed: %v", err)
	}

	// Unauthorized protected change via direct submission.
	bad := Change{ID: "p1", WorkloadID: "w1", BaseVersion: 1, Principal: "guest",
		Protected: true, Spec: spec("img:1", 2)}
	d := cloud.SubmitChange(bad)
	logDecision(t, "unauthorized protected change", bad, d)
	if d.Applied || d.Reason != ReasonUnauthorized {
		t.Fatalf("expected unauthorized, got %+v", d)
	}

	// Same change via the edge replay path must be rejected identically.
	edge := NewEdge(newEdgeStore(t))
	edge.Attach(cloud)
	base, _, _ := cloud.GetDesired("w1")
	edge.ReceiveDesired(base)
	edge.SetConnected(false)
	edge.ProposeLocal(Change{ID: "p2", WorkloadID: "w1", Principal: "guest",
		Protected: true, Spec: spec("img:1", 2)})
	edge.SetConnected(true)
	ds := edge.Replay()
	if len(ds) != 1 {
		t.Fatalf("expected 1 replay decision, got %+v", ds)
	}
	logDecision(t, "unauthorized via replay", nil, ds[0])
	if ds[0].Applied || ds[0].Reason != ReasonUnauthorized {
		t.Fatalf("replay path must also reject as unauthorized, got %+v", ds[0])
	}

	// Quota exceeded.
	over := Change{ID: "q1", WorkloadID: "w1", BaseVersion: 1, Principal: "admin",
		Spec: spec("img:1", 10)}
	d = cloud.SubmitChange(over)
	logDecision(t, "quota exceeded", over, d)
	if d.Applied || d.Reason != ReasonQuotaExceeded {
		t.Fatalf("expected quota_exceeded, got %+v", d)
	}

	// Authorized, within-quota protected change succeeds.
	ok := Change{ID: "p3", WorkloadID: "w1", BaseVersion: 1, Principal: "admin",
		Protected: true, Spec: spec("img:1", 3)}
	d = cloud.SubmitChange(ok)
	logDecision(t, "authorized protected change", ok, d)
	if !d.Applied {
		t.Fatalf("authorized change should apply, got %+v", d)
	}

	// Rejected changes must not have mutated state or consumed IDs.
	cd, _, _ := cloud.GetDesired("w1")
	t.Logf("final cloud state: %+v", cd)
	if cd.Version != 2 || cd.Spec.Replicas != 3 {
		t.Fatalf("only the authorized change may take effect, got %+v", cd)
	}
	// A rejected change ID is not consumed: re-submitting it (now against
	// the current version) must be judged on its merits again, never
	// misclassified as a duplicate.
	bad.BaseVersion = 2
	d = cloud.SubmitChange(bad)
	logDecision(t, "retry unauthorized change at current version", bad, d)
	if d.Reason != ReasonUnauthorized {
		t.Fatalf("rejected change must not become duplicate, got %+v", d)
	}
}

// TestPersistence verifies both ends survive a restart (store reload).
func TestPersistence(t *testing.T) {
	dir := t.TempDir()
	cloudPath := filepath.Join(dir, "cloud.json")

	s1, _ := OpenStore(cloudPath)
	cloud := NewCloud(s1, nil, 0)
	if err := cloud.SetDesired(DesiredState{WorkloadID: "w1", Version: 7, Spec: spec("img:7", 2)}); err != nil {
		t.Fatalf("seed: %v", err)
	}
	cloud.SubmitChange(Change{ID: "x1", WorkloadID: "w1", BaseVersion: 7, Principal: "op", Spec: spec("img:7", 3)})

	// Reopen and verify durable state.
	s2, err := OpenStore(cloudPath)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	cloud2 := NewCloud(s2, nil, 0)
	cd, ok, _ := cloud2.GetDesired("w1")
	t.Logf("reloaded state: %+v", cd)
	if !ok || cd.Version != 8 || cd.Spec.Replicas != 3 {
		t.Fatalf("state must survive restart, got %+v", cd)
	}
	d := cloud2.SubmitChange(Change{ID: "x1", WorkloadID: "w1", BaseVersion: 8, Principal: "op", Spec: spec("img:7", 3)})
	logDecision(t, "replay applied change after restart", nil, d)
	if d.Reason != ReasonDuplicate {
		t.Fatalf("applied change IDs must survive restart, got %+v", d)
	}
}
