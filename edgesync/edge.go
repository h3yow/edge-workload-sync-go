package edgesync

import (
	"fmt"
	"sync"
)

// Edge is an edge node: it applies desired states in version order, keeps
// running on the last acknowledged desired state while disconnected, and
// stages local changes for ordered, idempotent replay on reconnect.
type Edge struct {
	mu        sync.Mutex
	store     *Store
	cloud     *Cloud
	connected bool
	staged    []Change // local changes staged while disconnected, in order
}

// NewEdge creates an edge node backed by the given store.
func NewEdge(store *Store) *Edge {
	return &Edge{store: store, connected: true}
}

// Attach binds the edge to a cloud controller (the "network").
func (e *Edge) Attach(cloud *Cloud) { e.cloud = cloud }

// SetConnected toggles connectivity to the cloud.
func (e *Edge) SetConnected(connected bool) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.connected = connected
}

// ReceiveDesired processes a desired state delivered from the cloud.
// Identical re-delivery is an idempotent no-op; version regression (or the
// same version with different content) is rejected; skipping intermediate
// versions is allowed — the newest version always wins.
func (e *Edge) ReceiveDesired(d DesiredState) Decision {
	e.mu.Lock()
	defer e.mu.Unlock()
	dec := Decision{Workload: d.WorkloadID, Version: d.Version}

	cur, has, err := e.store.GetDesired(d.WorkloadID)
	if err != nil {
		dec.Reason, dec.Detail = ReasonUnknownWorkload, err.Error()
		return dec
	}
	if has {
		switch d.Version.Compare(cur.Version) {
		case 0:
			if desiredEqual(cur, d) {
				dec.Reason = ReasonDuplicate
				dec.Detail = fmt.Sprintf("version %d already running, identical re-delivery ignored", d.Version)
				return dec
			}
			dec.Reason = ReasonVersionRegression
			dec.Detail = fmt.Sprintf("version %d already exists with different content", d.Version)
			return dec
		case -1:
			dec.Reason = ReasonVersionRegression
			dec.Detail = fmt.Sprintf("version %d regresses below current %d", d.Version, cur.Version)
			return dec
		}
	}
	if err := e.store.PutDesired(d); err != nil {
		dec.Reason, dec.Detail = ReasonUnknownWorkload, err.Error()
		return dec
	}
	dec.Applied = true
	if has {
		dec.Detail = fmt.Sprintf("desired version %d -> %d applied", cur.Version, d.Version)
	} else {
		dec.Detail = fmt.Sprintf("initial desired version %d applied", d.Version)
	}
	return dec
}

// Current returns the desired state the edge is currently running.
func (e *Edge) Current(workloadID string) (DesiredState, bool, error) {
	return e.store.GetDesired(workloadID)
}

// ProposeLocal records a local change. While connected it is submitted to
// the cloud immediately and, if accepted, the resulting desired state is
// applied locally. While disconnected it is staged against the current
// local version and replayed on reconnect.
func (e *Edge) ProposeLocal(ch Change) Decision {
	e.mu.Lock()
	cur, has, err := e.store.GetDesired(ch.WorkloadID)
	connected := e.connected
	cloud := e.cloud
	e.mu.Unlock()

	if err != nil {
		return Decision{Workload: ch.WorkloadID, Reason: ReasonUnknownWorkload, Detail: err.Error()}
	}
	if !has {
		return Decision{Workload: ch.WorkloadID, Reason: ReasonUnknownWorkload,
			Detail: fmt.Sprintf("workload %s has no desired state", ch.WorkloadID)}
	}

	if connected && cloud != nil {
		ch.BaseVersion = cur.Version
		dec := cloud.SubmitChange(ch)
		if dec.Applied {
			if next, ok, err := cloud.GetDesired(ch.WorkloadID); err == nil && ok {
				e.ReceiveDesired(next)
			}
		}
		return dec
	}

	e.mu.Lock()
	// Predicted base: staged changes chain on top of each other, so the
	// n-th staged change is based on the version the previous ones will
	// produce. If the cloud moved ahead independently, the base is stale
	// and the change is rejected rather than silently applied.
	ch.BaseVersion = cur.Version + Version(len(e.staged))
	e.staged = append(e.staged, ch)
	n := len(e.staged)
	e.mu.Unlock()
	return Decision{Applied: true, Workload: ch.WorkloadID, Version: ch.BaseVersion,
		Detail: fmt.Sprintf("change %s staged offline (queue depth %d), base version %d", ch.ID, n, ch.BaseVersion)}
}

// Staged returns a copy of the staged (not yet replayed) local changes in
// order.
func (e *Edge) Staged() []Change {
	e.mu.Lock()
	defer e.mu.Unlock()
	out := make([]Change, len(e.staged))
	copy(out, e.staged)
	return out
}

// Replay pushes staged changes to the cloud in order. Each change gets a
// definitive outcome: applied, duplicate (already applied upstream —
// idempotent), or rejected (stale base, unauthorized, quota). Rejected
// changes are discarded rather than silently applied. The queue is cleared
// once every entry has a definitive outcome, so replay is safe to repeat.
func (e *Edge) Replay() []Decision {
	e.mu.Lock()
	if !e.connected || e.cloud == nil {
		e.mu.Unlock()
		return nil
	}
	cloud := e.cloud
	staged := make([]Change, len(e.staged))
	copy(staged, e.staged)
	e.staged = nil
	e.mu.Unlock()

	decisions := make([]Decision, 0, len(staged))
	for _, ch := range staged {
		// Pull the newest desired state first so the edge converges to the
		// authoritative version before attempting the change.
		if next, ok, err := cloud.GetDesired(ch.WorkloadID); err == nil && ok {
			e.ReceiveDesired(next)
		}
		dec := cloud.SubmitChange(ch)
		if dec.Applied {
			if next, ok, err := cloud.GetDesired(ch.WorkloadID); err == nil && ok {
				e.ReceiveDesired(next)
			}
		}
		decisions = append(decisions, dec)
	}
	return decisions
}
