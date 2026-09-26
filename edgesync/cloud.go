package edgesync

import (
	"fmt"
	"sync"
)

// Authorizer decides whether a principal may perform a protected change.
type Authorizer interface {
	Allowed(principal, workloadID string) bool
}

// AuthorizerFunc adapts a function to Authorizer.
type AuthorizerFunc func(principal, workloadID string) bool

// Allowed implements Authorizer.
func (f AuthorizerFunc) Allowed(principal, workloadID string) bool {
	return f(principal, workloadID)
}

// Cloud is the cloud-side controller: it owns the authoritative desired
// versions, validates protected changes, and converges concurrent updates.
// All mutating operations are serialized through mu, so concurrent
// submissions converge to a single total order per workload.
type Cloud struct {
	mu    sync.Mutex
	store *Store
	auth  Authorizer
	quota int // max replicas per workload
}

// NewCloud creates a cloud controller backed by the given store.
// replicaQuota <= 0 means no quota.
func NewCloud(store *Store, auth Authorizer, replicaQuota int) *Cloud {
	return &Cloud{store: store, auth: auth, quota: replicaQuota}
}

// SetDesired publishes a new desired state. Version regression is
// rejected; re-publishing the identical version is an idempotent no-op;
// skipping intermediate versions is allowed.
func (c *Cloud) SetDesired(d DesiredState) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	cur, has, err := c.store.GetDesired(d.WorkloadID)
	if err != nil {
		return err
	}
	if has {
		switch d.Version.Compare(cur.Version) {
		case 0:
			if desiredEqual(cur, d) {
				return nil // idempotent re-delivery
			}
			return &RejectError{Reason: ReasonVersionRegression,
				Message: fmt.Sprintf("workload %s: version %d already exists with different content", d.WorkloadID, d.Version)}
		case -1:
			return &RejectError{Reason: ReasonVersionRegression,
				Message: fmt.Sprintf("workload %s: version %d regresses below current %d", d.WorkloadID, d.Version, cur.Version)}
		}
	}
	return c.store.PutDesired(d)
}

// GetDesired returns the current authoritative desired state.
func (c *Cloud) GetDesired(workloadID string) (DesiredState, bool, error) {
	return c.store.GetDesired(workloadID)
}

// SubmitChange applies an edge-originated change (used during replay and
// for direct cloud-side submissions). Checks, in order: idempotency,
// base-version freshness, authorization for protected changes, and
// replica quota. Concurrent submissions are serialized: the first to
// commit bumps the version, so a conflicting change with the same base
// version is then rejected as stale — the final state is unique and
// explainable regardless of arrival path.
func (c *Cloud) SubmitChange(ch Change) Decision {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.submitLocked(ch)
}

func (c *Cloud) submitLocked(ch Change) Decision {
	dec := Decision{Workload: ch.WorkloadID}

	cur, has, err := c.store.GetDesired(ch.WorkloadID)
	if err != nil {
		dec.Reason, dec.Detail = ReasonUnknownWorkload, err.Error()
		return dec
	}
	if !has {
		dec.Reason = ReasonUnknownWorkload
		dec.Detail = fmt.Sprintf("workload %s has no desired state", ch.WorkloadID)
		return dec
	}
	dec.Version = cur.Version

	// Idempotency: a change ID may only take effect once.
	seen, err := c.store.HasChange(ch.WorkloadID, ch.ID)
	if err != nil {
		dec.Reason, dec.Detail = ReasonUnknownWorkload, err.Error()
		return dec
	}
	if seen {
		dec.Reason = ReasonDuplicate
		dec.Detail = fmt.Sprintf("change %s already applied at version %d", ch.ID, cur.Version)
		return dec
	}

	// Freshness: the change must be based on the current version; a change
	// superseded by a newer version is rejected, not silently applied.
	if ch.BaseVersion != cur.Version {
		dec.Reason = ReasonStaleBase
		dec.Detail = fmt.Sprintf("change %s based on version %d but current is %d", ch.ID, ch.BaseVersion, cur.Version)
		return dec
	}

	// Authorization for protected changes.
	if ch.Protected && (c.auth == nil || !c.auth.Allowed(ch.Principal, ch.WorkloadID)) {
		dec.Reason = ReasonUnauthorized
		dec.Detail = fmt.Sprintf("principal %q not authorized for protected change on %s", ch.Principal, ch.WorkloadID)
		return dec
	}

	// Quota.
	if c.quota > 0 && ch.Spec.Replicas > c.quota {
		dec.Reason = ReasonQuotaExceeded
		dec.Detail = fmt.Sprintf("replicas %d exceed quota %d", ch.Spec.Replicas, c.quota)
		return dec
	}

	// Commit: bump version and record the change ID atomically.
	next := DesiredState{WorkloadID: ch.WorkloadID, Version: cur.Version + 1, Spec: ch.Spec}
	dup, err := c.store.ApplyChangeTx(ch.WorkloadID, ch.ID, func(rec *workloadRecord) error {
		rec.Desired = next
		rec.HasDesired = true
		return nil
	})
	if err != nil {
		dec.Reason, dec.Detail = ReasonUnknownWorkload, err.Error()
		return dec
	}
	if dup {
		dec.Reason = ReasonDuplicate
		dec.Detail = fmt.Sprintf("change %s already applied", ch.ID)
		return dec
	}
	dec.Applied = true
	dec.Version = next.Version
	dec.Detail = fmt.Sprintf("change %s applied, version %d -> %d", ch.ID, cur.Version, next.Version)
	return dec
}

func desiredEqual(a, b DesiredState) bool {
	if a.WorkloadID != b.WorkloadID || a.Version != b.Version ||
		a.Spec.Image != b.Spec.Image || a.Spec.Replicas != b.Spec.Replicas ||
		a.Spec.Protected != b.Spec.Protected || len(a.Spec.Env) != len(b.Spec.Env) {
		return false
	}
	for k, v := range a.Spec.Env {
		if b.Spec.Env[k] != v {
			return false
		}
	}
	return true
}
