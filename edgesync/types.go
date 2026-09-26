// Package edgesync implements a declarative workload synchronization
// mechanism between a cloud controller and edge nodes, with local
// persistence simulating both ends.
package edgesync

// Version is a monotonically comparable desired-state version.
type Version int64

// Compare returns -1, 0 or 1 comparing v to o.
func (v Version) Compare(o Version) int {
	switch {
	case v < o:
		return -1
	case v > o:
		return 1
	default:
		return 0
	}
}

// Spec is the declarative desired configuration of a workload.
type Spec struct {
	Image     string            `json:"image"`
	Replicas  int               `json:"replicas"`
	Env       map[string]string `json:"env"`
	Protected bool              `json:"protected"` // protected changes require authorization
}

// DesiredState is a versioned desired state for one workload.
type DesiredState struct {
	WorkloadID string  `json:"workload_id"`
	Version    Version `json:"version"`
	Spec       Spec    `json:"spec"`
}

// Change is a locally produced mutation on the edge node, staged while
// disconnected and replayed upstream on reconnect.
type Change struct {
	ID            string  `json:"id"` // idempotency key
	WorkloadID    string  `json:"workload_id"`
	BaseVersion   Version `json:"base_version"` // desired version the change was made against
	Spec          Spec    `json:"spec"`
	Protected     bool    `json:"protected"`
	Principal     string  `json:"principal"` // who requested the change
	ReplicasDelta int     `json:"replicas_delta"`
}

// RejectReason classifies why an operation was rejected, so callers can
// distinguish causes programmatically.
type RejectReason string

const (
	ReasonVersionRegression RejectReason = "version_regression" // version rollback rejected
	ReasonStaleBase         RejectReason = "stale_base"         // change superseded by a newer version
	ReasonDuplicate         RejectReason = "duplicate"          // already applied (idempotent no-op)
	ReasonUnauthorized      RejectReason = "unauthorized"       // protected change without grant
	ReasonQuotaExceeded     RejectReason = "quota_exceeded"     // replica quota exceeded
	ReasonUnknownWorkload   RejectReason = "unknown_workload"
)

// RejectError is a rejection with a distinguishable reason.
type RejectError struct {
	Reason  RejectReason
	Message string
}

func (e *RejectError) Error() string { return string(e.Reason) + ": " + e.Message }

// Decision records the outcome of processing one item, for logging/tests.
type Decision struct {
	Applied  bool
	Reason   RejectReason // set when !Applied
	Detail   string
	Workload string
	Version  Version
}
