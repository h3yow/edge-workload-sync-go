package edgesync

import (
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"sync"
)

// Store is a small JSON-file-backed persistence used to simulate durable
// state on both the cloud and the edge ends. All writes are atomic
// (write tmp file + rename) and the whole store is guarded by a mutex so a
// Store is safe for concurrent use.
type Store struct {
	mu   sync.Mutex
	path string
	data storeData
}

type workloadRecord struct {
	Desired       DesiredState    `json:"desired"`
	HasDesired    bool            `json:"has_desired"`
	AppliedChange map[string]bool `json:"applied_change"` // change IDs already applied (idempotency)
}

type storeData struct {
	Workloads map[string]*workloadRecord `json:"workloads"`
}

// OpenStore loads (or creates) the store at path.
func OpenStore(path string) (*Store, error) {
	s := &Store{path: path, data: storeData{Workloads: map[string]*workloadRecord{}}}
	b, err := os.ReadFile(path)
	switch {
	case err == nil:
		if err := json.Unmarshal(b, &s.data); err != nil {
			return nil, err
		}
		if s.data.Workloads == nil {
			s.data.Workloads = map[string]*workloadRecord{}
		}
	case errors.Is(err, os.ErrNotExist):
		if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
			return nil, err
		}
		if err := s.flushLocked(); err != nil {
			return nil, err
		}
	default:
		return nil, err
	}
	return s, nil
}

func (s *Store) recordLocked(workloadID string) *workloadRecord {
	r, ok := s.data.Workloads[workloadID]
	if !ok {
		r = &workloadRecord{AppliedChange: map[string]bool{}}
		s.data.Workloads[workloadID] = r
	}
	if r.AppliedChange == nil {
		r.AppliedChange = map[string]bool{}
	}
	return r
}

func (s *Store) flushLocked() error {
	b, err := json.MarshalIndent(&s.data, "", "  ")
	if err != nil {
		return err
	}
	tmp := s.path + ".tmp"
	if err := os.WriteFile(tmp, b, 0o644); err != nil {
		return err
	}
	return os.Rename(tmp, s.path)
}

// GetDesired returns the persisted desired state for a workload.
func (s *Store) GetDesired(workloadID string) (DesiredState, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	r, ok := s.data.Workloads[workloadID]
	if !ok || !r.HasDesired {
		return DesiredState{}, false, nil
	}
	return r.Desired, true, nil
}

// PutDesired persists the desired state for a workload.
func (s *Store) PutDesired(d DesiredState) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	r := s.recordLocked(d.WorkloadID)
	r.Desired = d
	r.HasDesired = true
	return s.flushLocked()
}

// HasChange reports whether a change ID was already applied.
func (s *Store) HasChange(workloadID, changeID string) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	r, ok := s.data.Workloads[workloadID]
	if !ok {
		return false, nil
	}
	return r.AppliedChange[changeID], nil
}

// MarkChange records a change ID as applied.
func (s *Store) MarkChange(workloadID, changeID string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	r := s.recordLocked(workloadID)
	r.AppliedChange[changeID] = true
	return s.flushLocked()
}

// AppliedChanges returns the set of applied change IDs for a workload.
func (s *Store) AppliedChanges(workloadID string) (map[string]bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := map[string]bool{}
	if r, ok := s.data.Workloads[workloadID]; ok {
		for id := range r.AppliedChange {
			out[id] = true
		}
	}
	return out, nil
}

// ApplyChangeTx atomically checks idempotency, applies fn (which may
// update the desired state), marks the change applied, and persists.
// It returns (alreadyApplied, error).
func (s *Store) ApplyChangeTx(workloadID, changeID string, fn func(rec *workloadRecord) error) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	r := s.recordLocked(workloadID)
	if r.AppliedChange[changeID] {
		return true, nil
	}
	if err := fn(r); err != nil {
		return false, err
	}
	r.AppliedChange[changeID] = true
	return false, s.flushLocked()
}
