package registry

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"time"
)

// Store persists a Snapshot to disk.
//
// Durability strategy: write to a sibling temp file, fsync it, rename over the
// target, then fsync the directory. Rename within a directory is atomic on both
// Linux and Windows-with-ReplaceFile semantics, so a reader (or a crashed
// gateway) can only ever observe the complete old document or the complete new
// one -- never a half-written registry. That matters because the registry is
// the thing the "ketahanan konfigurasi" requirement is judged on: a torn write
// during a crash would silently lose routing metadata.
type Store struct {
	path string
}

// NewStore returns a store rooted at dir/registry.json.
func NewStore(dir string) *Store {
	return &Store{path: filepath.Join(dir, "registry.json")}
}

// Path is the file the store reads and writes.
func (s *Store) Path() string { return s.path }

// Load reads the persisted snapshot.
//
// The three outcomes are deliberately distinguished by the caller:
//   - (snap, true, nil)  -- a valid document was restored
//   - (zero, false, nil) -- no document exists yet, seed defaults
//   - (zero, false, err) -- a document exists but is unusable
//
// An unusable document is never silently discarded. Overwriting it with fresh
// defaults would destroy exactly the operator state the specification asks the
// gateway to preserve, so the file is quarantined and the error surfaced.
func (s *Store) Load() (Snapshot, bool, error) {
	raw, err := os.ReadFile(s.path)
	if os.IsNotExist(err) {
		return Snapshot{}, false, nil
	}
	if err != nil {
		return Snapshot{}, false, fmt.Errorf("read registry: %w", err)
	}
	if len(raw) == 0 {
		return Snapshot{}, false, fmt.Errorf("registry file %s is empty", s.path)
	}

	var snap Snapshot
	if err := json.Unmarshal(raw, &snap); err != nil {
		return Snapshot{}, false, fmt.Errorf("parse registry: %w", err)
	}
	if snap.SchemaVersion != CurrentSchemaVersion {
		// Refusing an unknown schema is safer than best-effort interpretation:
		// a future document could bind adapters this build cannot honour.
		return Snapshot{}, false, fmt.Errorf(
			"registry schema version %d is not supported by this build (expected %d)",
			snap.SchemaVersion, CurrentSchemaVersion)
	}
	if snap.Services == nil {
		snap.Services = map[string]Service{}
	}
	if snap.AdapterSpecs == nil {
		snap.AdapterSpecs = map[string]json.RawMessage{}
	}
	for id, svc := range snap.Services {
		if svc.ServiceID == "" {
			svc.ServiceID = id
			snap.Services[id] = svc
		}
		if svc.ServiceID != id {
			return Snapshot{}, false, fmt.Errorf("registry key %q does not match service_id %q", id, svc.ServiceID)
		}
		if err := svc.Validate(); err != nil {
			return Snapshot{}, false, fmt.Errorf("registry entry %q is invalid: %w", id, err)
		}
	}
	return snap, true, nil
}

// Quarantine moves an unreadable registry aside so the gateway can boot on
// defaults without destroying the evidence.
func (s *Store) Quarantine() (string, error) {
	dst := fmt.Sprintf("%s.corrupt-%d", s.path, time.Now().UnixMilli())
	if err := os.Rename(s.path, dst); err != nil {
		return "", err
	}
	return dst, nil
}

// Save writes the snapshot atomically and durably.
func (s *Store) Save(snap Snapshot) error {
	snap.SchemaVersion = CurrentSchemaVersion
	snap.UpdatedAt = time.Now().UTC().Format(time.RFC3339Nano)

	buf, err := json.MarshalIndent(snap, "", "  ")
	if err != nil {
		return fmt.Errorf("encode registry: %w", err)
	}
	buf = append(buf, '\n')

	dir := filepath.Dir(s.path)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return fmt.Errorf("create state dir: %w", err)
	}

	tmp, err := os.CreateTemp(dir, ".registry-*.tmp")
	if err != nil {
		return fmt.Errorf("create temp registry: %w", err)
	}
	tmpName := tmp.Name()
	// Any failure after this point must not leave debris behind.
	defer func() { _ = os.Remove(tmpName) }()

	if _, err := tmp.Write(buf); err != nil {
		tmp.Close()
		return fmt.Errorf("write temp registry: %w", err)
	}
	if err := tmp.Sync(); err != nil {
		tmp.Close()
		return fmt.Errorf("sync temp registry: %w", err)
	}
	if err := tmp.Close(); err != nil {
		return fmt.Errorf("close temp registry: %w", err)
	}
	if err := os.Rename(tmpName, s.path); err != nil {
		return fmt.Errorf("commit registry: %w", err)
	}
	syncDir(dir)
	return nil
}

// syncDir flushes the directory entry so the rename itself survives a crash.
// Best-effort: some filesystems (and Windows) do not permit opening a directory
// for sync, and a failure there is not worth failing the mutation over.
func syncDir(dir string) {
	d, err := os.Open(dir)
	if err != nil {
		return
	}
	defer d.Close()
	_ = d.Sync()
}
