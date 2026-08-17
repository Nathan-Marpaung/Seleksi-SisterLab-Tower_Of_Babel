package registry

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
)

func seed() SeedOptions {
	return SeedOptions{
		ServiceAEndpoint: "http://service-a:8101",
		ServiceBEndpoint: "service-b:8201",
		ServiceCEndpoint: "service-c:8301",
	}
}

func openTemp(t *testing.T) (*Registry, *Store, string) {
	t.Helper()
	dir := t.TempDir()
	store := NewStore(dir)
	reg, warn, err := Open(store, seed())
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	if warn != "" {
		t.Fatalf("unexpected warning on a fresh registry: %s", warn)
	}
	return reg, store, dir
}

// TestSeedsThenRestoresAcrossRestart is the configuration-durability
// requirement: what an operator changed at runtime must still be there after
// the process comes back.
func TestSeedsThenRestoresAcrossRestart(t *testing.T) {
	reg, store, dir := openTemp(t)
	if reg.Source() != SourceSeeded {
		t.Fatalf("source = %q, want %q", reg.Source(), SourceSeeded)
	}

	if err := reg.SetEnabled("service-c", false); err != nil {
		t.Fatalf("disable: %v", err)
	}
	if err := reg.SetOperationReplaySafe("service-b", "reverse", true); err != nil {
		t.Fatalf("replay-safe: %v", err)
	}
	if err := reg.AddVariant("service-b", Variant{Version: 2, Weight: 0, AdapterName: "tcp-frame-json-v2"}); err != nil {
		t.Fatalf("add variant: %v", err)
	}
	if err := reg.SetVariantWeights("service-b", map[int]int{1: 70, 2: 30}); err != nil {
		t.Fatalf("weights: %v", err)
	}
	revision := reg.Revision()

	// Reopen from the same directory, exactly as a restart would.
	reopened, warn, err := Open(NewStore(dir), seed())
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	if warn != "" {
		t.Fatalf("unexpected warning: %s", warn)
	}
	if reopened.Source() != SourceRestored {
		t.Errorf("source = %q, want %q", reopened.Source(), SourceRestored)
	}
	if reopened.Revision() != revision {
		t.Errorf("revision = %d, want %d", reopened.Revision(), revision)
	}

	c, _ := reopened.Get("service-c")
	if c.Enabled {
		t.Error("service-c came back enabled after being disabled")
	}
	b, _ := reopened.Get("service-b")
	if op, ok := b.Operation("reverse"); !ok || !op.ReplaySafe {
		t.Error("the replay-safe flag did not survive the restart")
	}
	weights := map[int]int{}
	for _, v := range b.Variants {
		weights[v.Version] = v.Weight
	}
	if weights[1] != 70 || weights[2] != 30 {
		t.Errorf("variant weights = %v, want {1:70 2:30}", weights)
	}

	if _, err := os.Stat(store.Path()); err != nil {
		t.Errorf("registry file missing: %v", err)
	}
}

// TestRejectedMutationLeavesLiveConfigurationIntact pins the rule that memory
// and disk never diverge.
func TestRejectedMutationLeavesLiveConfigurationIntact(t *testing.T) {
	reg, _, _ := openTemp(t)
	before := reg.Revision()

	// A variant with no adapter binding cannot be routed, so it must not be
	// committed.
	err := reg.Upsert(Service{
		ServiceID: "service-a", Protocol: ProtocolHTTPJSON, Endpoint: "http://x:1", Enabled: true,
		Operations: []Operation{{Name: "echo"}},
		Variants:   []Variant{{Version: 1, Weight: 100}},
	})
	if err == nil {
		t.Fatal("a variant with no adapter_name was accepted")
	}
	if reg.Revision() != before {
		t.Errorf("revision moved to %d on a rejected mutation", reg.Revision())
	}
	a, _ := reg.Get("service-a")
	if len(a.Variants) != 1 || a.Variants[0].AdapterName == "" {
		t.Errorf("live service-a was damaged by a rejected mutation: %+v", a)
	}
}

// TestUnknownVersionWeightIsRefused stops a migration shifting traffic to a
// version that does not exist.
func TestUnknownVersionWeightIsRefused(t *testing.T) {
	reg, _, _ := openTemp(t)
	if err := reg.SetVariantWeights("service-b", map[int]int{9: 100}); err == nil {
		t.Fatal("weight was accepted for an unregistered version")
	}
	b, _ := reg.Get("service-b")
	if b.Variants[0].Weight != 100 {
		t.Errorf("existing weights were disturbed: %+v", b.Variants)
	}
}

// TestAdapterStillBoundCannotBeDeleted: dropping the adapter a live variant
// uses would leave the service unroutable.
func TestAdapterStillBoundCannotBeDeleted(t *testing.T) {
	reg, _, _ := openTemp(t)
	if err := reg.PutAdapterSpec("tcp-frame-json-v1", map[string]any{"name": "tcp-frame-json-v1"}); err != nil {
		t.Fatalf("put spec: %v", err)
	}
	if err := reg.DeleteAdapterSpec("tcp-frame-json-v1"); err == nil {
		t.Fatal("an adapter still bound by a variant was deleted")
	}
}

// TestAdapterSpecsSurviveARestartByteForByte guards a bug that golden vectors
// caught in practice: a spec round-tripped through a generic decode turns its
// 64-bit correlation identifiers into float64, silently rounding them, and the
// adapter then fails its own self-test after a restart.
func TestAdapterSpecsSurviveARestartByteForByte(t *testing.T) {
	dir := t.TempDir()
	reg, _, err := Open(NewStore(dir), seed())
	if err != nil {
		t.Fatalf("open: %v", err)
	}

	// A value that cannot survive a float64 round trip.
	const exact = uint64(1234605616436508552)
	spec := map[string]any{
		"name":       "tcp-frame-json-v2",
		"service_id": "service-b",
		"self_test":  []any{map[string]any{"correlation_id": exact}},
	}
	if err := reg.PutAdapterSpec("tcp-frame-json-v2", spec); err != nil {
		t.Fatalf("put spec: %v", err)
	}

	reopened, _, err := Open(NewStore(dir), seed())
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	raw, ok := reopened.AdapterSpecs()["tcp-frame-json-v2"]
	if !ok {
		t.Fatal("the adapter spec did not survive the restart")
	}

	var restored struct {
		SelfTest []struct {
			CorrelationID uint64 `json:"correlation_id"`
		} `json:"self_test"`
	}
	if err := json.Unmarshal(raw, &restored); err != nil {
		t.Fatalf("decode restored spec: %v", err)
	}
	if len(restored.SelfTest) != 1 || restored.SelfTest[0].CorrelationID != exact {
		t.Fatalf("correlation id came back as %d, want %d", restored.SelfTest[0].CorrelationID, exact)
	}
}

// TestCorruptRegistryIsQuarantinedNotOverwritten: an unreadable document is
// operator state, and destroying it would be the opposite of durability.
func TestCorruptRegistryIsQuarantinedNotOverwritten(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "registry.json")
	if err := os.WriteFile(path, []byte("{this is not json"), 0o644); err != nil {
		t.Fatalf("write: %v", err)
	}

	reg, warn, err := Open(NewStore(dir), seed())
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	if warn == "" {
		t.Error("a corrupt registry was recovered silently")
	}
	if reg.Source() != SourceQuarantine {
		t.Errorf("source = %q, want %q", reg.Source(), SourceQuarantine)
	}
	if len(reg.List()) != 3 {
		t.Errorf("expected the seeded defaults, got %d services", len(reg.List()))
	}

	entries, _ := os.ReadDir(dir)
	found := false
	for _, e := range entries {
		if strings.Contains(e.Name(), ".corrupt-") {
			found = true
		}
	}
	if !found {
		t.Error("the unreadable document was not preserved for inspection")
	}
}

// TestUnknownSchemaVersionIsRefused: a document written by a future build may
// bind adapters this one cannot honour.
func TestUnknownSchemaVersionIsRefused(t *testing.T) {
	dir := t.TempDir()
	doc := map[string]any{"schema_version": CurrentSchemaVersion + 1, "services": map[string]any{}}
	raw, _ := json.Marshal(doc)
	if err := os.WriteFile(filepath.Join(dir, "registry.json"), raw, 0o644); err != nil {
		t.Fatalf("write: %v", err)
	}
	if _, _, err := NewStore(dir).Load(); err == nil {
		t.Fatal("a future schema version was accepted")
	}
}

// TestCandidatesAreDeterministic underpins reproducible fallback.
func TestCandidatesAreDeterministic(t *testing.T) {
	reg, _, _ := openTemp(t)
	want := []string{}
	for _, svc := range reg.Candidates("echo") {
		want = append(want, svc.ServiceID)
	}
	if len(want) != 3 || want[0] != "service-a" {
		t.Fatalf("echo candidates = %v, want service-a first", want)
	}
	for i := 0; i < 20; i++ {
		got := []string{}
		for _, svc := range reg.Candidates("echo") {
			got = append(got, svc.ServiceID)
		}
		if strings.Join(got, ",") != strings.Join(want, ",") {
			t.Fatalf("candidate order changed between calls: %v then %v", want, got)
		}
	}

	if ids := ids(reg.Candidates("reverse")); strings.Join(ids, ",") != "service-b" {
		t.Errorf("reverse candidates = %v, want only service-b", ids)
	}
	if ids := ids(reg.Candidates("sum")); strings.Join(ids, ",") != "service-b,service-c" {
		t.Errorf("sum candidates = %v, want service-b then service-c", ids)
	}
}

// TestDisabledServiceIsNotACandidate.
func TestDisabledServiceIsNotACandidate(t *testing.T) {
	reg, _, _ := openTemp(t)
	if err := reg.SetEnabled("service-a", false); err != nil {
		t.Fatalf("disable: %v", err)
	}
	for _, svc := range reg.Candidates("echo") {
		if svc.ServiceID == "service-a" {
			t.Fatal("a disabled service was offered as a routing candidate")
		}
	}
}

// TestReturnedServicesAreCopies stops a caller mutating live routing state.
func TestReturnedServicesAreCopies(t *testing.T) {
	reg, _, _ := openTemp(t)
	svc, _ := reg.Get("service-a")
	svc.Operations[0].Name = "hijacked"
	svc.Variants[0].Weight = 0

	again, _ := reg.Get("service-a")
	if again.Operations[0].Name == "hijacked" || again.Variants[0].Weight == 0 {
		t.Fatal("mutating a returned service changed the registry")
	}
}

// TestConcurrentMutationsAndReadsAreSafe is a race-detector target.
func TestConcurrentMutationsAndReadsAreSafe(t *testing.T) {
	reg, _, _ := openTemp(t)
	var wg sync.WaitGroup

	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			for n := 0; n < 20; n++ {
				reg.Candidates("echo")
				reg.List()
				reg.HealthOf("service-b")
				reg.SetHealth("service-b", "available", "probe")
			}
		}(i)
	}
	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			for n := 0; n < 10; n++ {
				_ = reg.SetEnabled("service-c", n%2 == 0)
			}
		}(i)
	}
	wg.Wait()
}

func ids(services []Service) []string {
	out := make([]string, 0, len(services))
	for _, s := range services {
		out = append(out, s.ServiceID)
	}
	return out
}
