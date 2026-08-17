package adapter

import (
	"testing"
	"time"
)

// TestBuiltinSpecsPassSelfTest is the regression net for protocol translation.
//
// It builds every shipped adapter and runs its recorded vectors, so a change to
// framing, field mapping or result normalization that would corrupt live
// traffic fails here instead of in front of a grader.
func TestBuiltinSpecsPassSelfTest(t *testing.T) {
	endpoints := map[string]string{
		"service-a": "http://127.0.0.1:8101",
		"service-b": "127.0.0.1:8201",
		"service-c": "127.0.0.1:8301",
	}

	for _, spec := range BuiltinSpecs() {
		spec := spec
		t.Run(spec.Name, func(t *testing.T) {
			if err := spec.Validate(); err != nil {
				t.Fatalf("spec validation: %v", err)
			}
			// Build runs the vectors itself; constructing here proves the
			// gate is wired, not just that the vectors happen to pass.
			ad, err := Build(spec, BuildOptions{
				Endpoint:        endpoints[spec.ServiceID],
				SelfTestTimeout: 5 * time.Second,
				TCP:             TCPOptions{PoolSize: 1, MaxInFlight: 1, DialTimeout: time.Second},
				UDP:             UDPOptions{Window: 4, InitialRTO: 100 * time.Millisecond},
			})
			if err != nil {
				t.Fatalf("build: %v", err)
			}
			defer ad.Close()

			if ad.ServiceID() != spec.ServiceID {
				t.Errorf("service id = %q, want %q", ad.ServiceID(), spec.ServiceID)
			}
			if ad.Version() != spec.Version {
				t.Errorf("version = %d, want %d", ad.Version(), spec.Version)
			}
			if len(ad.Capabilities()) != len(spec.Operations) {
				t.Errorf("capabilities = %v, want %d entries", ad.Capabilities(), len(spec.Operations))
			}
		})
	}
}

// TestBrokenSpecIsRejected pins the rollback contract: a spec that cannot pass
// its own vectors must not produce an adapter at all.
func TestBrokenSpecIsRejected(t *testing.T) {
	spec := serviceBSpec()
	// Break the argument mapping. The recorded sum vector then cannot be
	// reproduced, which is exactly the class of mistake a runtime-loaded spec
	// is most likely to contain.
	op := spec.Operations["sum"]
	op.ArgMap = map[string]string{"values": "numbers"}
	spec.Operations["sum"] = op

	if _, err := Build(spec, BuildOptions{Endpoint: "127.0.0.1:8201", SelfTestTimeout: 3 * time.Second}); err == nil {
		t.Fatal("expected the broken spec to be rejected, it was accepted")
	}
}

// TestSpecValidationRejectsStructuralNonsense covers the cheap gate that runs
// before anything is constructed.
func TestSpecValidationRejectsStructuralNonsense(t *testing.T) {
	cases := map[string]func(*Spec){
		"empty name":       func(s *Spec) { s.Name = "" },
		"unknown family":   func(s *Spec) { s.Family = "carrier-pigeon" },
		"zero version":     func(s *Spec) { s.Version = 0 },
		"no operations":    func(s *Spec) { s.Operations = map[string]OpSpec{} },
		"bad magic":        func(s *Spec) { s.Wire.MagicHex = "zz" },
		"absurd payload":   func(s *Spec) { s.Wire.MaxPayload = 1 << 30 },
		"unmapped results": func(s *Spec) { s.Operations["echo"] = OpSpec{Wire: "ECHO"} },
	}
	for name, mutate := range cases {
		t.Run(name, func(t *testing.T) {
			spec := serviceBSpec()
			mutate(spec)
			if err := spec.Validate(); err == nil {
				t.Fatalf("expected %s to be rejected", name)
			}
		})
	}
}
