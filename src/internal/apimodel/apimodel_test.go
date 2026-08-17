package apimodel

import (
	"encoding/json"
	"testing"

	"babel/gateway/internal/gwerr"
)

func parse(t *testing.T, body string) (*ExecuteRequest, map[string]json.RawMessage) {
	t.Helper()
	var raw map[string]json.RawMessage
	if err := json.Unmarshal([]byte(body), &raw); err != nil {
		t.Fatalf("test body is not JSON: %v", err)
	}
	var req ExecuteRequest
	if err := json.Unmarshal([]byte(body), &req); err != nil {
		t.Fatalf("decode: %v", err)
	}
	return &req, raw
}

func TestValidateEnvelope(t *testing.T) {
	valid := `{"request_id":"r1","operation":"echo","arguments":{"value":"x"},"options":{}}`
	if req, raw := parse(t, valid); ValidateEnvelope(req, raw) != nil {
		t.Fatalf("a valid body was rejected: %v", ValidateEnvelope(req, raw))
	}

	bad := map[string]string{
		"missing request_id": `{"operation":"echo","arguments":{},"options":{}}`,
		"empty request_id":   `{"request_id":"","operation":"echo","arguments":{},"options":{}}`,
		"missing operation":  `{"request_id":"r","arguments":{},"options":{}}`,
		"empty operation":    `{"request_id":"r","operation":"","arguments":{},"options":{}}`,
		"arguments is array": `{"request_id":"r","operation":"echo","arguments":[],"options":{}}`,
		"options is string":  `{"request_id":"r","operation":"echo","arguments":{},"options":"x"}`,
	}
	for name, body := range bad {
		t.Run(name, func(t *testing.T) {
			var raw map[string]json.RawMessage
			_ = json.Unmarshal([]byte(body), &raw)
			var req ExecuteRequest
			// A type mismatch may fail strict decoding, which is itself a
			// rejection; only assert further when decoding succeeded.
			if err := json.Unmarshal([]byte(body), &req); err != nil {
				return
			}
			if e := ValidateEnvelope(&req, raw); e == nil {
				t.Fatalf("%s was accepted", name)
			} else if e.Code != gwerr.CodeInvalidRequest {
				t.Errorf("code = %s, want INVALID_REQUEST", e.Code)
			}
		})
	}

	// An explicit null for arguments or options is treated as empty rather than
	// refused: rejecting it would break well-behaved callers for no benefit.
	t.Run("explicit nulls are accepted", func(t *testing.T) {
		req, raw := parse(t, `{"request_id":"r","operation":"echo","arguments":null,"options":null}`)
		if e := ValidateEnvelope(req, raw); e != nil {
			t.Fatalf("explicit nulls were rejected: %v", e)
		}
	})
}

func TestParseOptionsNeverFails(t *testing.T) {
	cases := []struct {
		name      string
		raw       map[string]any
		preferred string
		timeout   int
		hasTO     bool
	}{
		{"empty", map[string]any{}, "", 0, false},
		{"nil", nil, "", 0, false},
		{"both set", map[string]any{"preferred_service": "service-b", "timeout_ms": float64(1500)}, "service-b", 1500, true},
		// The reference CLI sends a null preference when the flag is unset.
		{"null preference", map[string]any{"preferred_service": nil}, "", 0, false},
		// Unusable values for known keys are treated as absent, and unknown
		// keys are ignored entirely -- the contract forbids failing over either.
		{"garbage timeout", map[string]any{"timeout_ms": "soon"}, "", 0, false},
		{"negative timeout", map[string]any{"timeout_ms": float64(-5)}, "", 0, false},
		{"unknown keys", map[string]any{"turbo": true, "retries": float64(9)}, "", 0, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := ParseOptions(tc.raw)
			if got.PreferredService != tc.preferred || got.TimeoutMS != tc.timeout || got.HasTimeout != tc.hasTO {
				t.Fatalf("got %+v, want preferred=%q timeout=%d has=%v",
					got, tc.preferred, tc.timeout, tc.hasTO)
			}
		})
	}
}

func TestValidateArguments(t *testing.T) {
	good := []struct {
		op   string
		args map[string]any
	}{
		{"echo", map[string]any{"value": "x"}},
		{"echo", map[string]any{"value": float64(42)}},
		{"echo", map[string]any{}},
		{"uppercase", map[string]any{"value": "x"}},
		{"reverse", map[string]any{"value": ""}},
		{"sum", map[string]any{"values": []any{float64(1), float64(-2), float64(3.5)}}},
		{"sum", map[string]any{"values": []any{}}},
		{"metadata", map[string]any{}},
	}
	for _, tc := range good {
		if e := ValidateArguments(tc.op, tc.args); e != nil {
			t.Errorf("%s%v was rejected: %s", tc.op, tc.args, e.Message)
		}
	}

	bad := []struct {
		name string
		op   string
		args map[string]any
	}{
		{"uppercase without value", "uppercase", map[string]any{}},
		{"uppercase with a number", "uppercase", map[string]any{"value": float64(1)}},
		{"reverse with an object", "reverse", map[string]any{"value": map[string]any{}}},
		{"sum without values", "sum", map[string]any{}},
		{"sum with a string list", "sum", map[string]any{"values": "1,2"}},
		{"sum with a string element", "sum", map[string]any{"values": []any{"1"}}},
		{"sum above int32", "sum", map[string]any{"values": []any{float64(1 << 40)}}},
		{"sum below int32", "sum", map[string]any{"values": []any{float64(-(1 << 40))}}},
	}
	for _, tc := range bad {
		t.Run(tc.name, func(t *testing.T) {
			e := ValidateArguments(tc.op, tc.args)
			if e == nil {
				t.Fatal("accepted")
			}
			if e.Code != gwerr.CodeInvalidArgs {
				t.Errorf("code = %s, want INVALID_ARGUMENT", e.Code)
			}
			if e.Retryable {
				t.Error("an argument error must not be retryable: retrying cannot change it")
			}
		})
	}
}

// TestEnvelopeInvariantsAreStructural: the six keys are always present, and the
// success/error shapes are mutually exclusive.
func TestEnvelopeInvariantsAreStructural(t *testing.T) {
	t.Run("success", func(t *testing.T) {
		raw, _ := json.Marshal(Success("r", "echo", "service-a", map[string]any{"value": 1}))
		var m map[string]json.RawMessage
		if err := json.Unmarshal(raw, &m); err != nil {
			t.Fatalf("unmarshal: %v", err)
		}
		for _, key := range []string{"request_id", "status", "service_id", "operation", "result", "error"} {
			if _, ok := m[key]; !ok {
				t.Errorf("key %q is missing from the envelope", key)
			}
		}
		if string(m["error"]) != "null" {
			t.Errorf("error = %s, want null on success", m["error"])
		}
	})

	t.Run("nil result is coerced", func(t *testing.T) {
		// A success with a null result would violate the contract, so it is
		// repaired rather than emitted.
		resp := Success("r", "echo", "service-a", nil)
		if resp.Result == nil {
			t.Fatal("a nil result survived into a success envelope")
		}
	})

	t.Run("failure", func(t *testing.T) {
		resp := Failure("r", "echo", "", gwerr.New(gwerr.CodeNoRoute, "nothing available", true, true))
		if resp.Result != nil {
			t.Error("an error envelope must carry a null result")
		}
		if resp.ServiceID != nil {
			t.Error("service_id must be null when no backend was resolved")
		}
		if resp.Error.Code != gwerr.CodeNoRoute || !resp.Error.Retryable {
			t.Errorf("error = %+v", resp.Error)
		}
	})

	t.Run("failure names its backend when there is one", func(t *testing.T) {
		resp := Failure("r", "echo", "service-b", gwerr.Timeout("service-b"))
		if resp.ServiceID == nil || *resp.ServiceID != "service-b" {
			t.Errorf("service_id = %v, want service-b", resp.ServiceID)
		}
	})
}
