package adapter

import (
	"encoding/json"
	"fmt"
	"strings"

	"babel/gateway/internal/gwerr"
)

// buildArguments maps logical argument keys onto the backend's names.
//
// Unlisted keys pass through unchanged. That is deliberate: the gateway is a
// translator, not a filter, and silently dropping an argument it did not
// recognise would turn a caller's mistake into a wrong answer instead of a
// backend error.
func buildArguments(op OpSpec, args map[string]any) map[string]any {
	out := make(map[string]any, len(args))
	for k, v := range args {
		if mapped, ok := op.ArgMap[k]; ok && mapped != "" {
			out[mapped] = v
			continue
		}
		out[k] = v
	}
	return out
}

// decodePayload is the shared response validator and normalizer.
//
// It is the choke point where "validasi protokol" happens for every family: a
// payload is parsed, its correlation identifier checked, its error object
// recognised, and its result rewritten into Gateway API shape. Anything that
// does not fit is a classified protocol violation, never a panic and never a
// half-translated result forwarded to the client.
//
// wantCorrelation is the value the payload's correlation field must carry. A
// nil value disables the payload-level check, which is correct for families
// where correlation lives in the frame header instead.
func decodePayload(spec *Spec, op OpSpec, raw []byte, wantCorrelation any) (map[string]any, *gwerr.Error) {
	svc := spec.ServiceID

	var payload map[string]any
	dec := json.NewDecoder(strings.NewReader(string(raw)))
	dec.UseNumber() // preserve integer precision; sum results must not become 1e+06
	if err := dec.Decode(&payload); err != nil {
		return nil, gwerr.ProtocolViolation(svc, "payload is not valid JSON").Wrap(err)
	}
	if payload == nil {
		return nil, gwerr.ProtocolViolation(svc, "payload is JSON null")
	}

	if wantCorrelation != nil && spec.Wire.CorrelationField != "" {
		got, present := payload[spec.Wire.CorrelationField]
		if !present {
			return nil, gwerr.Correlation(svc, fmt.Sprintf("payload has no %s field", spec.Wire.CorrelationField))
		}
		if !sameCorrelation(got, wantCorrelation) {
			return nil, gwerr.Correlation(svc, fmt.Sprintf("expected %v, payload carried %v", wantCorrelation, got))
		}
	}

	if e := extractError(spec, payload); e != nil {
		return nil, e
	}

	if spec.Wire.ResultField == "" {
		return nil, gwerr.Internal(fmt.Errorf("adapter %s has no result_field configured", spec.Name))
	}
	result, present := payload[spec.Wire.ResultField]
	if !present || result == nil {
		// A response that is neither a recognised error nor a result is a
		// contract violation by the backend, which is exactly what the
		// missing-required-field fault produces.
		return nil, gwerr.ProtocolViolation(svc,
			fmt.Sprintf("response has neither an error nor a %s field", spec.Wire.ResultField))
	}

	return normalizeResult(svc, op, result)
}

// normalizeResult rewrites a backend result into Gateway API shape.
//
//   - A pass-through operation (metadata) keeps its object as-is.
//   - Otherwise the scalar answer is looked up under the spec's result keys and
//     re-emitted as {"value": ...}. Key *presence* is what counts, so a
//     legitimate {"value": null} survives instead of being mistaken for absence.
//   - A backend that answers with a bare scalar is accepted and wrapped, since
//     the information is unambiguous.
func normalizeResult(serviceID string, op OpSpec, result any) (map[string]any, *gwerr.Error) {
	obj, isObject := result.(map[string]any)

	if op.Passthrough {
		if !isObject {
			return nil, gwerr.ProtocolViolation(serviceID, "expected an object result")
		}
		return obj, nil
	}

	if !isObject {
		return map[string]any{"value": result}, nil
	}
	for _, key := range op.ResultKeys {
		if v, ok := obj[key]; ok {
			return map[string]any{"value": v}, nil
		}
	}
	return nil, gwerr.ProtocolViolation(serviceID,
		fmt.Sprintf("result object carries none of %v", op.ResultKeys))
}

// extractError recognises a backend-reported domain error.
//
// Backend error codes are forwarded verbatim rather than remapped: the backend
// knows more about why it refused than any gateway-side substitute would, and
// the Gateway API contract only requires the envelope shape, not a fixed code
// vocabulary.
//
// Every such error is marked fallback-safe. A backend that answers with a
// structured error did not complete the operation -- the reference
// environment's execution ledger confirms this for injected 503s and refused
// operations -- so re-issuing the work elsewhere cannot duplicate it.
func extractError(spec *Spec, payload map[string]any) *gwerr.Error {
	w := spec.Wire
	svc := spec.ServiceID

	fields := payload
	if w.ErrorField != "" {
		raw, ok := payload[w.ErrorField]
		if !ok || raw == nil {
			return nil
		}
		obj, ok := raw.(map[string]any)
		if !ok {
			return gwerr.ProtocolViolation(svc, fmt.Sprintf("%s is present but not an object", w.ErrorField))
		}
		fields = obj
	} else {
		// Flat error shape: the presence of the code field is the signal.
		if w.ErrorCodeField == "" {
			return nil
		}
		if v, ok := payload[w.ErrorCodeField]; !ok || v == nil {
			return nil
		}
	}

	code := stringField(fields, w.ErrorCodeField)
	if code == "" {
		code = gwerr.CodeBackendError
	}
	message := stringField(fields, w.ErrorMessageField)
	if message == "" {
		message = "Backend reported an error."
	}

	retryable := boolField(fields, w.ErrorRetryableField)
	for _, rc := range w.RetryableCodes {
		if strings.EqualFold(rc, code) {
			retryable = true
			break
		}
	}

	return &gwerr.Error{
		Code:      code,
		Message:   message,
		Retryable: retryable,
		// See the doc comment: a structured refusal means nothing was executed.
		FallbackSafe: true,
		ServiceID:    svc,
	}
}

func stringField(m map[string]any, key string) string {
	if key == "" {
		return ""
	}
	if v, ok := m[key]; ok {
		if s, ok := v.(string); ok {
			return s
		}
	}
	return ""
}

func boolField(m map[string]any, key string) bool {
	if key == "" {
		return false
	}
	if v, ok := m[key]; ok {
		if b, ok := v.(bool); ok {
			return b
		}
	}
	return false
}

// sameCorrelation compares identifiers across the type differences JSON
// introduces. Service B echoes its request id as a JSON number, Service A as a
// string; both must compare equal to the uint64 the gateway allocated.
func sameCorrelation(got, want any) bool {
	return correlationString(got) == correlationString(want)
}

func correlationString(v any) string {
	switch t := v.(type) {
	case nil:
		return ""
	case string:
		return t
	case json.Number:
		return t.String()
	case uint64:
		return fmt.Sprintf("%d", t)
	case int64:
		return fmt.Sprintf("%d", t)
	case float64:
		// Formatted without exponent so that 1.234e+18 style rendering cannot
		// make two equal identifiers look different.
		return fmt.Sprintf("%.0f", t)
	default:
		return fmt.Sprintf("%v", t)
	}
}
