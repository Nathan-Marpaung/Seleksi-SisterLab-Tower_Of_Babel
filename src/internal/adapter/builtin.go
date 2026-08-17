package adapter

// Built-in adapter specifications.
//
// These are ordinary specs, loaded through the same path as anything that
// arrives over the admin API. Keeping the shipped adapters honest about that is
// the point: if the declarative path could not express the three real backends,
// "runtime adapter loading" would be a demo feature rather than the mechanism
// the gateway actually runs on.
//
// The self-test vectors below were recorded against the live reference services
// with an independent client, and the negative vectors were derived from those
// recordings by corrupting exactly one field each. An adapter therefore has to
// reproduce byte sequences it did not generate, and reject corruptions it did
// not invent, before it is allowed to carry traffic.

// BuiltinNames are the adapters seeded on first boot.
const (
	BuiltinHTTPv1 = "http-json-v1"
	BuiltinTCPv1  = "tcp-frame-json-v1"
	BuiltinUDPv1  = "udp-crc-json-v1"
)

// selfTestCorrelation is the correlation id the recorded vectors were captured
// under. It is fixed so the recordings stay reproducible.
const selfTestCorrelation uint64 = 0x1122334455667788

// BuiltinSpecs returns fresh copies of the shipped adapter specs.
func BuiltinSpecs() []*Spec {
	return []*Spec{serviceASpec(), serviceBSpec(), serviceCSpec()}
}

func serviceASpec() *Spec {
	return &Spec{
		Name:      BuiltinHTTPv1,
		Family:    FamilyHTTPJSON,
		ServiceID: "service-a",
		Version:   1,
		Wire: Wire{
			ExecutePath:     "/v1/execute",
			HealthPath:      "/v1/health",
			Method:          "POST",
			Headers:         map[string]string{"Content-Type": "application/json", "Accept": "application/json"},
			VersionHeader:   "X-Protocol-Version",
			RequestIDHeader: "X-Request-ID",

			OperationField: "operation_name",
			ArgumentsField: "operation_arguments",

			CorrelationField: "request_id",
			ResultField:      "operation_result",
			// Service A reports errors flat at the top level rather than in a
			// nested object, so ErrorField stays empty and the presence of
			// error_code is the signal.
			ErrorCodeField:      "error_code",
			ErrorMessageField:   "error_message",
			ErrorRetryableField: "retryable",
			RetryableCodes:      []string{"UNAVAILABLE", "RATE_LIMITED", "INTERNAL_ERROR"},
		},
		Operations: map[string]OpSpec{
			"echo":      {Wire: "echo", ResultKeys: []string{"value"}},
			"uppercase": {Wire: "uppercase", ResultKeys: []string{"value"}},
			"metadata":  {Wire: "metadata", Passthrough: true},
		},
		SelfTest: []Vector{
			{
				Name: "echo round trip", Operation: "echo",
				Arguments: map[string]any{"value": "babel"}, CorrelationID: selfTestCorrelation,
				ExpectRequestHex: "7b226f7065726174696f6e5f617267756d656e7473223a7b2276616c7565223a22626162656c227d2c226f7065726174696f6e5f6e616d65223a226563686f227d",
				ResponseHex:      "7b226f7065726174696f6e5f6e616d65223a226563686f222c226f7065726174696f6e5f726573756c74223a7b2276616c7565223a22626162656c227d2c22726571756573745f6964223a2231323334363035363136343336353038353532222c22736572766963655f6e616d65223a22736572766963652d61227d0a",
				ExpectResult:     map[string]any{"value": "babel"},
			},
			{
				Name: "uppercase round trip", Operation: "uppercase",
				Arguments: map[string]any{"value": "babel"}, CorrelationID: selfTestCorrelation,
				ExpectRequestHex: "7b226f7065726174696f6e5f617267756d656e7473223a7b2276616c7565223a22626162656c227d2c226f7065726174696f6e5f6e616d65223a22757070657263617365227d",
				ResponseHex:      "7b226f7065726174696f6e5f6e616d65223a22757070657263617365222c226f7065726174696f6e5f726573756c74223a7b2276616c7565223a22424142454c227d2c22726571756573745f6964223a2231323334363035363136343336353038353532222c22736572766963655f6e616d65223a22736572766963652d61227d0a",
				ExpectResult:     map[string]any{"value": "BABEL"},
			},
			{
				// metadata passes through unwrapped, and none of Service A's
				// own envelope fields may leak into it.
				Name: "metadata passthrough", Operation: "metadata",
				Arguments: map[string]any{}, CorrelationID: selfTestCorrelation,
				ExpectRequestHex: "7b226f7065726174696f6e5f617267756d656e7473223a7b7d2c226f7065726174696f6e5f6e616d65223a226d65746164617461227d",
				ResponseHex:      "7b226f7065726174696f6e5f6e616d65223a226d65746164617461222c226f7065726174696f6e5f726573756c74223a7b226361706162696c6974696573223a5b226563686f222c226d65746164617461222c22757070657263617365225d2c2270726f746f636f6c5f76657273696f6e223a312c22736572766963655f6964223a22736572766963652d61227d2c22726571756573745f6964223a2231323334363035363136343336353038353532222c22736572766963655f6e616d65223a22736572766963652d61227d0a",
				ExpectResult: map[string]any{
					"capabilities": []any{"echo", "metadata", "uppercase"}, "protocol_version": 1, "service_id": "service-a",
				},
			},
			{
				Name: "backend error is forwarded verbatim", Operation: "echo",
				CorrelationID:   selfTestCorrelation,
				ResponseHex:     "7b226572726f725f636f6465223a224f5045524154494f4e5f4e4f545f535550504f52544544222c226572726f725f6d657373616765223a22736572766963652d6120646f6573206e6f7420737570706f72742073756d222c22726571756573745f6964223a2231323334363035363136343336353038353532222c22726574727961626c65223a66616c73657d0a",
				ExpectErrorCode: "OPERATION_NOT_SUPPORTED",
			},
			{
				Name: "truncated json is rejected", Operation: "echo",
				CorrelationID:   selfTestCorrelation,
				ResponseHex:     "7b226f7065726174696f6e5f6e616d65223a226563686f222c226f7065726174696f6e5f726573756c74223a",
				ExpectErrorCode: "BACKEND_PROTOCOL_VIOLATION",
			},
			{
				Name: "missing result field is rejected", Operation: "echo",
				CorrelationID:   selfTestCorrelation,
				ResponseHex:     "7b226f7065726174696f6e5f6e616d65223a226563686f222c22726571756573745f6964223a2231323334363035363136343336353038353532222c22736572766963655f6e616d65223a22736572766963652d61227d",
				ExpectErrorCode: "BACKEND_PROTOCOL_VIOLATION",
			},
			{
				Name: "mismatched request id is rejected", Operation: "echo",
				CorrelationID:   selfTestCorrelation,
				ResponseHex:     "7b226f7065726174696f6e5f6e616d65223a226563686f222c226f7065726174696f6e5f726573756c74223a7b2276616c7565223a22626162656c227d2c22726571756573745f6964223a22393939222c22736572766963655f6e616d65223a22736572766963652d61227d",
				ExpectErrorCode: "BACKEND_CORRELATION_MISMATCH",
			},
		},
	}
}

func serviceBSpec() *Spec {
	return &Spec{
		Name:      BuiltinTCPv1,
		Family:    FamilyTCPFrameJSON,
		ServiceID: "service-b",
		Version:   1,
		Wire: Wire{
			MagicHex:   "babe",
			MaxPayload: 65536,

			OperationField: "opCode",
			ArgumentsField: "args",

			CorrelationField: "requestId",
			ResultField:      "resultData",
			ErrorField:       "errorData",

			ErrorCodeField:      "code",
			ErrorMessageField:   "message",
			ErrorRetryableField: "retryable",
		},
		Operations: map[string]OpSpec{
			// Service B answers string operations under `value` and numeric
			// ones under `numericResult`, and which one it picks depends on the
			// argument rather than the operation -- echoing a number comes back
			// as numericResult. Both keys are therefore listed for every
			// scalar operation.
			"echo":      {Wire: "ECHO", ResultKeys: []string{"value", "numericResult"}},
			"uppercase": {Wire: "UPPERCASE", ResultKeys: []string{"value", "numericResult"}},
			"reverse":   {Wire: "REVERSE", ResultKeys: []string{"value", "numericResult"}},
			"sum": {
				Wire:       "SUM",
				ArgMap:     map[string]string{"values": "numberList"},
				ResultKeys: []string{"numericResult", "value"},
			},
			"metadata": {Wire: "METADATA", Passthrough: true},
		},
		SelfTest: []Vector{
			{
				Name: "echo round trip", Operation: "echo",
				Arguments: map[string]any{"value": "babel"}, CorrelationID: selfTestCorrelation,
				ExpectRequestHex: "babe01000000002a11223344556677887b2261726773223a7b2276616c7565223a22626162656c227d2c226f70436f6465223a224543484f227d",
				ResponseHex:      "babe01000000006911223344556677887b226572726f7244617461223a6e756c6c2c22726571756573744964223a313233343630353631363433363530383535322c22726573756c7444617461223a7b2276616c7565223a22626162656c227d2c22736572766963654964223a22736572766963652d62227d",
				ExpectResult:     map[string]any{"value": "babel"},
			},
			{
				// Pins the argument rename values -> numberList and the
				// numericResult unwrapping in one vector.
				Name: "sum renames its argument list", Operation: "sum",
				Arguments: map[string]any{"values": []any{1, 2, 3}}, CorrelationID: selfTestCorrelation,
				ExpectRequestHex: "babe01000000002e11223344556677887b2261726773223a7b226e756d6265724c697374223a5b312c322c335d7d2c226f70436f6465223a2253554d227d",
				ResponseHex:      "babe01000000006b11223344556677887b226572726f7244617461223a6e756c6c2c22726571756573744964223a313233343630353631363433363530383535322c22726573756c7444617461223a7b226e756d65726963526573756c74223a367d2c22736572766963654964223a22736572766963652d62227d",
				ExpectResult:     map[string]any{"value": 6},
			},
			{
				Name: "reverse round trip", Operation: "reverse",
				Arguments: map[string]any{"value": "babel"}, CorrelationID: selfTestCorrelation,
				ExpectRequestHex: "babe01000000002d11223344556677887b2261726773223a7b2276616c7565223a22626162656c227d2c226f70436f6465223a2252455645525345227d",
				ResponseHex:      "babe01000000006911223344556677887b226572726f7244617461223a6e756c6c2c22726571756573744964223a313233343630353631363433363530383535322c22726573756c7444617461223a7b2276616c7565223a226c65626162227d2c22736572766963654964223a22736572766963652d62227d",
				ExpectResult:     map[string]any{"value": "lebab"},
			},
			{
				// Echoing a number comes back under a different key than
				// echoing a string; the normalized envelope must not change.
				Name: "numeric echo normalizes to value", Operation: "echo",
				Arguments: map[string]any{"value": 42}, CorrelationID: selfTestCorrelation,
				ExpectRequestHex: "babe01000000002511223344556677887b2261726773223a7b2276616c7565223a34327d2c226f70436f6465223a224543484f227d",
				ResponseHex:      "babe01000000006c11223344556677887b226572726f7244617461223a6e756c6c2c22726571756573744964223a313233343630353631363433363530383535322c22726573756c7444617461223a7b226e756d65726963526573756c74223a34327d2c22736572766963654964223a22736572766963652d62227d",
				ExpectResult:     map[string]any{"value": 42},
			},
			{
				Name: "metadata passthrough", Operation: "metadata",
				Arguments: map[string]any{}, CorrelationID: selfTestCorrelation,
				ExpectRequestHex: "babe01000000001f11223344556677887b2261726773223a7b7d2c226f70436f6465223a224d45544144415441227d",
				ResponseHex:      "babe0100000000c611223344556677887b226572726f7244617461223a6e756c6c2c22726571756573744964223a313233343630353631363433363530383535322c22726573756c7444617461223a7b226361706162696c6974696573223a5b226563686f222c226d65746164617461222c2272657665727365222c2273756d222c22757070657263617365225d2c2270726f746f636f6c5f76657273696f6e223a312c22736572766963655f6964223a22736572766963652d62227d2c22736572766963654964223a22736572766963652d62227d",
				ExpectResult: map[string]any{
					"capabilities":     []any{"echo", "metadata", "reverse", "sum", "uppercase"},
					"protocol_version": 1, "service_id": "service-b",
				},
			},
			{
				Name: "backend error is forwarded verbatim", Operation: "echo",
				CorrelationID:   selfTestCorrelation,
				ResponseHex:     "babe0100000000ba11223344556677887b226572726f7244617461223a7b22636f6465223a224f5045524154494f4e5f4e4f545f535550504f52544544222c226d657373616765223a22736572766963652d6220646f6573206e6f7420737570706f7274206e6f73756368222c22726574727961626c65223a66616c73657d2c22726571756573744964223a313233343630353631363433363530383535322c22726573756c7444617461223a6e756c6c2c22736572766963654964223a22736572766963652d62227d",
				ExpectErrorCode: "OPERATION_NOT_SUPPORTED",
			},
			{
				Name: "invalid magic is rejected", Operation: "echo",
				CorrelationID:   selfTestCorrelation,
				ResponseHex:     "bbbe01000000006911223344556677887b226572726f7244617461223a6e756c6c2c22726571756573744964223a313233343630353631363433363530383535322c22726573756c7444617461223a7b2276616c7565223a22626162656c227d2c22736572766963654964223a22736572766963652d62227d",
				ExpectErrorCode: "BACKEND_PROTOCOL_VIOLATION",
			},
			{
				Name: "unexpected protocol version is rejected", Operation: "echo",
				CorrelationID:   selfTestCorrelation,
				ResponseHex:     "babe02000000006911223344556677887b226572726f7244617461223a6e756c6c2c22726571756573744964223a313233343630353631363433363530383535322c22726573756c7444617461223a7b2276616c7565223a22626162656c227d2c22736572766963654964223a22736572766963652d62227d",
				ExpectErrorCode: "UNSUPPORTED_PROTOCOL_VERSION",
			},
			{
				Name: "declared length must match the frame", Operation: "echo",
				CorrelationID:   selfTestCorrelation,
				ResponseHex:     "babe01000000003011223344556677887b226572726f7244617461223a6e756c6c2c22726571756573744964223a313233343630353631363433363530383535322c22726573756c7444617461223a7b2276616c7565223a22626162656c227d2c22736572766963654964223a22736572766963652d62227d",
				ExpectErrorCode: "BACKEND_PROTOCOL_VIOLATION",
			},
			{
				// The frame header correlates but the payload does not. Both
				// layers are checked, because the fault catalogue can corrupt
				// either one.
				Name: "payload request id must match too", Operation: "echo",
				CorrelationID:   selfTestCorrelation,
				ResponseHex:     "babe01000000005911223344556677887b226572726f7244617461223a6e756c6c2c22726571756573744964223a3939392c22726573756c7444617461223a7b2276616c7565223a22626162656c227d2c22736572766963654964223a22736572766963652d62227d",
				ExpectErrorCode: "BACKEND_CORRELATION_MISMATCH",
			},
		},
	}
}

func serviceCSpec() *Spec {
	return &Spec{
		Name:      BuiltinUDPv1,
		Family:    FamilyUDPCRCJSON,
		ServiceID: "service-c",
		Version:   1,
		Wire: Wire{
			MagicHex:   "c0de",
			MaxPayload: 4096,
			Checksum:   "crc32-ieee",

			// The operation travels in the header opcode, and the payload is
			// the argument object itself, so neither field name is set.
			ResultField: "result",
			ErrorField:  "error",

			ErrorCodeField:      "code",
			ErrorMessageField:   "message",
			ErrorRetryableField: "retryable",
		},
		Operations: map[string]OpSpec{
			"echo":     {OpCode: 1, ResultKeys: []string{"value"}},
			"sum":      {OpCode: 2, ResultKeys: []string{"value"}},
			"metadata": {OpCode: 3, Passthrough: true},
		},
		SelfTest: []Vector{
			{
				Name: "echo round trip", Operation: "echo",
				Arguments: map[string]any{"value": "babel"}, CorrelationID: selfTestCorrelation, Seq: 7,
				ExpectRequestHex: "c0de0101000000071122334455667788010000117b2276616c7565223a22626162656c227d2e32d28d",
				ResponseHex:      "c0de0102000000071122334455667788010000417b226572726f72223a6e756c6c2c22726573756c74223a7b2276616c7565223a22626162656c227d2c22736572766963654964223a22736572766963652d63227dace6912d",
				ExpectResult:     map[string]any{"value": "babel"},
			},
			{
				Name: "sum round trip", Operation: "sum",
				Arguments: map[string]any{"values": []any{1, 2, 3}}, CorrelationID: selfTestCorrelation, Seq: 7,
				ExpectRequestHex: "c0de0101000000071122334455667788020000127b2276616c756573223a5b312c322c335d7d7aac4bc5",
				ResponseHex:      "c0de01020000000711223344556677880200003b7b226572726f72223a6e756c6c2c22726573756c74223a7b2276616c7565223a367d2c22736572766963654964223a22736572766963652d63227dcc5b6a5c",
				ExpectResult:     map[string]any{"value": 6},
			},
			{
				Name: "metadata passthrough", Operation: "metadata",
				Arguments: map[string]any{}, CorrelationID: selfTestCorrelation, Seq: 7,
				ExpectRequestHex: "c0de0101000000071122334455667788030000027b7d7d89869d",
				ResponseHex:      "c0de0102000000071122334455667788030000887b226572726f72223a6e756c6c2c22726573756c74223a7b226361706162696c6974696573223a5b226563686f222c226d65746164617461222c2273756d225d2c2270726f746f636f6c5f76657273696f6e223a312c22736572766963655f6964223a22736572766963652d63227d2c22736572766963654964223a22736572766963652d63227d850e3461",
				ExpectResult: map[string]any{
					"capabilities": []any{"echo", "metadata", "sum"}, "protocol_version": 1, "service_id": "service-c",
				},
			},
			{
				Name: "backend error is forwarded verbatim", Operation: "echo",
				CorrelationID:   selfTestCorrelation,
				ResponseHex:     "c0de01030000000711223344556677880000008c7b226572726f72223a7b22636f6465223a224f5045524154494f4e5f4e4f545f535550504f52544544222c226d657373616765223a22736572766963652d6320646f6573206e6f7420737570706f727420222c22726574727961626c65223a66616c73657d2c22726573756c74223a6e756c6c2c22736572766963654964223a22736572766963652d63227df5a742ae",
				ExpectErrorCode: "OPERATION_NOT_SUPPORTED",
			},
			{
				// One flipped bit in the trailing checksum. Nothing else in the
				// datagram changed, so only integrity validation can catch it.
				Name: "corrupt checksum is rejected", Operation: "echo",
				CorrelationID:   selfTestCorrelation,
				ResponseHex:     "c0de0102000000071122334455667788010000417b226572726f72223a6e756c6c2c22726573756c74223a7b2276616c7565223a22626162656c227d2c22736572766963654964223a22736572766963652d63227dace691d2",
				ExpectErrorCode: "BACKEND_CHECKSUM_MISMATCH",
			},
			{
				// Version bumped and the checksum recomputed, so the datagram
				// is internally consistent and only the version check can
				// reject it.
				Name: "unexpected protocol version is rejected", Operation: "echo",
				CorrelationID:   selfTestCorrelation,
				ResponseHex:     "c0de0202000000071122334455667788010000417b226572726f72223a6e756c6c2c22726573756c74223a7b2276616c7565223a22626162656c227d2c22736572766963654964223a22736572766963652d63227da666ec2f",
				ExpectErrorCode: "UNSUPPORTED_PROTOCOL_VERSION",
			},
			{
				Name: "mismatched request id is rejected", Operation: "echo",
				CorrelationID:   selfTestCorrelation,
				ResponseHex:     "c0de01020000000700000000000003e7010000417b226572726f72223a6e756c6c2c22726573756c74223a7b2276616c7565223a22626162656c227d2c22736572766963654964223a22736572766963652d63227d275c6a72",
				ExpectErrorCode: "BACKEND_CORRELATION_MISMATCH",
			},
		},
	}
}
