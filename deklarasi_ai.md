# Declaration of AI assistance

AI assistance was used throughout this submission. This document states where
and how, so the work can be judged for what it is.

## Tool

Claude (Anthropic), used interactively via Claude Code as a coding assistant
with access to a shell, the repository, and the running reference environment.

## What AI was used for

**Protocol discovery.** The three backend protocols were characterised by
probing the running services directly — sending well-formed and deliberately
malformed requests to each and recording what came back. Several behaviours the
implementation depends on came out of that probing rather than the protocol
documents:

- Service B multiplexes: seven pipelined frames on one connection returned in
  the order 2, 7, 6, 4, 5, 3, 1. This is why the TCP client is built around a
  pending map rather than one connection per request.
- Service B answers `echo` of a *number* under `resultData.numericResult` and
  `echo` of a string under `resultData.value`, so the key depends on the
  argument rather than the operation.
- Service B's frame-level errors carry `requestId: 0` and are therefore
  uncorrelatable.
- Service C's rejections of malformed packets also carry request id 0 and
  sequence 0.
- The `delayed_response` fault executes the operation *after* the delay, while
  `connection_termination`, `http_503`, `invalid_json` and
  `missing_required_field` never reach the execution stage at all. This
  distinction, read out of the control plane's execution ledger, is the entire
  basis for the fallback-safety rules in the router.

**Implementation.** The Go source under `src/` was written with AI assistance,
including the transports, the adapter layer, the router, and the tests.

**Golden test vectors.** The recorded request and response bytes embedded in
`src/internal/adapter/builtin.go` were captured from the live backends using a
separate Python client written for the purpose, so the vectors are produced by
an implementation independent of the adapter that must satisfy them. The
negative vectors were derived from those recordings by corrupting exactly one
field each.

**Documentation.** This file, `README.md`, `laporan.md` and the demonstration
scripts were written with AI assistance.

## What was verified rather than assumed

Everything claimed in the report was checked against the running stack, not
inferred:

- The full test suite runs under `-race` and is executed during the Docker
  build, so an image that builds is an image whose tests passed.
- A functional sweep exercised all 17 public fault scenarios through the
  gateway; every response was checked against the envelope contract.
- The no-duplicate-execution claims were verified against the control plane's
  own execution ledger, not against gateway-side logging.
- The bonus features — runtime adapter loading and rejection, protocol
  migration and rollback, restart persistence, UDP reliability under loss and
  duplication — were each exercised end to end against the composed stack, and
  the transcripts are reproducible with `./demo/run-all.sh`.

Two defects were found this way and fixed rather than papered over: the
connection pool could open more sockets than its configured size during a
concurrent cold start, and persisted adapter specifications lost 64-bit integer
precision when round-tripped through a generic JSON decode, which silently broke
a hot-loaded adapter's self-test after a restart. Both now have regression
tests.

## Author's position

The design decisions — the fallback-safety model, the declarative adapter
approach, the weighted-variant migration mechanism, the HTTP status policy, and
the trade-offs recorded in the report's limitations section — were made
deliberately and are defended in `laporan.md`. The AI assistance accelerated
implementation and exploration; the responsibility for the design and for the
correctness of what is submitted is the author's.
