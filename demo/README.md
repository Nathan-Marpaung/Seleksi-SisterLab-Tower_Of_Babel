# Demonstration scripts

```bash
docker compose up -d          # from the repository root
./demo/run-all.sh             # or --core / --bonus
```

On Windows PowerShell, `./demo/run-all.sh` will not run: PowerShell does not
execute `.sh` files, it just opens them. Use the wrapper instead, which locates
Git Bash and delegates to the same script:

```powershell
docker compose up -d
.\demo\run-all.ps1            # or .\demo\run-all.ps1 --core
```

Requirements: `bash`, `curl`, `python3`. `jq` is used for pretty-printing when
present and skipped when it is not. A full run takes about a minute and exits 0.

| Script | Covers |
|---|---|
| `01-core.sh` | The nine points the specification asks to be demonstrated |
| `02-bonus.sh` | Runtime adapter loading, protocol migration, advanced networking, failure isolation |
| `run-all.sh` | Both, then resets the fault environment |
| `run-all.ps1` | PowerShell entry point that delegates to `run-all.sh` |
| `lib.sh` | Shared helpers: gateway calls, control-plane calls, ledger summaries |
| `adapters/` | Adapter specifications the bonus demo loads at runtime |

## What `01-core.sh` shows

| Section | Requirement |
|---|---|
| 1-2 | Gateway startup, and all three backends discovered and reachable |
| 3 | Protocol translation across HTTP-JSON, framed TCP and CRC-checked UDP, including argument renaming and result normalization |
| 4 | Capability-based routing, `preferred_service`, and the incapable-preference policy |
| 5 | 40 concurrent mixed requests with no mispairing and no duplicated identifiers |
| 6 | Timeout handling, and the reason this particular timeout must **not** fall back |
| 7 | Detection and rejection of malformed frames, bad checksums, mismatched identifiers and unsupported versions |
| 8 | Unsupported operations, invalid arguments, and caller contract violations |
| 9 | Runtime configuration changes surviving a container restart |

## What `02-bonus.sh` shows

| Section | Bonus |
|---|---|
| A | An adapter loaded and replaced at runtime; a broken one rejected with the previous adapter left serving |
| B | Protocol migration by variant weight: register at zero weight, canary, observe, roll back, and survive a restart |
| C | UDP reliability (retransmission, duplicate suppression, adaptive RTO), TCP multiplexing, backpressure |
| D | Failure isolation: one dead backend does not disturb the others |

## Reading the output

Two things in the transcript are worth watching, because they are where the
gateway's correctness claims are actually checked rather than asserted:

**The execution ledger.** Grader Hiura's control plane records a `received` and
an `executed` stage for every backend call. Section 6 prints it after a timeout
to show the operation ran exactly once. A gateway that fell back there would
show two executions of the same logical request.

**The counter deltas.** Sections 7 and C print the difference in the gateway's
own counters across a scenario: how many responses were rejected as malformed,
how many datagrams failed CRC32, how many duplicates were suppressed, how many
retransmissions were needed. A request that succeeds after corruption looks
identical to one that never hit a fault unless you look at these.

## Notes

- `02-bonus.sh` deliberately canaries onto a protocol version the reference
  backend does not speak. The failures it produces are the point: they show a
  bad rollout failing safely, being isolated to its own circuit breaker, and
  being reversible.
- Both scripts restart the `gateway` container via `docker compose`, and both
  restore the configuration they changed. Re-running either is safe.
- The control-plane token `babel-local-dev` is the documented default from the
  provided kit and is only ever used against `localhost`.
