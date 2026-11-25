# State: redo

Overwritten to stay current (loopers and agents). Last updated: 2026-11-25 by the looper (#17).

- **Version / line:** v0.1.8 (source tags on main; v0.1.0 the first, 2026-11-07; v0.1.6 the relock of #14; v0.1.7 the release
  Dockerfile of #15; v0.1.8 its base pinned by digest, #17); tacplus floor 2.6, pinned by
  tacplus.lock (tacplus 2.6.14 and its `build` image by digest, #8; relocked in #13 and #14)
- **Works:** the build pipeline (gitlab.com/tacplus-dev/build) on every land: `just gates` (justfile: generate with
  the pinned tacplus, `cargo build --all-targets --locked`, `cargo test --locked`) and the security checks, whose
  cargo-deny reads deny.toml (the generated crates clarified Apache-2.0 until the pin has tacplus#368's fix) (#11).
  The service, tested in process (#1): `redolog.Redo` `Append` (seq order per tenant, LOCAL ack after
  fsync, REMOTE ack once the second site has it, MEMORY refused) and `Replay` (snapshot then tail); compaction
  (never past what the second site acked); tenant isolation by the caller's `/tenants/<tid>` identity, logs keyed by
  trust domain + tid; `redo.RedoSite` `Append` (shipping, resends and gaps, catch-up after an outage or a restart);
  tenant deletion from kernel's tid tombstones, at both sites, idempotent; the deletion shipped to the second site
  carries its tombstone's `trace_id`, kept on disk until the second site acks it (#10). The tests wait on one
  60 s `DEADLINE`, so a write-back backlog after a compile does not fail the gates (#7).
  The release [Dockerfile](Dockerfile) the fleet's image pipeline builds (pre-built binary and launcher, base
  `debian:trixie-slim` pinned by digest (#17), 65532:65532, `HEALTH_PORT=8444`), checked by tests/release.rs in the gates (#15).
- **Released / deployed:** nowhere yet.
- **Known gaps:** no deployed second site: a `DURABILITY_REMOTE` append is refused (FAILED_PRECONDITION) until the
  cross-deployment link (tacplus#204, #1 step 2). No published image or GitHub release yet (#2): the Dockerfile exists (#15), not the release workflow. Never run in a fleet: the
  kernel `ConfigState.tenant_ids` watch and the generated authorizer are exercised only by generation and
  compile, not end to end.
- **Blocked on:** tacplus-dev/tacplus#204 (the link between sites, #1 step 2).
