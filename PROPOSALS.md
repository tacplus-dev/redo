# Proposals: redo

Written by loopers. Each proposal: what, why, links. Jody promotes accepted ones into ROADMAP.md (via an agent);
the looper then marks it `promoted` or `declined` here with the date.

## Open
- **Seed a second site from a snapshot** (2026-10-28, #1): `RedoSite` ships operations only, so a second site can
  only receive what is still in the tail. The local site never folds an operation the second site has not acked,
  but a log compacted while no second site was configured (every deployment until tacplus#204), or a second site
  that lost its volume, can never catch up: the shipper logs it, stops shipping the tenant, and fails its
  `DURABILITY_REMOTE` acks (FAILED_PRECONDITION) rather than leave them waiting. A snapshot shipment (the
  tenant's live entries at a seq, replacing the copy) closes that. It is a change to the `RedoSite` contract, so it
  wants a decision before step 2 of #1.
- **Bind `source_trust_domain` to the caller's verified trust domain** (2026-10-28, #1): today the receiving site
  refuses its own trust domain and any caller but a restore-tenant `redo`, but such a caller could still ship (and
  delete) under another deployment's trust domain. With tacplus#204 the
  caller's identity carries its deployment's trust domain, and the receiving site should refuse a shipment that
  names another.
- **Formal verification of the log's framing and recovery** (2026-10-28, #1): src/store.rs's torn-tail and
  compaction recovery is tested by example; a Kani harness over frame parsing (tacplus-images record: Kani is in the
  build image) would cover every cut point.
- **Drop deny.toml's license clarifications** (2026-11-14, #11): tacplus 2.6.1 writes no `license` into the
  manifests of the crates it generates, so the security job's cargo-deny fails each as `unlicensed`
  (tacplus-dev/tacplus#368). `deny.toml` clarifies the 22 generated crates as Apache-2.0 (the header
  `tacplus.yaml` puts on every generated file) over looper-scan's default policy, copied. Once `tacplus pin` moves
  tacplus.lock to a release whose manifests declare their license (#368's backport to the 2.6 line), delete
  the clarifications; if looper-scan's default policy changes meanwhile, `deny.toml`'s copy must follow it.
  (Nightly scan 2026-11-17: #368 is still open, and no 2.6 release carries it yet: v2.6.4, published 2026-11-08,
  holds only #313's trace_id backport; the line is at v2.6.13. `deny.toml`'s comment naming "2.6.4" is wrong the
  same way, so correct it when the clarifications go.)
- **Pin the image's base by digest and upgrade it at build** (2026-11-23, #15): the Dockerfile's default
  `BASE_IMAGE` is the `debian:trixie-slim` tag, as #15 asked (oauth's form), so each build takes whatever the tag
  is that day, and the tag lags Debian's security fixes (gateway#38, llmproxy#62: zlib1g CVE-2026-85091). gateway
  and llmproxy pin `debian:trixie-slim@sha256:…` and `apt-get upgrade` (gateway from a dated snapshot, for a
  reproducible digest). Doing the same here wants the default to carry a digest and tests/release.rs to require one.
  (2026-11-25, #17: the default now carries the digest, `debian:trixie-slim@sha256:8aa5a7bf…`, and tests/release.rs
  requires one; the nightly relock moves it. The `apt-get upgrade` at build is still open.)
- **Ship each operation with its append's `trace_id`, once the sys contract carries one** (2026-11-15, nightly
  scan): tacplus's USER.md ("Distributed Tracing") says `trace_id` (field 200) is on every message, but the
  messages tacplus 2.6.1 generates for the sys contract `redolog.Redo` (`RedoAppend`, `RedoAck`,
  `RedoReplayRequest`, `RedoReplayEntry`, target/generated-sources/model/types/sys/redolog) have none, while
  redo's own (`Shipment`, `ShippedOperation`, `ShipmentAck`) and kernel's `TenantIdRow` do. So a Stratum's trace
  ends at `Append`, and the shipper's `// trace: unrelated:` markers in src/ship.rs are right today. When a tacplus
  release gives `RedoAppend` its field 200 (tacplus#265 revisits tracing), keep each operation's `trace_id` in its
  tail frame (`ShippedOperation` already has the field, so the frame format does not change) and ship it on the
  operation's `Shipment`, as a deletion already carries its tombstone's (#10), with a test asserting it arrives;
  drop the markers then. Until then there is nothing to propagate.
- **ROADMAP.md "Now" to the floor in force** (2026-11-16, #9): #9's change 4 asks for it, but a looper's land that
  touches ROADMAP.md is refused unless the issue's own text says it is at Jody's request (tacplus-dev/looper#123),
  and #9's does not, so this is for an agent at Jody's request (or for the looper, once #9's body says so). "Now"
  still reads "Unreleased; no code yet (README and LICENSE only). On the Stratum critical path: tacplus#163 → #1 →
  stratum#2." The line in force: "Released as source tags on main, v0.1.0 (2026-11-07) to v0.1.3 (2026-11-14); no
  image yet (#2). tacplus floor 2.6, pinned at 2.6.1 by tacplus.lock (#8, #9); every land and build runs `just
  gates` and the security checks in the pinned build image (gitlab.com/tacplus-dev/build, #11). The second site
  waits on tacplus#204 (#1 step 2)."
- **Unmaintained crates in the generated chassis' dependency tree** (2026-11-16, #9): the build pipeline's
  security checks (cargo-deny and osv-scanner) report four advisories against Cargo.lock, all severity UNKNOWN,
  none failing the policy: RUSTSEC-2025-0012 `backoff` 0.4.0 and RUSTSEC-2024-0384 `instant` 0.1.13 (through
  `backoff`), both pulled by the generated `httpclient`; RUSTSEC-2025-0134 `rustls-pemfile` 2.2.0, pulled by
  `httpclient` and `servicemanager`; RUSTSEC-2024-0449 `unsafe-libyaml` 0.2.11 (through `serde_yaml`), pulled by
  `grpcconfig`, `grpcserver` and the generated types crate. None is a dependency of redo's own (Cargo.toml): they
  are the chassis crates tacplus generates, so the fix is in tacplus (replace `backoff`, `rustls-pemfile` with
  `rustls-pki-types`' parser, `serde_yaml` with a maintained fork), after which `tacplus pin` to the release that
  carries it moves Cargo.lock here. Until then they are accepted as they stand: a `security-baseline.json`
  (`looper-scan baseline`) would record them, but they are already not new to the checks and block nothing.
  (Nightly scan 2026-11-21: a fifth, RUSTSEC-2026-0543 `chrono` 0.4.45 unmaintained, severity UNKNOWN, no fixed
  version, not blocking, pulled only by the generated `logger` crate; cargo-deny and osv-scanner both report it, and
  a local `osv-scanner scan source --lockfile Cargo.lock` shows the same five. The fix goes in tacplus the same
  way: the generated `logger` moves to `time` or `jiff`.)
  (Nightly scan 2026-11-23, on the report of 43f3f78's land build, whose Cargo.lock is HEAD's: `backoff` and
  `instant` are gone since the relock of #13; three unmaintained remain (`unsafe-libyaml`, `rustls-pemfile`,
  `chrono`). New in the report: RUSTSEC-2026-0592 (GHSA-hrj5-67jp-wqp9), `url` 2.5.8, a stack overflow in
  `Url::origin()` on nested `blob:` URLs, fixed in 2.5.9; severity UNKNOWN, not blocking, so not filed. `url` comes
  in through `reqwest`, `tower-http` and the generated `service_im`; redo's own src never calls `Url::origin`.
  `cargo update -p url --precise 2.5.9` would clear it; a `tacplus pin` that moves Cargo.lock may do so too.)

## Promoted / declined
- **The gates in CI** (2026-10-28, #1; done 2026-11-14, #11): `tacplus build` resolves `depends: kernel` through
  the private tacplus-dev/registry, which a workflow's own token cannot read, so the gates ran only where tacplus had
  GitHub credentials (the looper). The build pipeline of decisions/builds-in-containers-2026-11-03.md
  (gitlab.com/tacplus-dev/build) now runs `just gates` on every land and build of redo, in the pinned `build_image`,
  reading the registry through the read-only tacplus-reader app; the first green run is #11's.
