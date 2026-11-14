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
  tacplus.lock to a release whose manifests declare their license (#368's backport to the 2.6 line, v2.6.4), delete
  the clarifications; if looper-scan's default policy changes meanwhile, `deny.toml`'s copy must follow it.

## Promoted / declined
- **The gates in CI** (2026-10-28, #1; done 2026-11-14, #11): `tacplus build` resolves `depends: kernel` through
  the private tacplus-dev/registry, which a workflow's own token cannot read, so the gates ran only where tacplus had
  GitHub credentials (the looper). The build pipeline of decisions/builds-in-containers-2026-11-03.md
  (gitlab.com/tacplus-dev/build) now runs `just gates` on every land and build of redo, in the pinned `build_image`,
  reading the registry through the read-only tacplus-reader app; the first green run is #11's.
