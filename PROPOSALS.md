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
- **The gates in CI** (2026-10-28, #1): `tacplus build` resolves `depends: kernel` through the private
  tacplus-dev/registry, which a workflow's own token cannot read, so the gates run only where tacplus has GitHub
  credentials (the looper). kernel, oauth and tenantecho, which also have `depends`, have no gates workflow either.
  The build container of decisions/deterministic-builds-2026-10-27.md, or a read token for the registry, would let
  CI run them.

## Promoted / declined
