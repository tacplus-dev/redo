# redo

Durability for every tenant's Stratum, and its backup at a second site. Not per tenant.

Each tenant's Stratum holds no durable state; a write is acked once the redo instance local to the cluster
has it on disk (a regional, two-zone volume), and that instance ships it asynchronously to the redo service
at another site, tagged by trust domain and tenant id. A tenant's Stratum starting anywhere replays its tenant
from here (a compacted snapshot per tenant plus the tail behind it). Restore is to the latest state.

Decision record: `stratum-per-tenant-2026-10-25` (schematic roadmap), items 5–8 and 11, and its
"Scope now" section and 2026-10-28 addendum (the second site is redo to redo); `tenant-deletion-2026-10-27`.
Work arrives as issues; see github.com/tacplus-dev/looper.

## The service

A tacplus service in Rust, a tenant service of the reserved `restore` tenant (tid `00000002`). It serves:

- **`redolog.Redo`**, the sys contract (tacplus `sys/redo.proto`), to every tenant's Stratum: `Append` (acked
  `DURABILITY_LOCAL` once on this site's disk, `DURABILITY_REMOTE` once the second site has it too) and `Replay`
  (the snapshot, then the tail). The tenant a write belongs to is the caller's `/tenants/<tid>` identity.
- **`redo.RedoSite`** ([proto/services/redo.proto](proto/services/redo.proto)), to the redo of another deployment:
  `Append`, the stream one site ships its tenants' logs over, and their deletions.

How it works is in [proto/services/redo.proto](proto/services/redo.proto) and [src/service.rs](src/service.rs);
the logs on disk in [src/store.rs](src/store.rs), shipping in [src/ship.rs](src/ship.rs).

The second site is behind one interface ([src/site.rs](src/site.rs)). The tests reach a second in-process site
through it. The deployed link between two deployments is a generated cross-deployment S2S client
([tacplus#204](https://github.com/tacplus-dev/tacplus/issues/204)); until it lands, no second site is
configured, and a write owed `DURABILITY_REMOTE` is refused.

## Building

The gates: generate with tacplus at the floor `tacplus.yaml` declares (`tacplus build`), then
`cargo build --locked` and `cargo test --locked` (with `protoc` on the path: a dependency's build script runs it).
`tacplus build` resolves `depends` (kernel, for its tenant tombstones) through the private tacplus-dev/registry,
so it needs GitHub credentials that can read it; that is why there is no CI workflow for the gates yet
([PROPOSALS.md](PROPOSALS.md)).
