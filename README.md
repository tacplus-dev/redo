# redo

Durability for every tenant's Stratum, and its backup at a second site. Not per tenant.

Each tenant's Stratum holds no durable state; a write is acked once the redo instance local to the cluster
has it on disk (a regional, two-zone volume), and that instance ships it asynchronously to the redo service
at another site, tagged by tenant id. A tenant's Stratum starting anywhere replays its tenant from here
(a compacted snapshot per tenant plus the tail behind it). Restore is to the latest state.

Decision record: `stratum-per-tenant-2026-10-25` (schematic roadmap), items 5–8 and 11, and its
"Scope now" section. Work arrives as issues; see github.com/tacplus-dev/looper.
