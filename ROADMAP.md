# Roadmap: redo

Owned by Jody; changed only by an agent at Jody's request. Loopers propose in PROPOSALS.md.

## Now
The redo service: durability for every tenant's Stratum and its backup at a second site; one service for all tenants, in the reserved `restore` tenant (00000002).
Unreleased; no code yet (README and LICENSE only). On the Stratum critical path: tacplus#163 → #1 → stratum#2.

## Next
- Nothing unblocked yet.

## Later
- The redo service: local ack, second-site copy, compaction, replay, tenant isolation (#1), after tacplus#163 and tacplus#170
- Release: tag v0.1.0 and publish the image (#2), after #1 and tacplus-dev/looper#12
- Per-tenant encryption keys (tacplus-dev/roadmap:decisions/stratum-per-tenant-2026-10-25.md, Open)
- Region failover: every redo write carries the lease term, older terms refused (tacplus-dev/roadmap:decisions/stratum-per-tenant-2026-10-25.md, Kept for later)

## Not doing
- Running per tenant: one service receives every tenant's writes, tagged by tenant id (decisions/stratum-per-tenant-2026-10-25.md)
- Running as a platform service in `tac-<deployment>`: it is a tenant service of `restore` (redo#1, Jody 2026-10-26)
- Built-in point-in-time restore: restore is to the latest state; point-in-time is by editing the log by hand (decisions/stratum-per-tenant-2026-10-25.md)
