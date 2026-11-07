# State: redo

Overwritten to stay current (loopers and agents). Last updated: 2026-11-07 by the looper (#8).

- **Version / line:** v0.1.1 (source tags on main; v0.1.0 the first, 2026-11-07); tacplus floor 2.6, pinned by
  tacplus.lock (tacplus 2.6.1 and its `build` image by digest, #8)
- **Works:** the service, tested in process (#1): `redolog.Redo` `Append` (seq order per tenant, LOCAL ack after
  fsync, REMOTE ack once the second site has it, MEMORY refused) and `Replay` (snapshot then tail); compaction
  (never past what the second site acked); tenant isolation by the caller's `/tenants/<tid>` identity, logs keyed by
  trust domain + tid; `redo.RedoSite` `Append` (shipping, resends and gaps, catch-up after an outage or a restart);
  tenant deletion from kernel's tid tombstones, at both sites, idempotent.
- **Released / deployed:** nowhere yet.
- **Known gaps:** no deployed second site: a `DURABILITY_REMOTE` append is refused (FAILED_PRECONDITION) until the
  cross-deployment link (tacplus#204, #1 step 2). No image or Dockerfile yet (#2). Never run in a fleet: the
  kernel `ConfigState.tenant_ids` watch and the generated authorizer are exercised only by generation and
  compile, not end to end.
- **Blocked on:** tacplus-dev/tacplus#204 (the link between sites, #1 step 2).
