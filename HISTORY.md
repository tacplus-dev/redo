# History: redo

Append-only: add entries at the end, never edit or remove one (CI enforces it). One line each:
`- <YYYY-MM-DD> <who: looper|agent|jody> <kind: issue|decision|release|deploy> <one line> (<links>)`

- 2026-10-30 looper issue Gates fail on main: Cargo.lock is stale for tacplus 2.5.3's generated chassis (cargo build --locked) (#4, 01738a8)
- 2026-11-02 looper issue Gates fail on main: Cargo.lock is stale for tacplus 2.5.6's generated chassis (digest 0.11 stack) (#5, 9db1edf)
- 2026-11-05 looper issue Trace propagation: the shipper's RedoSite.Append call lacks its // trace: unrelated: marker (src/ship.rs) (#6, 30270ad)
- 2026-11-08 looper issue tacplus floor 2.5 is behind the latest released line (2.6): move the floor to 2.6 and commit tacplus.lock (#8)
- 2026-11-14 looper release v0.1.2 (https://github.com/tacplus-dev/redo/releases/tag/v0.1.2)
