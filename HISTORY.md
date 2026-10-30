# History: redo

Append-only: add entries at the end, never edit or remove one (CI enforces it). One line each:
`- <YYYY-MM-DD> <who: looper|agent|jody> <kind: issue|decision|release|deploy> <one line> (<links>)`

- 2026-10-30 looper issue Gates fail on main: Cargo.lock is stale for tacplus 2.5.3's generated chassis (cargo build --locked) (#4, 01738a8)
