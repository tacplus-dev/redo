# Local development builds, and `just gates`: the checks the build job and the
# looper's lands run on every change to main (README: Building).

TACPLUS := env_var_or_default("TACPLUS_BIN", "tacplus")

default: generate

# Regenerate target/generated-sources (not committed: redo-model and the chassis
# crates Cargo.toml names by path) with exactly the release tacplus.lock pins
# (moved only by `tacplus pin`, in a commit); any other compiler is refused, so a
# new patch never reaches main without a commit. Generated code is never
# committed or edited by hand, so generation must leave every tracked file as it
# found it. The build job keeps what this writes for its security checks, so
# clippy and cargo-deny see the generated crates too.
generate:
  #!/usr/bin/env bash
  set -euo pipefail
  pin="$(sed -n -E 's/^tacplus:[[:space:]]*"?([0-9]+\.[0-9]+\.[0-9]+)"?[[:space:]]*$/\1/p' tacplus.lock)"
  [[ -n "$pin" ]] || { echo "generate: tacplus.lock pins no tacplus release" >&2; exit 1; }
  running="$({{TACPLUS}} --version | awk '{print $NF}')"
  running="${running#v}"
  if [[ "$running" != "$pin" ]]; then
    echo "generate: tacplus.lock pins tacplus $pin, but {{TACPLUS}} is $running" >&2; exit 1
  fi
  before="$(git status --porcelain --untracked-files=no; git diff HEAD | sha256sum)"
  {{TACPLUS}} build --tag latest
  if [[ "$(git status --porcelain --untracked-files=no; git diff HEAD | sha256sum)" != "$before" ]]; then
    echo "generate: tacplus build changed tracked files:" >&2; git status --short >&2; exit 1
  fi

# Nothing under a build output may be tracked.
check-generated: generate
  test -z "$(git ls-files target)" || { echo "check-generated: target/ is build output and must not be committed" >&2; exit 1; }

# Cargo.lock is committed and never moved by a build (--locked); protoc must be
# on the path (a dependency's build script runs it).
build: generate
  cargo build --all-targets --locked

test: build
  cargo test --locked

# The gates every change passes before it is pushed: generate first, so the
# build and the tests compile what the pinned tacplus writes, never stale code.
gates: check-generated test

clean:
  rm -rf target
