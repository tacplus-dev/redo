# redo Release Image
# Layers the pre-built redo binary onto a minimal runtime image. It compiles
# nothing: the fleet's image pipeline (fleet-seatrials images/service.cloudbuild.yaml)
# cross-builds <arch>-unknown-linux-gnu and puts the binary at target/release/redo,
# once per platform (linux/arm64, and linux/amd64), the RUN steps running under
# QEMU for a platform the worker is not. tests/release.rs checks this file.

# The base is a build argument: a fleet may hand in its own hardened base, the
# default is the public one (glibc, as the binary is built for), pinned by digest
# (#16: immutable tags only), so a rebuild ships the base that was scanned. Nightly
# maintenance moves the digest (tacplus-dev/looper#234).
ARG BASE_IMAGE=debian:trixie-slim@sha256:8aa5a7bf5a1955bddbe7d74fcdd0b91ab30e74362a8b73af672b0481b35b44a0
FROM ${BASE_IMAGE}

ARG VCS_REF
ARG VERSION

LABEL org.opencontainers.image.authors="Jody Scott <jody@thescottsweb.com>" \
      org.opencontainers.image.version="${VERSION}" \
      org.opencontainers.image.revision="${VCS_REF}" \
      org.opencontainers.image.source="https://github.com/tacplus-dev/redo" \
      org.opencontainers.image.vendor="tacplus.dev" \
      org.opencontainers.image.title="redo" \
      org.opencontainers.image.description="Durability for every tenant's Stratum, and its copy at a second site"

# The chassis talks TLS to kernel: ca-certificates is not in the slim base.
RUN apt-get update \
 && apt-get install -y --no-install-recommends ca-certificates \
 && rm -rf /var/lib/apt/lists/*

# redo needs no root. It runs as a fixed UID/GID, not a useradd default, so the
# operator's securityContext (runAsUser/runAsGroup 65532) can name it. It owns
# /var/lib/redo, Config.data_dir's default (proto/services/redo.proto), where
# the instance's regional volume is mounted; without one the logs stay there.
RUN groupadd --system --gid 65532 redo \
 && useradd --system --uid 65532 --gid 65532 --no-create-home \
      --home-dir /nonexistent --shell /usr/sbin/nologin redo \
 && install -d -o 65532 -g 65532 -m 0700 /var/lib/redo

COPY target/release/redo /usr/local/bin/redo

# Every service runs under the launcher (github.com/tacplus-dev/launcher): PID 1
# that forwards signals, propagates the exit code, and kills a deadlocked child
# on gRPC health. The fleet's image pipeline puts the binary beside the Dockerfile.
# The launcher probes plaintext (no HEALTH_CA): the chassis's health_probe_port,
# 8444 by default, which serves only grpc.health.v1, not the TLS port 8443.
ARG TARGETARCH
COPY launcher-linux-${TARGETARCH} /usr/local/bin/launcher
ENV LAUNCH_CMD="redo" HEALTH_PORT=8444
USER 65532:65532
ENTRYPOINT ["launcher"]
