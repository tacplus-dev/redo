//! The release image (Dockerfile, #15), read against what the fleet's image pipeline hands it
//! (tacplus-dev/fleet-seatrials images/service.cloudbuild.yaml): the binary pre-built at target/release/redo,
//! the launcher beside the Dockerfile as launcher-linux-<arch>, and the build args VERSION, VCS_REF and
//! TARGETARCH, for linux/arm64 and linux/amd64.

use std::path::{Path, PathBuf};

fn root() -> &'static Path {
    Path::new(env!("CARGO_MANIFEST_DIR"))
}

fn read(path: impl AsRef<Path>) -> String {
    let path = root().join(path);
    std::fs::read_to_string(&path).unwrap_or_else(|e| panic!("{}: {e}", path.display()))
}

/// The Dockerfile's instructions, one per entry: comments and blank lines dropped, continuations joined.
fn instructions() -> Vec<String> {
    let mut out = Vec::new();
    let mut cur = String::new();
    for line in read("Dockerfile").lines() {
        let t = line.trim();
        if t.is_empty() || t.starts_with('#') {
            continue;
        }
        match t.strip_suffix('\\') {
            Some(part) => {
                cur.push_str(part.trim_end());
                cur.push(' ');
            }
            None => {
                cur.push_str(t);
                out.push(std::mem::take(&mut cur));
            }
        }
    }
    out
}

fn with(keyword: &str) -> Vec<String> {
    let prefix = format!("{keyword} ");
    instructions().into_iter().filter(|i| i.starts_with(&prefix)).collect()
}

fn has(want: &str) {
    let all = instructions();
    assert!(all.iter().any(|i| i == want), "no instruction {want:?} in {all:#?}");
}

#[test]
fn layers_the_prebuilt_binary_on_the_base_and_compiles_nothing() {
    // A public base by default (glibc: the pipeline builds <arch>-unknown-linux-gnu); a fleet may hand in its own.
    assert_eq!(
        with("FROM"),
        ["FROM ${BASE_IMAGE}"],
        "one stage, from the base argument"
    );
    let all = instructions();
    let base = all.iter().position(|i| i == "ARG BASE_IMAGE=debian:trixie-slim");
    let from = all.iter().position(|i| i == "FROM ${BASE_IMAGE}");
    assert!(base.is_some() && base < from, "ARG BASE_IMAGE=debian:trixie-slim before the FROM: {all:#?}");
    has("COPY target/release/redo /usr/local/bin/redo");
    for i in with("RUN") {
        assert!(!i.contains("cargo") && !i.contains("rustc"), "the image compiles: {i}");
    }
}

#[test]
fn installs_ca_certificates() {
    let run = with("RUN");
    assert!(
        run.iter().any(|i| i.contains("apt-get install") && i.contains("ca-certificates")),
        "no ca-certificates in {run:#?}"
    );
}

#[test]
fn runs_under_the_launcher_probing_the_chassis_plaintext_health_port() {
    has("ARG TARGETARCH");
    has("COPY launcher-linux-${TARGETARCH} /usr/local/bin/launcher");
    has(r#"ENTRYPOINT ["launcher"]"#);
    // The launcher probes plaintext unless given HEALTH_CA (tacplus-dev/launcher#7): the chassis's
    // health_probe_port, never the TLS port 8443, where every probe would fail and the launcher kill redo.
    has(r#"ENV LAUNCH_CMD="redo" HEALTH_PORT=8444"#);
}

/// The generated grpcserver's default health_probe_port, which redo's config does not override.
#[test]
fn the_health_port_is_the_chassis_default() {
    let lib: PathBuf = ["target", "generated-sources", "model", "chassis", "grpcserver", "src", "lib.rs"]
        .iter()
        .collect();
    let src = read(lib);
    let body = src
        .split_once("fn default_health_probe_port()")
        .expect("the chassis has a default_health_probe_port")
        .1;
    let body = &body[body.find('{').unwrap() + 1..body.find('}').unwrap()];
    assert_eq!(body.trim(), "8444", "the chassis's plaintext health port moved: the Dockerfile's HEALTH_PORT must too");
}

#[test]
fn labels_the_version_and_the_revision_from_the_build_args() {
    has("ARG VERSION");
    has("ARG VCS_REF");
    let labels = with("LABEL").join(" ");
    for want in [
        r#"org.opencontainers.image.version="${VERSION}""#,
        r#"org.opencontainers.image.revision="${VCS_REF}""#,
        r#"org.opencontainers.image.source="https://github.com/tacplus-dev/redo""#,
    ] {
        assert!(labels.contains(want), "no label {want} in {labels}");
    }
}

#[test]
fn runs_as_a_fixed_non_root_user_that_owns_the_data_dir() {
    // A fixed UID/GID, so the operator's securityContext (runAsUser/runAsGroup 65532) can name it.
    let users = with("USER");
    assert_eq!(users, ["USER 65532:65532"]);
    let all = instructions();
    assert_eq!(
        all.iter().position(|i| i.starts_with("USER ")),
        all.len().checked_sub(2),
        "USER last but the ENTRYPOINT: {all:#?}"
    );
    // Config.data_dir's default (proto/services/redo.proto): writable by redo when no volume is mounted there.
    assert!(read("proto/services/redo.proto").contains(r#"(spec.default_string) = "/var/lib/redo""#));
    let run = with("RUN").join(" ");
    assert!(
        run.contains("--uid 65532") && run.contains("--gid 65532"),
        "no 65532 user and group: {run}"
    );
    assert!(run.contains("install -d -o 65532 -g 65532 -m 0700 /var/lib/redo"), "no /var/lib/redo for 65532: {run}");
}

#[test]
fn the_build_context_keeps_what_the_dockerfile_copies_and_drops_the_rest() {
    let ignore = read(".dockerignore");
    let lines: Vec<&str> = ignore.lines().map(str::trim).filter(|l| !l.is_empty() && !l.starts_with('#')).collect();
    // The pipeline's build tree (target/, CARGO_HOME at .cargo, .fleet) stays out of the context but for the binary.
    for want in ["target/", "!target/release/redo", ".cargo/", ".fleet/", ".git/"] {
        assert!(lines.contains(&want), "no {want} in .dockerignore: {lines:?}");
    }
    assert!(
        lines.iter().position(|l| *l == "target/") < lines.iter().position(|l| *l == "!target/release/redo"),
        "the exception must follow target/: {lines:?}"
    );
    assert!(!lines.iter().any(|l| l.starts_with("launcher")), "the launcher is ignored: {lines:?}");
}
