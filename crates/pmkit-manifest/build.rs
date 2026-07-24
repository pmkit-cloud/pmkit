//! Captures manifest provenance while the crate is compiled.

use std::env;
use std::fs;
use std::path::Path;
use std::process::Command;

const UNKNOWN: &str = "unknown";
const UNKNOWN_SHA256: &str = "0000000000000000000000000000000000000000000000000000000000000000";

fn main() {
    println!("cargo::rerun-if-changed=../../Cargo.lock");
    println!("cargo::rerun-if-changed=../../.git/HEAD");
    println!("cargo::rerun-if-changed=../../.git/index");
    println!("cargo::rerun-if-changed=src");

    let Some(manifest_dir) = env::var_os("CARGO_MANIFEST_DIR") else {
        println!("cargo::error=CARGO_MANIFEST_DIR is unavailable");
        return;
    };
    let Some(workspace_root) = Path::new(&manifest_dir).parent().and_then(Path::parent) else {
        println!("cargo::error=pmkit-manifest is not under the workspace crates directory");
        return;
    };
    let Some(out_dir) = env::var_os("OUT_DIR") else {
        println!("cargo::error=OUT_DIR is unavailable");
        return;
    };

    let (git_commit, git_dirty) = git_provenance(workspace_root);
    let cargo_lock_sha256 = cargo_lock_sha256(&workspace_root.join("Cargo.lock"));
    let rustc = env::var_os("RUSTC").unwrap_or_else(|| "rustc".into());
    let toolchain = command_stdout(Command::new(rustc).arg("--version"))
        .filter(|identity| !identity.is_empty())
        .unwrap_or_else(|| UNKNOWN.to_owned());
    let generated = format!(
        "pub const GIT_COMMIT: &str = {git_commit:?};\n\
         pub const GIT_DIRTY: bool = {git_dirty};\n\
         pub const CARGO_LOCK_SHA256: &str = {cargo_lock_sha256:?};\n\
         pub const TOOLCHAIN: &str = {toolchain:?};\n"
    );

    if let Err(error) = fs::write(Path::new(&out_dir).join("provenance.rs"), generated) {
        println!("cargo::error=failed to write provenance constants: {error}");
    }
}

fn git_provenance(workspace_root: &Path) -> (String, bool) {
    let commit = command_stdout(
        Command::new("git")
            .args(["rev-parse", "HEAD"])
            .current_dir(workspace_root),
    )
    .filter(|commit| !commit.is_empty());
    let Some(commit) = commit else {
        return (UNKNOWN.to_owned(), false);
    };
    let dirty = command_stdout(
        Command::new("git")
            .args(["status", "--porcelain"])
            .current_dir(workspace_root),
    )
    .is_some_and(|status| !status.is_empty());
    (commit, dirty)
}

fn cargo_lock_sha256(cargo_lock: &Path) -> String {
    command_stdout(Command::new("sha256sum").arg(cargo_lock))
        .and_then(|output| output.split_whitespace().next().map(str::to_owned))
        .filter(|hash| is_sha256(hash))
        .or_else(|| {
            command_stdout(Command::new("shasum").args(["-a", "256"]).arg(cargo_lock))
                .and_then(|output| output.split_whitespace().next().map(str::to_owned))
                .filter(|hash| is_sha256(hash))
        })
        .or_else(|| {
            command_stdout(
                Command::new("certutil")
                    .arg("-hashfile")
                    .arg(cargo_lock)
                    .arg("SHA256"),
            )
            .and_then(|output| {
                output.lines().find_map(|line| {
                    let hash = line.split_whitespace().collect::<String>();
                    is_sha256(&hash).then_some(hash)
                })
            })
        })
        .map_or_else(
            || UNKNOWN_SHA256.to_owned(),
            |hash| hash.to_ascii_lowercase(),
        )
}

fn is_sha256(value: &str) -> bool {
    value.len() == 64 && value.bytes().all(|byte| byte.is_ascii_hexdigit())
}

fn command_stdout(command: &mut Command) -> Option<String> {
    let output = command.output().ok()?;
    output
        .status
        .success()
        .then(|| String::from_utf8_lossy(&output.stdout).trim().to_owned())
}
