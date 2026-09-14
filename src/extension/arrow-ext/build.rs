use std::path::Path;
use std::process::Command;

fn main() {
    let version = extension_version("ARROW_VERSION");

    println!("cargo:rustc-env=ARROW_VERSION={version}");
    println!("cargo:rerun-if-changed=Cargo.lock");

    let arrow_version =
        resolve_dep_version("arrow-schema").unwrap_or_else(|| "unknown".to_string());
    let parquet_version = resolve_dep_version("parquet").unwrap_or_else(|| "unknown".to_string());

    println!("cargo:rustc-env=ARROW_LIB_VERSION={arrow_version}");
    println!("cargo:rustc-env=PARQUET_LIB_VERSION={parquet_version}");
}

fn extension_version(env_name: &str) -> String {
    println!("cargo:rerun-if-env-changed={env_name}");
    println!("cargo:rerun-if-changed=.git_archival.txt");

    let describe = std::env::var(env_name)
        .ok()
        .filter(|s| !s.is_empty())
        .or_else(archived_describe)
        .or_else(git_describe)
        .unwrap_or_else(|| {
            panic!("cannot determine the extension version: set {env_name}, build from a GitHub release archive, or build from a git checkout with tags")
        });

    as_semver(&describe)
}

/// GitHub archives expand the `$Format:...$` placeholder (export-subst), a git checkout leaves it verbatim.
fn archived_describe() -> Option<String> {
    std::fs::read_to_string(".git_archival.txt")
        .ok()?
        .lines()
        .find_map(|line| line.strip_prefix("describe-name: "))
        .map(str::trim)
        .filter(|describe| !describe.is_empty() && !describe.starts_with("$Format"))
        .map(str::to_string)
}

fn git_describe() -> Option<String> {
    let describe = git(&["describe", "--tags", "--match", "[0-9]*"])?;

    for path in ["HEAD", "refs/heads", "refs/tags", "packed-refs"] {
        if let Some(watched) = git(&["rev-parse", "--path-format=absolute", "--git-path", path]) {
            if Path::new(&watched).exists() {
                println!("cargo:rerun-if-changed={watched}");
            }
        }
    }

    Some(describe)
}

fn git(args: &[&str]) -> Option<String> {
    Command::new("git")
        .args(args)
        .output()
        .ok()
        .filter(|output| output.status.success())
        .and_then(|output| String::from_utf8(output.stdout).ok())
        .map(|stdout| stdout.trim().to_string())
        .filter(|stdout| !stdout.is_empty())
}

/// `0.43.0-107-gf998a45d0` (git describe) becomes `0.43.0+107.gf998a45d0`, semver build metadata Composer reads as 0.43.0.
fn as_semver(describe: &str) -> String {
    let mut parts = describe.rsplitn(3, '-');

    match (parts.next(), parts.next(), parts.next()) {
        (Some(hash), Some(distance), Some(tag))
            if hash.starts_with('g')
                && !distance.is_empty()
                && distance.bytes().all(|b| b.is_ascii_digit()) =>
        {
            format!("{tag}+{distance}.{hash}")
        }
        _ => describe.to_string(),
    }
}

fn resolve_dep_version(crate_name: &str) -> Option<String> {
    let lock_contents = std::fs::read_to_string("Cargo.lock").ok()?;
    let needle = format!("name = \"{crate_name}\"");

    for chunk in lock_contents.split("[[package]]") {
        if chunk.contains(&needle) {
            for line in chunk.lines() {
                let line = line.trim();
                if line.starts_with("version = ") {
                    return Some(
                        line.trim_start_matches("version = ")
                            .trim_matches('"')
                            .to_string(),
                    );
                }
            }
        }
    }

    None
}
