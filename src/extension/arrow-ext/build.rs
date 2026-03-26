fn main() {
    let version = std::env::var("ARROW_VERSION")
        .ok()
        .filter(|s| !s.is_empty())
        .or_else(|| {
            std::process::Command::new("git")
                .args(["describe", "--tags", "--always"])
                .output()
                .ok()
                .filter(|o| o.status.success())
                .and_then(|o| String::from_utf8(o.stdout).ok())
                .map(|s| s.trim().to_string())
                .filter(|s| !s.is_empty())
        })
        .unwrap_or_else(|| env!("CARGO_PKG_VERSION").to_string());

    println!("cargo:rustc-env=ARROW_VERSION={version}");

    let arrow_version = resolve_dep_version("arrow-schema").unwrap_or_else(|| "unknown".to_string());
    let parquet_version = resolve_dep_version("parquet").unwrap_or_else(|| "unknown".to_string());

    println!("cargo:rustc-env=ARROW_LIB_VERSION={arrow_version}");
    println!("cargo:rustc-env=PARQUET_LIB_VERSION={parquet_version}");
}

fn resolve_dep_version(crate_name: &str) -> Option<String> {
    let lock_contents = std::fs::read_to_string("Cargo.lock").ok()?;
    let needle = format!("name = \"{crate_name}\"");

    for chunk in lock_contents.split("[[package]]") {
        if chunk.contains(&needle) {
            for line in chunk.lines() {
                let line = line.trim();
                if line.starts_with("version = ") {
                    return Some(line.trim_start_matches("version = ").trim_matches('"').to_string());
                }
            }
        }
    }

    None
}
