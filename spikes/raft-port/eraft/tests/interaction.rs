// Ported from go.etcd.io/raft v3.7.0 (b867cf13f6bc0dae21204302df97bc2355c3af55),
// interaction_test.go. Licensed under the Apache License 2.0.
//
// P2: runs every testdata/*.txt file in rewrite mode and compares the
// rewritten file with the original byte for byte. Usage:
//   cargo test --release --test interaction -- <testdata dir> [<out dir>]
// The rewritten files go to <out dir> (default target/interaction-out) so
// `diff -u` shows every difference.

mod datadriven;
mod rafttest;

use std::panic::{AssertUnwindSafe, catch_unwind};

fn main() {
    let args: Vec<String> = std::env::args()
        .skip(1)
        .filter(|a| !a.starts_with("--"))
        .collect();
    let dir = args
        .first()
        .cloned()
        .unwrap_or_else(|| "../vendor/raft/testdata".to_string());
    let out_dir = args
        .get(1)
        .cloned()
        .unwrap_or_else(|| "../target/interaction-out".to_string());
    std::fs::create_dir_all(&out_dir).unwrap();
    let mut files: Vec<_> = std::fs::read_dir(&dir)
        .unwrap_or_else(|e| panic!("{dir}: {e}"))
        .map(|e| e.unwrap().path())
        .filter(|p| p.extension().is_some_and(|x| x == "txt"))
        .collect();
    files.sort();
    let (mut pass, mut total) = (0, 0);
    for path in &files {
        total += 1;
        let name = path.file_name().unwrap().to_string_lossy().to_string();
        let src = std::fs::read_to_string(path).unwrap();
        let res = catch_unwind(AssertUnwindSafe(|| {
            let mut env = rafttest::InteractionEnv::new();
            datadriven::rewrite(&name, &src, |d| env.handle(d))
        }));
        match res {
            Ok(out) => {
                std::fs::write(format!("{out_dir}/{name}"), &out).unwrap();
                if out == src {
                    pass += 1;
                    println!(
                        "[OK ] P2 {name}: byte-identical ({} lines)",
                        src.lines().count()
                    );
                } else {
                    let (a, b): (Vec<_>, Vec<_>) = (src.lines().collect(), out.lines().collect());
                    let first = a
                        .iter()
                        .zip(b.iter())
                        .position(|(x, y)| x != y)
                        .unwrap_or(a.len().min(b.len()));
                    println!(
                        "[FAIL] P2 {name}: got={} lines want={} lines, first difference at line {}",
                        b.len(),
                        a.len(),
                        first + 1
                    );
                }
            }
            Err(e) => {
                let msg = e
                    .downcast_ref::<String>()
                    .cloned()
                    .or_else(|| e.downcast_ref::<&str>().map(|s| s.to_string()))
                    .unwrap_or_default();
                println!("[FAIL] P2 {name}: panicked: {msg}");
            }
        }
    }
    println!(
        "P2 summary: {pass}/{total} files byte-identical ({:.0}%)",
        100.0 * pass as f64 / total.max(1) as f64
    );
    if pass != total {
        std::process::exit(1);
    }
}
