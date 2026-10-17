// Copyright 2019 The etcd Authors
// Ported from go.etcd.io/raft v3.7.0 (b867cf13f6bc0dae21204302df97bc2355c3af55),
// confchange/datadriven_test.go, confchange/quick_test.go and
// confchange/restore_test.go. Licensed under the Apache License 2.0.
//
// Usage: cargo test --test confchange_datadriven -- [<testdata dir>]
// Each testdata file is rewritten and compared byte for byte (see
// tests/interaction.rs). The quickcheck tests use a seeded xorshift instead of
// testing/quick.

mod datadriven;

use eraft::confchange::{Changer, describe, restore};
use eraft::raftpb::{ConfChangeSingle, ConfChangeType, ConfState};
use eraft::tracker::{ProgressTracker, progress_map_string};

use datadriven::TestData;

struct Env {
    tracker: ProgressTracker,
    last_index: u64,
}

impl Env {
    fn handle(&mut self, d: &TestData) -> String {
        let out = self.handle_inner(d);
        self.last_index += 1;
        out
    }

    fn handle_inner(&mut self, d: &TestData) -> String {
        let mut ccs = Vec::new();
        let input = d.input.trim();
        let toks: Vec<&str> = if input.is_empty() {
            Vec::new()
        } else {
            input.split(' ').collect()
        };
        for tok in toks {
            if tok.len() < 2 {
                return format!("unknown token {tok}");
            }
            let change_type = match tok.as_bytes()[0] {
                b'v' => ConfChangeType::ConfChangeAddNode,
                b'l' => ConfChangeType::ConfChangeAddLearnerNode,
                b'r' => ConfChangeType::ConfChangeRemoveNode,
                b'u' => ConfChangeType::ConfChangeUpdateNode,
                _ => return format!("unknown input: {tok}"),
            };
            let node_id = match tok[1..].parse::<u64>() {
                Ok(v) => v,
                Err(_) => {
                    return format!("strconv.ParseUint: parsing {:?}: invalid syntax", &tok[1..]);
                }
            };
            ccs.push(ConfChangeSingle {
                change_type,
                node_id,
            });
        }
        let c = Changer {
            tracker: &self.tracker,
            last_index: self.last_index,
        };
        let res = match d.cmd.as_str() {
            "simple" => c.simple(&ccs),
            "enter-joint" => {
                let auto_leave = d.arg("autoleave").is_some_and(|a| a.vals[0] == "true");
                c.enter_joint(auto_leave, &ccs)
            }
            "leave-joint" => {
                if !ccs.is_empty() {
                    Err("this command takes no input".to_string())
                } else {
                    c.leave_joint()
                }
            }
            _ => return "unknown command".to_string(),
        };
        match res {
            Err(e) => e + "\n",
            Ok((cfg, trk)) => {
                self.tracker.config = cfg;
                self.tracker.progress = trk;
                format!(
                    "{}\n{}",
                    self.tracker.config,
                    progress_map_string(&self.tracker.progress)
                )
            }
        }
    }
}

struct XorShift(u64);

impl XorShift {
    fn next(&mut self) -> u64 {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        self.0
    }
    fn intn(&mut self, n: usize) -> usize {
        if n == 0 {
            0
        } else {
            (self.next() % n as u64) as usize
        }
    }
    fn perm(&mut self, n: usize) -> Vec<usize> {
        let mut p: Vec<usize> = (0..n).collect();
        for i in (1..n).rev() {
            p.swap(i, self.intn(i + 1));
        }
        p
    }
}

fn gen_cc(
    num: &mut dyn FnMut() -> usize,
    id: &mut dyn FnMut() -> u64,
    typ: &mut dyn FnMut() -> ConfChangeType,
) -> Vec<ConfChangeSingle> {
    let n = num();
    (0..n)
        .map(|_| ConfChangeSingle {
            change_type: typ(),
            node_id: id(),
        })
        .collect()
}

fn type_of(i: usize) -> ConfChangeType {
    [
        ConfChangeType::ConfChangeAddNode,
        ConfChangeType::ConfChangeRemoveNode,
        ConfChangeType::ConfChangeUpdateNode,
        ConfChangeType::ConfChangeAddLearnerNode,
    ][i]
}

fn run_with_simple(t: &mut ProgressTracker, ccs: &[ConfChangeSingle]) -> Result<(), String> {
    for cc in ccs {
        let (cfg, trk) = Changer {
            tracker: t,
            last_index: 10,
        }
        .simple(std::slice::from_ref(cc))?;
        t.config = cfg;
        t.progress = trk;
    }
    Ok(())
}

fn run_with_joint(t: &mut ProgressTracker, ccs: &[ConfChangeSingle]) -> Result<(), String> {
    let (cfg, trk) = Changer {
        tracker: t,
        last_index: 10,
    }
    .enter_joint(false, ccs)?;
    let (mut cfg2a, trk2a) = Changer {
        tracker: t,
        last_index: 10,
    }
    .enter_joint(true, ccs)?;
    cfg2a.auto_leave = false;
    if cfg != cfg2a || trk != trk2a {
        return Err("autoLeave=true entered a different config".into());
    }
    t.config = cfg.clone();
    t.progress = trk.clone();
    let (cfg2b, trk2b) = Changer {
        tracker: t,
        last_index: 10,
    }
    .leave_joint()?;
    t.config = cfg;
    t.progress = trk;
    let (cfg, trk) = Changer {
        tracker: t,
        last_index: 10,
    }
    .leave_joint()?;
    if cfg != cfg2b || trk != trk2b {
        return Err("leave-joint not deterministic".into());
    }
    t.config = cfg;
    t.progress = trk;
    Ok(())
}

/// TestConfChangeQuick: simple and joint config changes arrive at the same result.
fn test_conf_change_quick() -> Result<(), String> {
    let mut rng = XorShift(0x9E3779B97F4A7C15);
    for i in 0..1000 {
        let setup = {
            let mut v = vec![ConfChangeSingle {
                change_type: ConfChangeType::ConfChangeAddNode,
                node_id: 1,
            }];
            let n = 1 + rng.intn(5);
            for _ in 0..n {
                let id = 1 + rng.intn(5) as u64;
                v.push(ConfChangeSingle {
                    change_type: ConfChangeType::ConfChangeAddNode,
                    node_id: id,
                });
            }
            v
        };
        let ccs = {
            let r = std::cell::RefCell::new(&mut rng);
            gen_cc(
                &mut || 1 + r.borrow_mut().intn(9),
                &mut || 1 + (1 + r.borrow_mut().intn(9)) as u64,
                &mut || type_of(r.borrow_mut().intn(4)),
            )
        };
        let mut t1 = ProgressTracker::new(10, 0);
        run_with_simple(&mut t1, &setup)?;
        run_with_simple(&mut t1, &ccs).map_err(|e| format!("#{i} simple: {e}"))?;
        let mut t2 = ProgressTracker::new(10, 0);
        run_with_simple(&mut t2, &setup)?;
        run_with_joint(&mut t2, &ccs).map_err(|e| format!("#{i} joint: {e}"))?;
        if t1.config != t2.config || t1.progress != t2.progress {
            return Err(format!(
                "#{i} setup: {} ccs: {}\nout1: {} {:?}\nout2: {} {:?}",
                describe(&setup),
                describe(&ccs),
                t1.config,
                t1.progress,
                t2.config,
                t2.progress
            ));
        }
    }
    Ok(())
}

fn restore_ok(cs: &ConfState) -> Result<(), String> {
    let t = ProgressTracker::new(20, 0);
    let (cfg, trk) = restore(
        Changer {
            tracker: &t,
            last_index: 10,
        },
        cs,
    )?;
    let mut t2 = t.clone();
    t2.config = cfg;
    t2.progress = trk;
    let mut cs = cs.clone();
    cs.voters.sort_unstable();
    cs.learners.sort_unstable();
    cs.voters_outgoing.sort_unstable();
    cs.learners_next.sort_unstable();
    let cs2 = t2.conf_state();
    if cs != cs2 {
        return Err(format!("{cs:?} != {cs2:?}"));
    }
    cs.equivalent(&cs2)?;
    cs2.equivalent(&cs)?;
    Ok(())
}

/// TestRestore: restoring a ConfState reproduces it.
fn test_restore() -> Result<(), String> {
    let ids = |s: &[u64]| s.to_vec();
    for cs in [
        ConfState::default(),
        ConfState {
            voters: ids(&[1, 2, 3]),
            ..Default::default()
        },
        ConfState {
            voters: ids(&[1, 2, 3]),
            learners: ids(&[4, 5, 6]),
            ..Default::default()
        },
        ConfState {
            voters: ids(&[1, 2, 3]),
            learners: ids(&[5]),
            voters_outgoing: ids(&[1, 2, 4, 6]),
            learners_next: ids(&[4]),
            auto_leave: false,
        },
    ] {
        restore_ok(&cs)?;
    }
    let mut rng = XorShift(0xD1B54A32D192ED03);
    for i in 0..1000 {
        let conv = |sl: &[usize]| sl.iter().map(|&v| v as u64 + 1).collect::<Vec<u64>>();
        let mut cs = ConfState::default();
        let n_voters = 1 + rng.intn(5);
        let n_learners = rng.intn(5);
        let n_removed_voters = rng.intn(3);
        let mut ids = conv(&rng.perm(2 * (n_voters + n_learners + n_removed_voters)));
        cs.voters = ids[..n_voters].to_vec();
        ids.drain(..n_voters);
        if n_learners > 0 {
            cs.learners = ids[..n_learners].to_vec();
            ids.drain(..n_learners);
        }
        let n_outgoing_retained = rng.intn(n_voters + 1);
        if n_outgoing_retained > 0 || n_removed_voters > 0 {
            cs.voters_outgoing = cs.voters[..n_outgoing_retained].to_vec();
            cs.voters_outgoing
                .extend_from_slice(&ids[..n_removed_voters]);
        }
        if n_removed_voters > 0 {
            let n_learners_next = rng.intn(n_removed_voters + 1);
            if n_learners_next > 0 {
                cs.learners_next = ids[..n_learners_next].to_vec();
            }
        }
        cs.auto_leave = !cs.voters_outgoing.is_empty() && rng.intn(2) == 1;
        restore_ok(&cs).map_err(|e| format!("#{i}: {e}"))?;
    }
    Ok(())
}

fn main() {
    let args: Vec<String> = std::env::args()
        .skip(1)
        .filter(|a| !a.starts_with("--"))
        .collect();
    let dir = args
        .first()
        .cloned()
        .unwrap_or_else(|| "../vendor/raft/confchange/testdata".to_string());
    let mut files: Vec<_> = std::fs::read_dir(&dir)
        .unwrap_or_else(|e| panic!("{dir}: {e}"))
        .map(|e| e.unwrap().path())
        .filter(|p| p.extension().is_some_and(|x| x == "txt"))
        .collect();
    files.sort();
    let mut fail = 0;
    for path in &files {
        let name = path.file_name().unwrap().to_string_lossy().to_string();
        let src = std::fs::read_to_string(path).unwrap();
        let mut env = Env {
            tracker: ProgressTracker::new(10, 0),
            last_index: 0,
        };
        let out = datadriven::rewrite(&name, &src, |d| env.handle(d));
        if out == src {
            println!(
                "[OK ] confchange {name}: byte-identical ({} lines)",
                src.lines().count()
            );
        } else {
            fail += 1;
            let first = src
                .lines()
                .zip(out.lines())
                .position(|(a, b)| a != b)
                .unwrap_or(0);
            println!(
                "[FAIL] confchange {name}: first difference at line {}",
                first + 1
            );
        }
    }
    for (name, r) in [
        ("TestConfChangeQuick", test_conf_change_quick()),
        ("TestRestore", test_restore()),
    ] {
        match r {
            Ok(()) => println!("[OK ] confchange {name}"),
            Err(e) => {
                fail += 1;
                println!("[FAIL] confchange {name}: {e}");
            }
        }
    }
    if fail > 0 {
        std::process::exit(1);
    }
}
