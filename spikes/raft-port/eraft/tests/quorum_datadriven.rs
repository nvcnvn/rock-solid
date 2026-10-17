// Copyright 2019, 2025 The etcd Authors
// Ported from go.etcd.io/raft v3.7.0 (b867cf13f6bc0dae21204302df97bc2355c3af55),
// quorum/datadriven_test.go, quorum/quick_test.go and quorum/majority_test.go.
// Licensed under the Apache License 2.0.
//
// Usage: cargo test --test quorum_datadriven -- [<testdata dir>]
// Each testdata file is rewritten and compared byte for byte (see
// tests/interaction.rs). TestQuick uses a seeded xorshift instead of
// testing/quick.

mod datadriven;

use std::collections::{BTreeMap, BTreeSet};
use std::fmt::Write as _;

use eraft::quorum::{
    AckedIndexer, Index, JointConfig, MajorityConfig, MapAckIndexer, index_string,
};

use datadriven::TestData;

fn go_map_set(s: &BTreeSet<u64>) -> String {
    let items: Vec<String> = s.iter().map(|id| format!("{id}:{{}}")).collect();
    format!("map[{}]", items.join(" "))
}

fn go_index_slice(s: &[Index]) -> String {
    let items: Vec<String> = s.iter().map(|i| index_string(*i)).collect();
    format!("[{}]", items.join(" "))
}

fn make_lookuper(idxs: &[Index], ids: &[u64], idsj: &[u64]) -> MapAckIndexer {
    let mut l = BTreeMap::new();
    let mut p = 0;
    for &id in ids.iter().chain(idsj) {
        if l.contains_key(&id) {
            continue;
        }
        if p < idxs.len() {
            l.insert(id, idxs[p]);
            p += 1;
        }
    }
    l.retain(|_, v| *v != 0);
    MapAckIndexer(l)
}

/// alternativeMajorityCommittedIndex from quick_test.go.
fn alternative_majority_committed_index(c: &MajorityConfig, l: &dyn AckedIndexer) -> Index {
    if c.is_empty() {
        return u64::MAX;
    }
    let mut id_to_idx = BTreeMap::new();
    for &id in c.iter() {
        if let Some(idx) = l.acked_index(id) {
            id_to_idx.insert(id, idx);
        }
    }
    let mut idx_to_votes: BTreeMap<Index, usize> = BTreeMap::new();
    for &idx in id_to_idx.values() {
        idx_to_votes.insert(idx, 0);
    }
    for &idx in id_to_idx.values() {
        for (idy, n) in idx_to_votes.iter_mut() {
            if *idy > idx {
                continue;
            }
            *n += 1;
        }
    }
    let q = c.len() / 2 + 1;
    let mut max_quorum_idx = 0;
    for (&idx, &n) in &idx_to_votes {
        if n >= q && idx > max_quorum_idx {
            max_quorum_idx = idx;
        }
    }
    max_quorum_idx
}

fn handle(d: &TestData) -> String {
    let mut joint = false;
    let (mut ids, mut idsj) = (Vec::new(), Vec::new());
    let (mut idxs, mut votes): (Vec<Index>, Vec<Index>) = (Vec::new(), Vec::new());
    for arg in &d.cmd_args {
        for v in &arg.vals {
            match arg.key.as_str() {
                "cfg" => ids.push(v.parse::<u64>().unwrap()),
                "cfgj" => {
                    joint = true;
                    if v == "zero" {
                        assert_eq!(1, arg.vals.len(), "cannot mix 'zero' into configuration");
                    } else {
                        idsj.push(v.parse::<u64>().unwrap());
                    }
                }
                "idx" => {
                    let mut n = 0;
                    if v != "_" {
                        n = v.parse().unwrap();
                        assert_ne!(0, n, "cannot use 0 as idx");
                    }
                    idxs.push(n);
                }
                "votes" => votes.push(match v.as_str() {
                    "y" => 2,
                    "n" => 1,
                    "_" => 0,
                    _ => panic!("unknown vote: {v}"),
                }),
                k => panic!("unknown arg {k}"),
            }
        }
    }
    let c = MajorityConfig(ids.iter().copied().collect());
    let cj = MajorityConfig(idsj.iter().copied().collect());
    {
        let input = if d.cmd == "vote" { &votes } else { &idxs };
        let voters = JointConfig([c.clone(), cj.clone()]).ids();
        if voters.len() != input.len() {
            return format!(
                "error: mismatched input (explicit or _) for voters {}: {}",
                go_map_set(&voters),
                go_index_slice(input)
            );
        }
    }
    let mut buf = String::new();
    match d.cmd.as_str() {
        "committed" => {
            let l = make_lookuper(&idxs, &ids, &idsj);
            if !joint {
                let idx = c.committed_index(&l);
                buf.push_str(&c.describe(&l));
                let a = alternative_majority_committed_index(&c, &l);
                if a != idx {
                    let _ = writeln!(buf, "{} <-- via alternative computation", index_string(a));
                }
                let a = JointConfig([c.clone(), MajorityConfig::default()]).committed_index(&l);
                if a != idx {
                    let _ = writeln!(buf, "{} <-- via zero-joint quorum", index_string(a));
                }
                let a = JointConfig([c.clone(), c.clone()]).committed_index(&l);
                if a != idx {
                    let _ = writeln!(buf, "{} <-- via self-joint quorum", index_string(a));
                }
                let overlay = |id: u64, idx: Index| {
                    let mut ll = BTreeMap::new();
                    for &iid in c.iter() {
                        if iid == id {
                            ll.insert(iid, idx);
                        } else if let Some(i) = l.acked_index(iid) {
                            ll.insert(iid, i);
                        }
                    }
                    MapAckIndexer(ll)
                };
                for &id in c.iter() {
                    let iidx = l.acked_index(id).unwrap_or(0);
                    if idx > iidx && iidx > 0 {
                        let a = c.committed_index(&overlay(id, iidx - 1));
                        if a != idx {
                            let _ =
                                write!(buf, "{} <-- overlaying {}->{}", index_string(a), id, iidx);
                        }
                        let a = c.committed_index(&overlay(id, 0));
                        if a != idx {
                            let _ = write!(buf, "{} <-- overlaying {}->0", index_string(a), id);
                        }
                    }
                }
                let _ = writeln!(buf, "{}", index_string(idx));
            } else {
                let cc = JointConfig([c.clone(), cj.clone()]);
                buf.push_str(&cc.describe(&l));
                let idx = cc.committed_index(&l);
                let a = JointConfig([cj.clone(), c.clone()]).committed_index(&l);
                if a != idx {
                    let _ = writeln!(buf, "{} <-- via symmetry", index_string(a));
                }
                let _ = writeln!(buf, "{}", index_string(idx));
            }
        }
        "vote" => {
            let ll = make_lookuper(&votes, &ids, &idsj);
            let l: BTreeMap<u64, bool> = ll.0.iter().map(|(&id, &v)| (id, v != 1)).collect();
            if !joint {
                let _ = writeln!(buf, "{}", c.vote_result(&l));
            } else {
                let r = JointConfig([c.clone(), cj.clone()]).vote_result(&l);
                let ar = JointConfig([cj.clone(), c.clone()]).vote_result(&l);
                if ar != r {
                    let _ = writeln!(buf, "{ar} <-- via symmetry");
                }
                let _ = writeln!(buf, "{r}");
            }
        }
        cmd => panic!("unknown command: {cmd}"),
    }
    buf
}

struct XorShift(u64);

impl XorShift {
    fn next(&mut self) -> u64 {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        self.0
    }
    fn intn(&mut self, n: u64) -> u64 {
        if n == 0 { 0 } else { self.next() % n }
    }
    /// smallRandIdxMap from quick_test.go.
    fn small_rand_idx_map(&mut self) -> BTreeMap<u64, Index> {
        let n = self.intn(10);
        let mut perm: Vec<u64> = (0..2 * n).collect();
        for i in (1..perm.len()).rev() {
            perm.swap(i, self.intn(i as u64 + 1) as usize);
        }
        perm.truncate(n as usize);
        perm.into_iter().map(|id| (id, self.intn(n))).collect()
    }
}

/// TestQuick: CommittedIndex agrees with the alternative computation.
fn test_quick() -> Result<(), String> {
    let mut rng = XorShift(0x2545F4914F6CDD1D);
    for i in 0..50_000 {
        let c = MajorityConfig(rng.small_rand_idx_map().into_keys().collect());
        let l = MapAckIndexer(rng.small_rand_idx_map());
        let (a, b) = (
            c.committed_index(&l),
            alternative_majority_committed_index(&c, &l),
        );
        if a != b {
            return Err(format!("#{i}: {a} != {b} for c={c:?} l={:?}", l.0));
        }
    }
    Ok(())
}

/// TestDescribe from majority_test.go.
fn test_describe() -> Result<(), String> {
    let c = MajorityConfig([1, 2, 3].into_iter().collect());
    let cases: [(&str, Vec<(u64, u64)>, &str); 4] = [
        (
            "different commit index for each voter",
            vec![(1, 101), (2, 102), (3, 103)],
            "       idx\n>      101    (id=1)\nx>     102    (id=2)\nxx>    103    (id=3)\n",
        ),
        (
            "same commit index for all voters",
            vec![(1, 100), (2, 100), (3, 100)],
            "       idx\n>      100    (id=1)\n>      100    (id=2)\n>      100    (id=3)\n",
        ),
        (
            "same commit index for part of the voters",
            vec![(1, 100), (2, 102), (3, 102)],
            "       idx\n>      100    (id=1)\nx>     102    (id=2)\nx>     102    (id=3)\n",
        ),
        (
            "missing some commit indexes",
            vec![(1, 101), (3, 103)],
            "       idx\nx>     101    (id=1)\n?        0    (id=2)\nxx>    103    (id=3)\n",
        ),
    ];
    for (name, idx, want) in cases {
        let got = c.describe(&MapAckIndexer(idx.into_iter().collect()));
        if got != want {
            return Err(format!("{name}: got {got:?} want {want:?}"));
        }
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
        .unwrap_or_else(|| "../vendor/raft/quorum/testdata".to_string());
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
        let out = datadriven::rewrite(&name, &src, handle);
        if out == src {
            println!(
                "[OK ] quorum {name}: byte-identical ({} lines)",
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
                "[FAIL] quorum {name}: first difference at line {}",
                first + 1
            );
        }
    }
    for (name, r) in [
        ("TestQuick", test_quick()),
        ("TestDescribe", test_describe()),
    ] {
        match r {
            Ok(()) => println!("[OK ] quorum {name}"),
            Err(e) => {
                fail += 1;
                println!("[FAIL] quorum {name}: {e}");
            }
        }
    }
    if fail > 0 {
        std::process::exit(1);
    }
}
