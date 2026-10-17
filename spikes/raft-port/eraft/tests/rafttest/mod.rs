// Copyright 2019, 2021, 2022, 2023 The etcd Authors
// Ported from go.etcd.io/raft v3.7.0 (b867cf13f6bc0dae21204302df97bc2355c3af55),
// rafttest/interaction_env*.go. Licensed under the Apache License 2.0.
//
// Deviations: Go's datadriven t.Fatal calls become panics; the shared
// strings.Builder becomes an Rc<RefCell<..>> that the per-node loggers write to.

#![allow(
    dead_code,
    reason = "shared test module; not every test binary uses every item"
)]

use std::cell::RefCell;
use std::fmt;
use std::rc::Rc;

use eraft::logger::{Level, Logger};
use eraft::raftpb::{
    ConfChange, ConfChangeI, ConfChangeTransition, ConfChangeV2, Entry, EntryType, HardState,
    Message, MessageType, Snapshot, conf_changes_from_string,
};
use eraft::tracker::progress_map_string;
use eraft::util::{describe_entries, describe_message, describe_ready, is_local_msg_target};
use eraft::{
    Config, Error, MemoryStorage, RawNode, ReadOnlyOption, Storage, is_empty_hard_state,
    is_empty_snap,
};

use crate::datadriven::TestData;

const LVL_NAMES: [&str; 6] = ["DEBUG", "INFO", "WARN", "ERROR", "FATAL", "NONE"];

#[derive(Default)]
pub struct OutputState {
    pub builder: String,
    pub lvl: usize,
}

/// RedirectLogger: writes log lines into the shared output buffer.
#[derive(Clone, Default)]
pub struct Output(pub Rc<RefCell<OutputState>>);

impl Output {
    fn quiet(&self) -> bool {
        self.0.borrow().lvl == LVL_NAMES.len() - 1
    }
    pub fn write_str(&self, s: &str) {
        if self.quiet() {
            return;
        }
        self.0.borrow_mut().builder.push_str(s);
    }
    fn len(&self) -> usize {
        self.0.borrow().builder.len()
    }
    fn take(&self) -> String {
        std::mem::take(&mut self.0.borrow_mut().builder)
    }
    fn reset(&self) {
        self.0.borrow_mut().builder.clear();
    }
}

impl Logger for Output {
    fn enabled(&self, level: Level) -> bool {
        self.0.borrow().lvl <= level as usize
    }
    fn log(&self, level: Level, args: fmt::Arguments<'_>) {
        if !self.enabled(level) {
            return;
        }
        let msg = args.to_string();
        let mut st = self.0.borrow_mut();
        st.builder.push_str(LVL_NAMES[level as usize]);
        st.builder.push(' ');
        st.builder.push_str(&msg);
        if !msg.ends_with('\n') {
            st.builder.push('\n');
        }
    }
}

/// MemoryStorage whose Snapshot() returns the node's latest history entry.
pub struct SnapOverrideStorage {
    pub inner: MemoryStorage,
    pub history: Rc<RefCell<Vec<Snapshot>>>,
}

impl Storage for SnapOverrideStorage {
    fn initial_state(&self) -> Result<(HardState, eraft::raftpb::ConfState), Error> {
        self.inner.initial_state()
    }
    fn entries(&self, lo: u64, hi: u64, max_size: u64) -> Result<Vec<Entry>, Error> {
        self.inner.entries(lo, hi, max_size)
    }
    fn term(&self, i: u64) -> Result<u64, Error> {
        self.inner.term(i)
    }
    fn last_index(&self) -> Result<u64, Error> {
        self.inner.last_index()
    }
    fn first_index(&self) -> Result<u64, Error> {
        self.inner.first_index()
    }
    fn snapshot(&self) -> Result<Snapshot, Error> {
        Ok(self.history.borrow().last().cloned().unwrap_or_default())
    }
}

pub struct Node {
    pub rn: RawNode<SnapOverrideStorage>,
    pub config: Config,
    pub append_work: Vec<Message>,
    pub apply_work: Vec<Message>,
    pub history: Rc<RefCell<Vec<Snapshot>>>,
}

pub struct InteractionEnv {
    pub nodes: Vec<Node>,
    pub messages: Vec<Message>,
    pub output: Output,
}

fn fmt_entry(b: &[u8]) -> String {
    eraft::util::go_quote(b)
}

fn default_entry_formatter() -> &'static dyn Fn(&[u8]) -> String {
    &fmt_entry
}

fn raft_config_stub() -> Config {
    Config {
        election_tick: 3,
        heartbeat_tick: 1,
        max_size_per_msg: u64::MAX,
        max_inflight_msgs: i32::MAX as usize,
        ..Default::default()
    }
}

fn parse_bool(s: &str) -> bool {
    match s {
        "1" | "t" | "T" | "TRUE" | "true" | "True" => true,
        "0" | "f" | "F" | "FALSE" | "false" | "False" => false,
        _ => panic!("strconv.ParseBool: parsing {s:?}: invalid syntax"),
    }
}

fn first_as_int(d: &TestData) -> usize {
    d.cmd_args[0]
        .key
        .parse()
        .unwrap_or_else(|e| panic!("{}: {e}", d.pos))
}

fn first_as_node_idx(d: &TestData) -> usize {
    first_as_int(d) - 1
}

fn node_idxs(d: &TestData) -> Vec<usize> {
    d.cmd_args
        .iter()
        .filter(|a| a.vals.is_empty())
        .map(|a| {
            a.key
                .parse::<usize>()
                .unwrap_or_else(|e| panic!("{}: {e}", d.pos))
                - 1
        })
        .collect()
}

impl InteractionEnv {
    pub fn new() -> InteractionEnv {
        InteractionEnv {
            nodes: Vec::new(),
            messages: Vec::new(),
            output: Output::default(),
        }
    }

    fn with_indent(
        &mut self,
        f: impl FnOnce(&mut Self) -> Result<(), String>,
    ) -> Result<(), String> {
        let orig = self.output.take();
        let res = f(self);
        let inner = self.output.take();
        let mut st = self.output.0.borrow_mut();
        st.builder = orig;
        for l in inner.lines() {
            st.builder.push_str("  ");
            st.builder.push_str(l);
            st.builder.push('\n');
        }
        res
    }

    pub fn handle(&mut self, d: &TestData) -> String {
        self.output.reset();
        let res: Result<(), String> = match d.cmd.as_str() {
            "_breakpoint" => Ok(()),
            "add-nodes" => self.handle_add_nodes(d),
            "campaign" => self.nodes[first_as_node_idx(d)]
                .rn
                .campaign()
                .map_err(|e| e.to_string()),
            "compact" => self.handle_compact(d),
            "deliver-msgs" => self.handle_deliver_msgs(d),
            "process-ready" => self.for_each_idx(d, "handling Ready", Self::process_ready),
            "process-append-thread" => {
                self.for_each_idx(d, "processing append thread", Self::process_append_thread)
            }
            "process-apply-thread" => {
                self.for_each_idx(d, "processing apply thread", Self::process_apply_thread)
            }
            "log-level" => self.log_level(&d.cmd_args[0].key),
            "raft-log" => self.raft_log(first_as_node_idx(d)),
            "raft-state" => self.handle_raft_state(),
            "set-randomized-election-timeout" => {
                let idx = first_as_node_idx(d);
                let timeout: usize = d.arg("timeout").expect("timeout").vals[0].parse().unwrap();
                assert!(timeout != 0);
                self.nodes[idx].rn.set_randomized_election_timeout(timeout);
                Ok(())
            }
            "stabilize" => self.handle_stabilize(d),
            "status" => {
                let st = self.nodes[first_as_node_idx(d)].rn.status();
                self.output.write_str(&progress_map_string(&st.progress));
                Ok(())
            }
            "tick-election" => {
                let idx = first_as_node_idx(d);
                let n = self.nodes[idx].config.election_tick;
                self.tick(idx, n)
            }
            "tick-heartbeat" => {
                let idx = first_as_node_idx(d);
                let n = self.nodes[idx].config.heartbeat_tick;
                self.tick(idx, n)
            }
            "transfer-leadership" => {
                let from: u64 = d.arg("from").expect("from").vals[0].parse().unwrap();
                let to: u64 = d.arg("to").expect("to").vals[0].parse().unwrap();
                assert!(
                    from != 0 && from as usize <= self.nodes.len(),
                    "expected valid \"from\" argument"
                );
                assert!(
                    to != 0 && to as usize <= self.nodes.len(),
                    "expected valid \"to\" argument"
                );
                self.nodes[(from - 1) as usize].rn.transfer_leader(to);
                Ok(())
            }
            "forget-leader" => {
                let _ = self.nodes[first_as_node_idx(d)].rn.forget_leader();
                Ok(())
            }
            "send-snapshot" => self.handle_send_snapshot(d),
            "propose" => {
                let idx = first_as_node_idx(d);
                assert!(
                    d.cmd_args.len() == 2 && d.cmd_args[1].vals.is_empty(),
                    "expected exactly one key with no vals"
                );
                self.nodes[idx]
                    .rn
                    .propose(d.cmd_args[1].key.as_bytes().to_vec())
                    .map_err(|e| e.to_string())
            }
            "propose-conf-change" => self.handle_propose_conf_change(d),
            "report-unreachable" => {
                let sl = node_idxs(d);
                if sl.len() != 2 {
                    Err("must specify exactly two node indexes: node on which to report, and reported node".into())
                } else {
                    let id = self.nodes[sl[1]].config.id;
                    self.nodes[sl[0]].rn.report_unreachable(id);
                    Ok(())
                }
            }
            _ => Err("unknown command".into()),
        };
        if let Err(e) = res {
            if self.output.quiet() {
                return e;
            }
            self.output.write_str(&e);
        }
        if self.output.len() == 0 {
            return "ok".to_string();
        }
        self.output.take()
    }

    fn for_each_idx(
        &mut self,
        d: &TestData,
        what: &str,
        f: fn(&mut Self, usize) -> Result<(), String>,
    ) -> Result<(), String> {
        let idxs = node_idxs(d);
        let multi = idxs.len() > 1;
        for idx in idxs {
            if multi {
                self.output.write_str(&format!("> {} {}\n", idx + 1, what));
                self.with_indent(|env| f(env, idx))?;
            } else {
                f(self, idx)?;
            }
        }
        Ok(())
    }

    fn handle_add_nodes(&mut self, d: &TestData) -> Result<(), String> {
        let n = first_as_int(d);
        let mut snap = Snapshot::default();
        let mut cfg = raft_config_stub();
        for arg in &d.cmd_args[1..] {
            for v in &arg.vals {
                match arg.key.as_str() {
                    "voters" => snap.metadata.conf_state.voters.push(v.parse().unwrap()),
                    "learners" => snap.metadata.conf_state.learners.push(v.parse().unwrap()),
                    "inflight" => cfg.max_inflight_msgs = v.parse().unwrap(),
                    "index" => {
                        snap.metadata.index = v.parse().unwrap();
                        cfg.applied = snap.metadata.index;
                    }
                    "content" => panic!("unsupported type *[]uint8 for destination #1"),
                    "async-storage-writes" => cfg.async_storage_writes = parse_bool(v),
                    "prevote" => cfg.pre_vote = parse_bool(v),
                    "checkquorum" => cfg.check_quorum = parse_bool(v),
                    "max-committed-size-per-ready" => {
                        cfg.max_committed_size_per_ready = v.parse().unwrap()
                    }
                    "disable-conf-change-validation" => {
                        cfg.disable_conf_change_validation = parse_bool(v)
                    }
                    "read-only" => match v.as_str() {
                        "safe" => cfg.read_only_option = ReadOnlyOption::ReadOnlySafe,
                        "lease-based" => cfg.read_only_option = ReadOnlyOption::ReadOnlyLeaseBased,
                        _ => return Err(format!("invalid read-only option {:?}", v)),
                    },
                    "step-down-on-removal" => cfg.step_down_on_removal = parse_bool(v),
                    _ => {}
                }
            }
        }
        self.add_nodes(n, cfg, snap)
    }

    pub fn add_nodes(&mut self, n: usize, cfg: Config, mut snap: Snapshot) -> Result<(), String> {
        let bootstrap = snap != Snapshot::default();
        for _ in 0..n {
            let id = 1 + self.nodes.len() as u64;
            let history = Rc::new(RefCell::new(Vec::new()));
            let mut s = SnapOverrideStorage {
                inner: MemoryStorage::new(),
                history: history.clone(),
            };
            if bootstrap {
                if snap.metadata.index <= 1 {
                    return Err("index must be specified as > 1 due to bootstrap".into());
                }
                snap.metadata.term = 1;
                s.inner
                    .apply_snapshot(snap.clone())
                    .map_err(|e| e.to_string())?;
                let fi = s.inner.first_index().map_err(|e| e.to_string())?;
                let exp = snap.metadata.index + 1;
                if fi != exp {
                    return Err(format!("failed to establish first index {exp}; got {fi}"));
                }
            }
            history.borrow_mut().push(snap.clone());
            let mut cfg = cfg.clone();
            cfg.id = id;
            cfg.logger = Some(Rc::new(self.output.clone()));
            let rn = RawNode::new(&mut cfg, s).map_err(|e| e.to_string())?;
            self.nodes.push(Node {
                rn,
                config: cfg,
                append_work: Vec::new(),
                apply_work: Vec::new(),
                history,
            });
        }
        Ok(())
    }

    fn handle_compact(&mut self, d: &TestData) -> Result<(), String> {
        let idx = first_as_node_idx(d);
        let new_first_index: u64 = d.cmd_args[1].key.parse().map_err(|e| format!("{e}"))?;
        self.nodes[idx]
            .rn
            .store_mut()
            .inner
            .compact(new_first_index)
            .map_err(|e| e.to_string())?;
        self.raft_log(idx)
    }

    fn handle_deliver_msgs(&mut self, d: &TestData) -> Result<(), String> {
        let mut typ: Option<MessageType> = None;
        let mut rs: Vec<(u64, bool)> = Vec::new();
        for arg in &d.cmd_args {
            if arg.vals.is_empty() {
                rs.push((arg.key.parse().unwrap(), false));
            }
            for v in &arg.vals {
                match arg.key.as_str() {
                    "drop" => {
                        let id: u64 = v.parse().unwrap();
                        assert!(
                            !rs.iter().any(|r| r.0 == id),
                            "can't both deliver and drop msgs to {id}"
                        );
                        rs.push((id, true));
                    }
                    "type" => {
                        typ = Some(
                            MessageType::from_name(v)
                                .unwrap_or_else(|| panic!("unknown message type {v}")),
                        );
                    }
                    _ => {}
                }
            }
        }
        if self.deliver_msgs(typ, &rs) == 0 {
            self.output.write_str("no messages\n");
        }
        Ok(())
    }

    pub fn deliver_msgs(&mut self, typ: Option<MessageType>, rs: &[(u64, bool)]) -> usize {
        let mut n = 0;
        for &(id, drop) in rs {
            let (msgs, rest) = split_msgs(std::mem::take(&mut self.messages), id, typ, drop);
            self.messages = rest;
            n += msgs.len();
            for msg in msgs {
                if drop {
                    self.output.write_str("dropped: ");
                }
                self.output
                    .write_str(&describe_message(&msg, Some(default_entry_formatter())));
                self.output.write_str("\n");
                if drop {
                    continue;
                }
                let to_idx = (msg.to - 1) as usize;
                if let Err(e) = self.nodes[to_idx].rn.step(msg) {
                    self.output.write_str(&format!("{e}\n"));
                }
            }
        }
        n
    }

    fn process_ready(&mut self, idx: usize) -> Result<(), String> {
        let rd = self.nodes[idx].rn.ready();
        self.output
            .write_str(&describe_ready(&rd, Some(default_entry_formatter())));
        let async_writes = self.nodes[idx].config.async_storage_writes;
        if !async_writes {
            process_append(
                &mut self.nodes[idx],
                rd.hard_state.as_ref(),
                &rd.entries,
                rd.snapshot.as_ref(),
            )?;
            process_apply(&mut self.nodes[idx], &rd.committed_entries)?;
        }
        let mut rd = rd;
        for m in std::mem::take(&mut rd.messages) {
            if is_local_msg_target(m.to) {
                assert!(async_writes, "unexpected local msg target");
                match m.msg_type {
                    MessageType::MsgStorageAppend => self.nodes[idx].append_work.push(m),
                    MessageType::MsgStorageApply => self.nodes[idx].apply_work.push(m),
                    t => panic!("unexpected message type {t}"),
                }
            } else {
                self.messages.push(m);
            }
        }
        if !async_writes {
            self.nodes[idx].rn.advance(rd);
        }
        Ok(())
    }

    fn process_append_thread(&mut self, idx: usize) -> Result<(), String> {
        let n = &mut self.nodes[idx];
        if n.append_work.is_empty() {
            self.output.write_str("no append work to perform");
            return Ok(());
        }
        let mut m = n.append_work.remove(0);
        let resps = std::mem::take(&mut m.responses);
        self.output.write_str("Processing:\n");
        self.output
            .write_str(&(describe_message(&m, Some(default_entry_formatter())) + "\n"));
        let st = HardState {
            term: m.term,
            vote: m.vote,
            commit: m.commit,
        };
        let snap = m.snapshot.as_deref().cloned().unwrap_or_default();
        process_append(&mut self.nodes[idx], Some(&st), &m.entries, Some(&snap))?;
        self.output.write_str("Responses:\n");
        for r in &resps {
            self.output
                .write_str(&(describe_message(r, Some(default_entry_formatter())) + "\n"));
        }
        self.messages.extend(resps);
        Ok(())
    }

    fn process_apply_thread(&mut self, idx: usize) -> Result<(), String> {
        let n = &mut self.nodes[idx];
        if n.apply_work.is_empty() {
            self.output.write_str("no apply work to perform");
            return Ok(());
        }
        let mut m = n.apply_work.remove(0);
        let resps = std::mem::take(&mut m.responses);
        self.output.write_str("Processing:\n");
        self.output
            .write_str(&(describe_message(&m, Some(default_entry_formatter())) + "\n"));
        process_apply(&mut self.nodes[idx], &m.entries)?;
        self.output.write_str("Responses:\n");
        for r in &resps {
            self.output
                .write_str(&(describe_message(r, Some(default_entry_formatter())) + "\n"));
        }
        self.messages.extend(resps);
        Ok(())
    }

    fn log_level(&mut self, name: &str) -> Result<(), String> {
        for (i, s) in LVL_NAMES.iter().enumerate() {
            if s.eq_ignore_ascii_case(name) {
                self.output.0.borrow_mut().lvl = i;
                return Ok(());
            }
        }
        Err(format!(
            "log levels must be either of [{}]",
            LVL_NAMES.join(" ")
        ))
    }

    fn raft_log(&mut self, idx: usize) -> Result<(), String> {
        let s = &self.nodes[idx].rn.store().inner;
        let fi = s.first_index().map_err(|e| e.to_string())?;
        let li = s.last_index().map_err(|e| e.to_string())?;
        if li < fi {
            self.output
                .write_str(&format!("log is empty: first index={fi}, last index={li}"));
            return Ok(());
        }
        let ents = s.entries(fi, li + 1, u64::MAX).map_err(|e| e.to_string())?;
        let out = describe_entries(&ents, Some(default_entry_formatter()));
        self.output.write_str(&out);
        Ok(())
    }

    fn handle_raft_state(&mut self) -> Result<(), String> {
        let mut out = String::new();
        for n in &self.nodes {
            let st = n.rn.status();
            let voter = if st.config.voters.ids().contains(&st.basic.id) {
                "(Voter)"
            } else {
                "(Non-Voter)"
            };
            out.push_str(&format!(
                "{}: {} {} Term:{} Lead:{}\n",
                st.basic.id,
                st.basic.soft_state.raft_state,
                voter,
                st.basic.hard_state.term,
                st.basic.soft_state.lead
            ));
        }
        self.output.write_str(&out);
        Ok(())
    }

    fn handle_stabilize(&mut self, d: &TestData) -> Result<(), String> {
        let idxs = node_idxs(d);
        let mut restore = None;
        for arg in &d.cmd_args {
            for v in &arg.vals {
                if arg.key == "log-level" {
                    if restore.is_none() {
                        restore = Some(self.output.0.borrow().lvl);
                    }
                    if let Err(e) = self.log_level(v) {
                        if let Some(old) = restore {
                            self.output.0.borrow_mut().lvl = old;
                        }
                        return Err(e);
                    }
                }
            }
        }
        let res = self.stabilize(&idxs);
        if let Some(old) = restore {
            self.output.0.borrow_mut().lvl = old;
        }
        res
    }

    pub fn stabilize(&mut self, idxs: &[usize]) -> Result<(), String> {
        let nodes: Vec<usize> = if idxs.is_empty() {
            (0..self.nodes.len()).collect()
        } else {
            idxs.to_vec()
        };
        loop {
            let mut done = true;
            for &i in &nodes {
                if self.nodes[i].rn.has_ready() {
                    let idx = (self.nodes[i].rn.status().basic.id - 1) as usize;
                    self.output
                        .write_str(&format!("> {} handling Ready\n", idx + 1));
                    self.with_indent(|env| env.process_ready(idx))?;
                    done = false;
                }
            }
            for &i in &nodes {
                let id = self.nodes[i].rn.status().basic.id;
                if self
                    .messages
                    .iter()
                    .any(|m| msg_matches(m, id, None, false))
                {
                    self.output
                        .write_str(&format!("> {id} receiving messages\n"));
                    self.with_indent(|env| {
                        env.deliver_msgs(None, &[(id, false)]);
                        Ok(())
                    })?;
                    done = false;
                }
            }
            for &i in &nodes {
                let idx = (self.nodes[i].rn.status().basic.id - 1) as usize;
                if !self.nodes[i].append_work.is_empty() {
                    self.output
                        .write_str(&format!("> {} processing append thread\n", idx + 1));
                    while !self.nodes[i].append_work.is_empty() {
                        self.with_indent(|env| env.process_append_thread(idx))?;
                    }
                    done = false;
                }
            }
            for &i in &nodes {
                let idx = (self.nodes[i].rn.status().basic.id - 1) as usize;
                if !self.nodes[i].apply_work.is_empty() {
                    self.output
                        .write_str(&format!("> {} processing apply thread\n", idx + 1));
                    while !self.nodes[i].apply_work.is_empty() {
                        let _ = self.with_indent(|env| env.process_apply_thread(idx));
                    }
                    done = false;
                }
            }
            if done {
                return Ok(());
            }
        }
    }

    fn handle_send_snapshot(&mut self, d: &TestData) -> Result<(), String> {
        let idxs = node_idxs(d);
        assert_eq!(idxs.len(), 2);
        let (from_idx, to_idx) = (idxs[0], idxs[1]);
        let snap = self.nodes[from_idx]
            .rn
            .store()
            .snapshot()
            .map_err(|e| e.to_string())?;
        let (from, to) = (from_idx as u64 + 1, to_idx as u64 + 1);
        let term = self.nodes[from_idx].rn.basic_status().hard_state.term;
        let msg = Message {
            msg_type: MessageType::MsgSnap,
            term,
            from,
            to,
            snapshot: Some(Box::new(snap)),
            ..Default::default()
        };
        self.output.write_str(&describe_message(&msg, None));
        self.messages.push(msg);
        Ok(())
    }

    fn handle_propose_conf_change(&mut self, d: &TestData) -> Result<(), String> {
        let idx = first_as_node_idx(d);
        let mut v1 = false;
        let mut transition = ConfChangeTransition::ConfChangeTransitionAuto;
        for arg in &d.cmd_args[1..] {
            for val in &arg.vals {
                match arg.key.as_str() {
                    "v1" => v1 = parse_bool(val),
                    "transition" => {
                        transition = match val.as_str() {
                            "auto" => ConfChangeTransition::ConfChangeTransitionAuto,
                            "implicit" => ConfChangeTransition::ConfChangeTransitionJointImplicit,
                            "explicit" => ConfChangeTransition::ConfChangeTransitionJointExplicit,
                            _ => return Err(format!("unknown transition {val}")),
                        }
                    }
                    k => return Err(format!("unknown command {k}")),
                }
            }
        }
        let ccs = conf_changes_from_string(&d.input)?;
        let c: ConfChangeI = if v1 {
            if ccs.len() > 1 || transition != ConfChangeTransition::ConfChangeTransitionAuto {
                return Err("v1 conf change can only have one operation and no transition".into());
            }
            ConfChange {
                change_type: ccs[0].change_type,
                node_id: ccs[0].node_id,
                ..Default::default()
            }
            .into()
        } else {
            ConfChangeV2 {
                transition,
                changes: ccs,
                context: Vec::new(),
            }
            .into()
        };
        self.nodes[idx]
            .rn
            .propose_conf_change(c)
            .map_err(|e| e.to_string())
    }

    fn tick(&mut self, idx: usize, n: usize) -> Result<(), String> {
        for _ in 0..n {
            self.nodes[idx].rn.tick();
        }
        Ok(())
    }
}

fn process_append(
    n: &mut Node,
    st: Option<&HardState>,
    ents: &[Entry],
    snap: Option<&Snapshot>,
) -> Result<(), String> {
    let s = &mut n.rn.store_mut().inner;
    if let Some(st) = st {
        if !is_empty_hard_state(Some(st)) {
            s.set_hard_state(*st).map_err(|e| e.to_string())?;
        }
    }
    if !is_empty_snap(snap) {
        if !ents.is_empty() {
            return Err("can't apply snapshot and entries at the same time".into());
        }
        return s
            .apply_snapshot(snap.cloned().unwrap_or_default())
            .map_err(|e| e.to_string());
    }
    s.append(ents).map_err(|e| e.to_string())
}

fn process_apply(n: &mut Node, ents: &[Entry]) -> Result<(), String> {
    for ent in ents {
        let (update, cs) = match ent.entry_type {
            EntryType::EntryConfChange => {
                let cc = ConfChange::decode(&ent.data).map_err(|e| e.to_string())?;
                let update = cc.context.clone();
                let cs = n.rn.apply_conf_change(&ConfChangeI::V1(cc));
                (update, Some(cs))
            }
            EntryType::EntryConfChangeV2 => {
                let cc = ConfChangeV2::decode(&ent.data).map_err(|e| e.to_string())?;
                let update = cc.context.clone();
                let cs = n.rn.apply_conf_change(&ConfChangeI::V2(cc));
                (update, Some(cs))
            }
            EntryType::EntryNormal => (ent.data.clone(), None),
        };
        let mut hist = n.history.borrow_mut();
        let last = hist.last().cloned().unwrap_or_default();
        let mut snap = Snapshot::default();
        snap.data.extend_from_slice(&last.data);
        snap.data.extend_from_slice(&update);
        snap.metadata.index = ent.index;
        snap.metadata.term = ent.term;
        snap.metadata.conf_state = cs.unwrap_or(last.metadata.conf_state);
        hist.push(snap);
    }
    Ok(())
}

fn msg_matches(msg: &Message, to: u64, typ: Option<MessageType>, drop: bool) -> bool {
    msg.to == to && !(drop && is_local(msg)) && typ.is_none_or(|t| msg.msg_type == t)
}

fn split_msgs(
    msgs: Vec<Message>,
    to: u64,
    typ: Option<MessageType>,
    drop: bool,
) -> (Vec<Message>, Vec<Message>) {
    msgs.into_iter()
        .partition(|m| msg_matches(m, to, typ, drop))
}

fn is_local(msg: &Message) -> bool {
    msg.from == msg.to || is_local_msg_target(msg.from) || is_local_msg_target(msg.to)
}
