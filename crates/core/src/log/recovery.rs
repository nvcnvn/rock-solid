//! Recovery: load the newest manifest and its snapshots, then replay the records after it.
//!
//! Rounds are written one at a time, so at a crash only the newest record can be torn. Recovery
//! accepts records in LSN order from the manifest's base; a bad record with nothing after it was
//! the round in flight and is dropped, while a bad or missing record with later records after it
//! is corruption and fails recovery instead of silently cutting the log short. A scan that stops
//! at an invalid header probes the rest of the segment, so a damaged header cannot hide the
//! records behind it.

use std::collections::{BTreeMap, BTreeSet};

use super::commit::{OpenSegment, Resume};
use super::manifest::{BlobRef, Manifest};
use super::record::{self, BlobKind, Entry, Expected, RecordHeader};
use super::segment::{SegmentHeader, Superblock};
use super::{DecodeError, LogRecord, RecordPos, Stream};
use crate::buf::{BufPool, IoBuf, PoolExhausted};
use crate::event::{Action, BLOCK_BYTES, Context, DiskError, Event, IoId, Lba};

const PROBE_BLOCKS: u32 = 64;

/// What recovery reads.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum Mode {
    /// Load the newest manifest and its snapshots, replay the records after its base.
    Replay,
    /// Ignore manifests and return every record's header, to rebuild an index from scratch.
    Rebuild,
}

/// Reason recovery failed.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum RecoveryError {
    /// Block 0 holds no superblock of this format.
    NotFormatted(DecodeError),
    /// The record with this LSN is unreadable although later records exist, or the manifest is.
    Corrupt {
        /// LSN of the bad or missing record.
        lsn: u64,
    },
    /// A read failed.
    Disk(DiskError),
    /// The buffer pool cannot hold the next read.
    Pool(PoolExhausted),
}

/// Everything recovered from a disk's log.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Recovered {
    /// Each index's snapshot pieces from the manifest, in order.
    pub snapshots: Vec<(u64, Vec<Vec<u8>>)>,
    /// Records after the manifest's base in LSN order; in [`Mode::Rebuild`], every record, without blobs.
    pub records: Vec<LogRecord>,
    /// State for [`GroupCommit::resume`](super::commit::GroupCommit::resume).
    pub resume: Resume,
}

/// Progress of a recovery.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Step {
    /// Execute these reads and feed back their completions.
    Io(Vec<Action>),
    /// Recovery finished.
    Done(Result<Recovered, RecoveryError>),
}

struct Found {
    pos: RecordPos,
    header: RecordHeader,
    header_blocks: u32,
}

enum Stage {
    Superblock,
    Headers {
        index: u32,
    },
    Manifest {
        record_blocks: Option<u32>,
    },
    Snapshots {
        next: usize,
        record_blocks: Option<u32>,
    },
    Scan {
        item: usize,
        block: u32,
        header_blocks: Option<u32>,
    },
    Probe {
        item: usize,
        block: u32,
        blocks: u32,
    },
    Resolve {
        at: usize,
        candidate: usize,
    },
}

enum Flow {
    Next,
    Read(Lba, u32),
    Finish,
}

enum RecordRead {
    Need(Lba, u32),
    Got(RecordHeader, Vec<u8>),
}

/// Reads a disk's log back, one read at a time.
// ponytail: one read in flight; batch the segment header reads if startup time on 4096 segments matters.
pub struct Recovery {
    mode: Mode,
    next_io: u64,
    pending: Option<(IoId, IoBuf)>,
    finished: bool,
    stage: Stage,
    superblock: Option<Superblock>,
    segments: BTreeMap<u32, SegmentHeader>,
    manifest_ptr: Option<RecordPos>,
    manifest: Manifest,
    snapshot_refs: Vec<(usize, BlobRef)>,
    snapshots: Vec<(u64, Vec<Vec<u8>>)>,
    scan: Vec<(u32, u32)>,
    found: Vec<Found>,
    groups: BTreeMap<u64, Vec<usize>>,
    lsns: Vec<u64>,
    accepted: Vec<(usize, Vec<Vec<u8>>)>,
}

impl Recovery {
    /// Starts recovering the log on a disk; the first step reads the superblock.
    pub fn start(ctx: &mut Context<'_, impl BufPool>, mode: Mode, first_io: IoId) -> (Self, Step) {
        let mut recovery = Self {
            mode,
            next_io: first_io.0,
            pending: None,
            finished: false,
            stage: Stage::Superblock,
            superblock: None,
            segments: BTreeMap::new(),
            manifest_ptr: None,
            manifest: Manifest::default(),
            snapshot_refs: Vec::new(),
            snapshots: Vec::new(),
            scan: Vec::new(),
            found: Vec::new(),
            groups: BTreeMap::new(),
            lsns: Vec::new(),
            accepted: Vec::new(),
        };
        let step = recovery.drive(ctx, None);
        (recovery, step)
    }

    /// Feeds a read completion.
    #[allow(
        clippy::large_types_passed_by_value,
        reason = "sans-IO on_event takes Event by value"
    )]
    pub fn on_event(&mut self, ctx: &mut Context<'_, impl BufPool>, ev: Event) -> Step {
        let Event::DiskDone(io, result) = ev else {
            return Step::Io(Vec::new());
        };
        let Some((pending, buf)) = self.pending else {
            return Step::Io(Vec::new());
        };
        if pending != io || self.finished {
            return Step::Io(Vec::new());
        }
        self.pending = None;
        let bytes = ctx.pool.as_slice(buf).to_vec();
        ctx.pool.free(buf);
        if let Err(error) = result {
            return self.done(Err(RecoveryError::Disk(error)));
        }
        self.drive(ctx, Some(bytes))
    }

    fn drive(&mut self, ctx: &mut Context<'_, impl BufPool>, input: Option<Vec<u8>>) -> Step {
        match self.step(input) {
            Err(error) => self.done(Err(error)),
            Ok(None) => {
                let recovered = self.finish();
                self.done(Ok(recovered))
            }
            Ok(Some((lba, blocks))) => match ctx.pool.alloc(blocks as usize * BLOCK_BYTES) {
                Err(error) => self.done(Err(RecoveryError::Pool(error))),
                Ok(buf) => {
                    let io = IoId(self.next_io);
                    self.next_io += 1;
                    self.pending = Some((io, buf));
                    Step::Io(vec![Action::DiskRead(io, lba, buf)])
                }
            },
        }
    }

    fn done(&mut self, result: Result<Recovered, RecoveryError>) -> Step {
        self.finished = true;
        Step::Done(result)
    }

    fn superblock(&self) -> Superblock {
        self.superblock.expect("superblock read first")
    }

    fn lba(&self, segment: u32, block: u32) -> Lba {
        self.superblock().geometry.lba(segment, block)
    }

    fn expected(&self, segment: u32, block: u32) -> Option<Expected> {
        let header = self.segments.get(&segment)?;
        let segment_blocks = self.superblock().geometry.segment_blocks();
        Some(Expected {
            log_id: header.log_id,
            segment_seq: header.seq,
            offset: block,
            max_blocks: segment_blocks.checked_sub(block)?,
        })
    }

    /// Absorbs the previous read and returns the next one, or `None` once there is nothing left.
    fn step(&mut self, mut input: Option<Vec<u8>>) -> Result<Option<(Lba, u32)>, RecoveryError> {
        loop {
            let input = &mut input;
            let flow = match self.stage {
                Stage::Superblock => self.on_superblock(input)?,
                Stage::Headers { index } => self.on_segment_header(index, input),
                Stage::Manifest { record_blocks } => self.on_manifest(record_blocks, input)?,
                Stage::Snapshots {
                    next,
                    record_blocks,
                } => self.on_snapshot(next, record_blocks, input)?,
                Stage::Scan {
                    item,
                    block,
                    header_blocks,
                } => self.on_scan(item, block, header_blocks, input),
                Stage::Probe {
                    item,
                    block,
                    blocks,
                } => self.on_probe(item, block, blocks, input),
                Stage::Resolve { at, candidate } => self.on_resolve(at, candidate, input)?,
            };
            match flow {
                Flow::Next => {}
                Flow::Read(lba, blocks) => return Ok(Some((lba, blocks))),
                Flow::Finish => return Ok(None),
            }
        }
    }

    fn on_superblock(&mut self, input: &mut Option<Vec<u8>>) -> Result<Flow, RecoveryError> {
        let Some(bytes) = input.take() else {
            return Ok(Flow::Read(Lba(0), 1));
        };
        self.superblock = Some(Superblock::decode(&bytes).map_err(RecoveryError::NotFormatted)?);
        self.stage = Stage::Headers { index: 1 };
        Ok(Flow::Next)
    }

    fn on_segment_header(&mut self, index: u32, input: &mut Option<Vec<u8>>) -> Flow {
        let sb = self.superblock();
        if let Some(bytes) = input.take() {
            if let Ok(header) = SegmentHeader::decode(&bytes)
                && header.log_id == sb.log_id
                && header.index == index
            {
                self.segments.insert(index, header);
            }
            self.stage = Stage::Headers { index: index + 1 };
            return Flow::Next;
        }
        if index < sb.geometry.segment_count() {
            return Flow::Read(self.lba(index, 0), 1);
        }
        self.manifest_ptr = self
            .segments
            .values()
            .max_by_key(|h| h.seq)
            .and_then(|h| h.manifest);
        match (self.mode, self.manifest_ptr) {
            (Mode::Replay, Some(_)) => {
                self.stage = Stage::Manifest {
                    record_blocks: None,
                };
            }
            _ => self.begin_scan(),
        }
        Flow::Next
    }

    fn on_manifest(
        &mut self,
        record_blocks: Option<u32>,
        input: &mut Option<Vec<u8>>,
    ) -> Result<Flow, RecoveryError> {
        let ptr = self.manifest_ptr.expect("manifest stage needs a pointer");
        let corrupt = RecoveryError::Corrupt { lsn: ptr.lsn };
        let (header, data) = match self.read_record(ptr, record_blocks, input.take())? {
            RecordRead::Need(lba, blocks) => {
                self.stage = Stage::Manifest {
                    record_blocks: (blocks > 1).then_some(blocks),
                };
                return Ok(Flow::Read(lba, blocks));
            }
            RecordRead::Got(header, data) => (header, data),
        };
        let blob = header
            .entries
            .iter()
            .zip(header.blobs(&data))
            .find(|(e, _)| matches!(e, Entry::Blob { kind, .. } if *kind == BlobKind::MANIFEST))
            .map(|(_, blob)| blob)
            .ok_or(corrupt)?;
        self.manifest = Manifest::decode(&blob).map_err(|_| corrupt)?;
        self.snapshot_refs = self
            .manifest
            .trees
            .iter()
            .enumerate()
            .flat_map(|(t, (_, refs))| refs.iter().map(move |r| (t, *r)))
            .collect();
        self.snapshots = self
            .manifest
            .trees
            .iter()
            .map(|(owner, _)| (*owner, Vec::new()))
            .collect();
        self.stage = Stage::Snapshots {
            next: 0,
            record_blocks: None,
        };
        Ok(Flow::Next)
    }

    fn on_snapshot(
        &mut self,
        next: usize,
        record_blocks: Option<u32>,
        input: &mut Option<Vec<u8>>,
    ) -> Result<Flow, RecoveryError> {
        let Some(&(tree, blob_ref)) = self.snapshot_refs.get(next) else {
            self.begin_scan();
            return Ok(Flow::Next);
        };
        let (header, data) = match self.read_record(blob_ref.record, record_blocks, input.take())? {
            RecordRead::Need(lba, blocks) => {
                self.stage = Stage::Snapshots {
                    next,
                    record_blocks: (blocks > 1).then_some(blocks),
                };
                return Ok(Flow::Read(lba, blocks));
            }
            RecordRead::Got(header, data) => (header, data),
        };
        let entry = usize::from(blob_ref.entry);
        match header.entries.get(entry) {
            Some(Entry::Blob { kind, .. }) if *kind == BlobKind::SNAPSHOT => {}
            _ => {
                return Err(RecoveryError::Corrupt {
                    lsn: blob_ref.record.lsn,
                });
            }
        }
        let span = header.data_spans()[entry].clone();
        self.snapshots[tree].1.push(data[span].to_vec());
        self.stage = Stage::Snapshots {
            next: next + 1,
            record_blocks: None,
        };
        Ok(Flow::Next)
    }

    fn next_segment(&self, item: usize) -> Stage {
        Stage::Scan {
            item: item + 1,
            block: self.scan.get(item + 1).map_or(0, |s| s.1),
            header_blocks: None,
        }
    }

    fn on_scan(
        &mut self,
        item: usize,
        block: u32,
        header_blocks: Option<u32>,
        input: &mut Option<Vec<u8>>,
    ) -> Flow {
        let Some(&(segment, _)) = self.scan.get(item) else {
            self.begin_resolve();
            return Flow::Next;
        };
        let Some(expected) = self.expected(segment, block).filter(|e| e.max_blocks > 0) else {
            self.stage = self.next_segment(item);
            return Flow::Next;
        };
        let Some(bytes) = input.take() else {
            return Flow::Read(self.lba(segment, block), header_blocks.unwrap_or(1));
        };
        let probe = Stage::Probe {
            item,
            block: block + 1,
            blocks: PROBE_BLOCKS,
        };
        if header_blocks.is_none() {
            match record::peek(&bytes) {
                Ok((1, _)) => {}
                Ok((blocks, _)) if blocks <= expected.max_blocks => {
                    self.stage = Stage::Scan {
                        item,
                        block,
                        header_blocks: Some(blocks),
                    };
                    return Flow::Read(self.lba(segment, block), blocks);
                }
                _ => {
                    self.stage = probe;
                    return Flow::Next;
                }
            }
        }
        match record::decode_header(&bytes, &expected) {
            Ok(header) => self.found_at(item, segment, block, header),
            Err(_) => self.stage = probe,
        }
        Flow::Next
    }

    fn on_probe(
        &mut self,
        item: usize,
        block: u32,
        blocks: u32,
        input: &mut Option<Vec<u8>>,
    ) -> Flow {
        let (segment, _) = self.scan[item];
        let segment_blocks = self.superblock().geometry.segment_blocks();
        if block >= segment_blocks {
            self.stage = self.next_segment(item);
            return Flow::Next;
        }
        let n = blocks.min(segment_blocks - block);
        let Some(bytes) = input.take() else {
            return Flow::Read(self.lba(segment, block), n);
        };
        self.stage = Stage::Probe {
            item,
            block: block + n,
            blocks: PROBE_BLOCKS,
        };
        for i in 0..n {
            let at = &bytes[i as usize * BLOCK_BYTES..];
            let Ok((header_blocks, _)) = record::peek(at) else {
                continue;
            };
            if block + i + header_blocks > segment_blocks {
                continue;
            }
            if header_blocks > n - i {
                self.stage = Stage::Probe {
                    item,
                    block: block + i,
                    blocks: header_blocks.max(PROBE_BLOCKS),
                };
                break;
            }
            let header_bytes = &at[..header_blocks as usize * BLOCK_BYTES];
            if let Some(expected) = self.expected(segment, block + i)
                && let Ok(header) = record::decode_header(header_bytes, &expected)
            {
                self.found_at(item, segment, block + i, header);
                break;
            }
        }
        Flow::Next
    }

    fn on_resolve(
        &mut self,
        at: usize,
        candidate: usize,
        input: &mut Option<Vec<u8>>,
    ) -> Result<Flow, RecoveryError> {
        let Some(&lsn) = self.lsns.get(at) else {
            return Ok(Flow::Finish);
        };
        let expected_lsn = self
            .accepted
            .last()
            .map_or(self.manifest.base_lsn, |(f, _)| self.found[*f].pos.lsn)
            + 1;
        if self.mode == Mode::Replay && lsn != expected_lsn {
            return Err(RecoveryError::Corrupt { lsn: expected_lsn });
        }
        let candidates = &self.groups[&lsn];
        let is_last = at + 1 == self.lsns.len();
        if self.mode == Mode::Rebuild && candidates.len() == 1 && !is_last {
            self.accept(candidates[0], Vec::new());
            return Ok(Flow::Next);
        }
        let Some(&f) = candidates.get(candidate) else {
            return if is_last {
                Ok(Flow::Finish)
            } else {
                Err(RecoveryError::Corrupt { lsn })
            };
        };
        let found = &self.found[f];
        let data_blocks = found.header.layout().data_blocks;
        let data = match input.take() {
            Some(data) => data,
            None if data_blocks == 0 => Vec::new(),
            None => {
                let lba = self.lba(found.pos.segment, found.pos.block + found.header_blocks);
                return Ok(Flow::Read(lba, data_blocks));
            }
        };
        if found.header.verify_data(&data).is_ok() {
            let blobs = match self.mode {
                Mode::Replay => found.header.blobs(&data),
                Mode::Rebuild => Vec::new(),
            };
            self.accept(f, blobs);
        } else {
            self.stage = Stage::Resolve {
                at,
                candidate: candidate + 1,
            };
        }
        Ok(Flow::Next)
    }

    /// Reads the whole record at `pos`: first its first block to learn its size, then all of it.
    /// Only that first read asks for one block; `record_blocks` is the size of the second.
    fn read_record(
        &self,
        pos: RecordPos,
        record_blocks: Option<u32>,
        input: Option<Vec<u8>>,
    ) -> Result<RecordRead, RecoveryError> {
        let corrupt = RecoveryError::Corrupt { lsn: pos.lsn };
        let expected = self.expected(pos.segment, pos.block).ok_or(corrupt)?;
        let lba = self.lba(pos.segment, pos.block);
        let Some(bytes) = input else {
            return Ok(RecordRead::Need(lba, 1));
        };
        if record_blocks.is_none() {
            let (_, blocks) = record::peek(&bytes).map_err(|_| corrupt)?;
            if blocks > expected.max_blocks {
                return Err(corrupt);
            }
            if blocks > 1 {
                return Ok(RecordRead::Need(lba, blocks));
            }
        }
        let header = record::decode(&bytes, &expected).map_err(|_| corrupt)?;
        if header.lsn != pos.lsn {
            return Err(corrupt);
        }
        let data = bytes[header.layout().header_blocks as usize * BLOCK_BYTES..].to_vec();
        Ok(RecordRead::Got(header, data))
    }

    /// Records a valid header and continues scanning after its record.
    fn found_at(&mut self, item: usize, segment: u32, block: u32, header: RecordHeader) {
        let layout = header.layout();
        self.found.push(Found {
            pos: RecordPos {
                segment,
                block,
                lsn: header.lsn,
            },
            header,
            header_blocks: layout.header_blocks,
        });
        self.stage = Stage::Scan {
            item,
            block: block + layout.record_blocks(),
            header_blocks: None,
        };
    }

    fn begin_scan(&mut self) {
        let base_seq = match self.mode {
            Mode::Replay => self.manifest.base_seq,
            Mode::Rebuild => 0,
        };
        let mut scan: Vec<(u64, u32, u32)> = self
            .segments
            .values()
            .filter(|h| h.seq > base_seq)
            .map(|h| (h.seq, h.index, 1))
            .collect();
        if self.mode == Mode::Replay {
            for (segment, block) in self.manifest.starts.into_iter().flatten() {
                if let Some(h) = self.segments.get(&segment) {
                    scan.push((h.seq, segment, block));
                }
            }
        }
        scan.sort_unstable();
        self.scan = scan.into_iter().map(|(_, s, b)| (s, b)).collect();
        self.stage = Stage::Scan {
            item: 0,
            block: self.scan.first().map_or(0, |s| s.1),
            header_blocks: None,
        };
    }

    fn begin_resolve(&mut self) {
        let base = match self.mode {
            Mode::Replay => self.manifest.base_lsn,
            Mode::Rebuild => 0,
        };
        for (i, f) in self.found.iter().enumerate() {
            if f.pos.lsn > base {
                self.groups.entry(f.pos.lsn).or_default().push(i);
            }
        }
        self.lsns = self.groups.keys().copied().collect();
        self.stage = Stage::Resolve {
            at: 0,
            candidate: 0,
        };
    }

    fn accept(&mut self, found: usize, blobs: Vec<Vec<u8>>) {
        self.accepted.push((found, blobs));
        if let Stage::Resolve { at, .. } = self.stage {
            self.stage = Stage::Resolve {
                at: at + 1,
                candidate: 0,
            };
        }
    }

    fn finish(&mut self) -> Recovered {
        let sb = self.superblock();
        let geometry = sb.geometry;
        let mut ends: BTreeMap<u32, u32> = BTreeMap::new();
        for &(f, _) in &self.accepted {
            let found = &self.found[f];
            let end = found.pos.block + found.header.layout().record_blocks();
            let e = ends.entry(found.pos.segment).or_default();
            *e = (*e).max(end);
        }
        let scan_starts: BTreeMap<u32, u32> = self.scan.iter().copied().collect();
        let open = Stream::ALL.map(|stream| {
            let newest = self
                .segments
                .values()
                .filter(|h| h.stream == stream)
                .max_by_key(|h| h.seq)?;
            let next_block = ends
                .get(&newest.index)
                .or_else(|| scan_starts.get(&newest.index))
                .copied()
                .unwrap_or(geometry.segment_blocks());
            Some(OpenSegment {
                index: newest.index,
                seq: newest.seq,
                next_block,
            })
        });
        let used: BTreeSet<u32> = self.segments.keys().copied().collect();
        let base = match self.mode {
            Mode::Replay => self.manifest.base_lsn,
            Mode::Rebuild => 0,
        };
        let records: Vec<LogRecord> = core::mem::take(&mut self.accepted)
            .into_iter()
            .map(|(f, blobs)| {
                let found = &self.found[f];
                LogRecord {
                    pos: found.pos,
                    lba: geometry.lba(found.pos.segment, found.pos.block),
                    header: found.header.clone(),
                    blobs,
                }
            })
            .collect();
        let replay_bytes = records
            .iter()
            .filter(|r| r.pos.lsn > self.manifest.base_lsn)
            .map(|r| u64::from(r.header.layout().record_blocks()) * BLOCK_BYTES as u64)
            .sum();
        Recovered {
            snapshots: core::mem::take(&mut self.snapshots),
            resume: Resume {
                superblock: sb,
                open,
                free: (1..geometry.segment_count())
                    .filter(|s| !used.contains(s))
                    .collect(),
                next_seq: self.segments.values().map(|h| h.seq).max().unwrap_or(0) + 1,
                next_lsn: records.last().map_or(base, |r| r.pos.lsn) + 1,
                manifest: self.manifest_ptr,
                replay_bytes,
            },
            records,
        }
    }
}
