//! Group commit: each round appends one record to one stream's open segment with one write, then
//! flushes once; items from any number of producers that arrive during a round share the next one.

use std::collections::{BTreeMap, BTreeSet, VecDeque};

use super::manifest::{BlobRef, Manifest};
use super::record::{self, BlobKind, Entry, Extent, Layout, LayoutBuilder, RecordHeader};
use super::segment::{SegmentHeader, Superblock};
use super::{LogRecord, RecordPos, Stream};
use crate::buf::{BufPool, IoBuf, PoolExhausted};
use crate::event::{Action, BLOCK_BYTES, Context, DiskError, Event, IoId, Lba, TimerId};

/// Entries one record may hold, so a [`BlobRef`] can name any of them.
const MAX_ENTRIES: usize = u16::MAX as usize + 1;

/// Group commit settings.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct Config {
    /// Largest record (`max_group_commit_bytes`); bounds how long a big write delays a metadata commit.
    pub max_record_bytes: usize,
    /// Bytes of records after the last manifest that make a checkpoint due (`max_replay_bytes`).
    pub max_replay_bytes: u64,
    /// Timer that retries a round after the buffer pool ran out.
    pub retry_timer: TimerId,
    /// Ticks to wait before that retry.
    pub retry_delay: u64,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            max_record_bytes: 1 << 20,
            max_replay_bytes: 64 << 20,
            retry_timer: TimerId(0),
            retry_delay: 1_000,
        }
    }
}

/// Identifies a submitted item in [`LogEvent`]s.
#[repr(transparent)]
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Ticket(pub u64);

/// Something to append to the log.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Item {
    /// Chunk blocks; `data` holds exactly `extent.blocks` blocks and is freed by the log once copied.
    Write {
        /// Blocks written.
        extent: Extent,
        /// Pool buffer with the blocks' contents.
        data: IoBuf,
    },
    /// Chunk blocks that read as zeros from this version on.
    Punch(Extent),
    /// Opaque bytes of an index or a Raft shard.
    Blob {
        /// Type of the payload.
        kind: BlobKind,
        /// Index or shard the payload belongs to.
        owner: u64,
        /// Payload.
        bytes: Vec<u8>,
    },
}

impl Item {
    fn entry(&self) -> Entry {
        match self {
            Self::Write { extent, .. } => Entry::Write(*extent),
            Self::Punch(extent) => Entry::Punch(*extent),
            Self::Blob { kind, owner, bytes } => Entry::Blob {
                kind: *kind,
                owner: *owner,
                len: u32::try_from(bytes.len()).unwrap_or(u32::MAX),
            },
        }
    }
}

/// Reason the log refused an item; the caller keeps ownership of its buffer.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum SubmitError {
    /// The item alone exceeds the largest record.
    TooLarge,
    /// A write or punch extent is empty or runs past the end of its chunk.
    BadExtent,
    /// A checkpoint is already in progress.
    Busy,
    /// An earlier disk error failed the log.
    Failed(DiskError),
}

/// Outcome reported by [`GroupCommit`].
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum LogEvent {
    /// A record is durable; `tickets[i]` submitted `record.header.entries[i]`.
    Durable {
        /// The durable record.
        record: LogRecord,
        /// Ticket of each entry.
        tickets: Vec<Ticket>,
    },
    /// These items will never be durable.
    Failed {
        /// Tickets of the failed items.
        tickets: Vec<Ticket>,
        /// Cause.
        error: DiskError,
    },
    /// A checkpoint's manifest is durable at this position.
    CheckpointDone(RecordPos),
    /// The running checkpoint will never complete; a new one may begin.
    CheckpointFailed(DiskError),
}

/// Actions to execute and events to handle.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Output {
    /// Disk and timer actions for the driver.
    pub actions: Vec<Action>,
    /// Durable, failed and checkpoint events for the caller.
    pub events: Vec<LogEvent>,
}

impl Output {
    fn extend(&mut self, other: Self) {
        self.actions.extend(other.actions);
        self.events.extend(other.events);
    }
}

/// A segment a stream appends to.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct OpenSegment {
    /// Segment index.
    pub index: u32,
    /// Its open sequence number.
    pub seq: u64,
    /// Block the stream's next record goes to.
    pub next_block: u32,
}

/// Log state recovered from disk, enough to keep appending.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Resume {
    /// The disk's superblock.
    pub superblock: Superblock,
    /// Open segment of each stream.
    pub open: [Option<OpenSegment>; 2],
    /// Segments holding nothing of this log.
    pub free: BTreeSet<u32>,
    /// Sequence number for the next segment opened.
    pub next_seq: u64,
    /// LSN of the next record.
    pub next_lsn: u64,
    /// Manifest recovery started from.
    pub manifest: Option<RecordPos>,
    /// Bytes of records after that manifest's base.
    pub replay_bytes: u64,
}

enum Work {
    Format,
    Record {
        stream: Stream,
        opened: OpenSegment,
        record: LogRecord,
        tickets: Vec<Ticket>,
        bytes: u64,
    },
}

#[derive(Clone, Copy)]
struct Target {
    stream: Stream,
    index: u32,
    seq: u64,
    offset: u32,
    roll: bool,
}

struct Round {
    io: IoId,
    flushing: bool,
    buf: IoBuf,
    work: Work,
}

struct Checkpoint {
    base: Manifest,
    replay_bytes_at_begin: u64,
    pending: BTreeMap<Ticket, (usize, usize)>,
    refs: Vec<(u64, Vec<Option<BlobRef>>)>,
    manifest: Option<Ticket>,
}

/// Appends records to a disk's log, one write and one flush per round.
pub struct GroupCommit {
    config: Config,
    superblock: Superblock,
    max_record_blocks: u32,
    max_blob_bytes: usize,
    open: [Option<OpenSegment>; 2],
    free: BTreeSet<u32>,
    next_seq: u64,
    next_lsn: u64,
    durable_lsn: u64,
    next_ticket: u64,
    next_io: u64,
    queues: [VecDeque<(Ticket, Item)>; 2],
    last: Stream,
    round: Option<Round>,
    failed: Option<DiskError>,
    manifest: Option<RecordPos>,
    replay_bytes: u64,
    checkpoint: Option<Checkpoint>,
}

impl GroupCommit {
    /// Formats an empty log: writes and flushes the superblock before the first round.
    pub fn format(
        ctx: &mut Context<'_, impl BufPool>,
        config: Config,
        superblock: Superblock,
        first_io: IoId,
    ) -> Result<(Self, Vec<Action>), PoolExhausted> {
        let buf = ctx.pool.alloc_zeroed(BLOCK_BYTES)?;
        superblock.encode(ctx.pool.as_mut_slice(buf));
        let mut log = Self::resume(
            config,
            Resume {
                superblock,
                open: [None; 2],
                free: (1..superblock.geometry.segment_count()).collect(),
                next_seq: 1,
                next_lsn: 1,
                manifest: None,
                replay_bytes: 0,
            },
            first_io,
        );
        let io = log.io();
        log.round = Some(Round {
            io,
            flushing: false,
            buf,
            work: Work::Format,
        });
        Ok((log, vec![Action::DiskWrite(io, Lba(0), buf)]))
    }

    /// Continues a recovered log.
    pub fn resume(config: Config, resume: Resume, first_io: IoId) -> Self {
        let segment_blocks = resume.superblock.geometry.segment_blocks();
        let max_record_blocks = u32::try_from(config.max_record_bytes / BLOCK_BYTES)
            .unwrap_or(u32::MAX)
            .clamp(2, segment_blocks - 1);
        Self {
            config,
            superblock: resume.superblock,
            max_record_blocks,
            max_blob_bytes: max_blob_bytes(max_record_blocks),
            open: resume.open,
            free: resume.free,
            next_seq: resume.next_seq,
            next_lsn: resume.next_lsn,
            durable_lsn: resume.next_lsn - 1,
            next_ticket: 1,
            next_io: first_io.0,
            queues: [VecDeque::new(), VecDeque::new()],
            last: Stream::Cold,
            round: None,
            failed: None,
            manifest: resume.manifest,
            replay_bytes: resume.replay_bytes,
            checkpoint: None,
        }
    }

    /// The disk's superblock.
    pub const fn superblock(&self) -> &Superblock {
        &self.superblock
    }

    /// LSN of the newest durable record.
    pub const fn durable_lsn(&self) -> u64 {
        self.durable_lsn
    }

    /// Newest durable manifest.
    pub const fn manifest(&self) -> Option<RecordPos> {
        self.manifest
    }

    /// Error that failed the log, if any.
    pub const fn failed(&self) -> Option<DiskError> {
        self.failed
    }

    /// Whether a round is being written or flushed.
    pub const fn in_flight(&self) -> bool {
        self.round.is_some()
    }

    /// Segments not holding any of this log's records.
    pub const fn free_segments(&self) -> &BTreeSet<u32> {
        &self.free
    }

    /// Whether `max_replay_bytes` of records follow the last manifest and no checkpoint is running.
    pub fn checkpoint_due(&self) -> bool {
        self.checkpoint.is_none()
            && self.failed.is_none()
            && self.replay_bytes >= self.config.max_replay_bytes
    }

    /// Largest blob payload that fits in one record.
    pub const fn max_blob_bytes(&self) -> usize {
        self.max_blob_bytes
    }

    /// Queues `item` for `stream`; it is durable once a [`LogEvent::Durable`] names its ticket.
    pub fn submit(
        &mut self,
        ctx: &mut Context<'_, impl BufPool>,
        stream: Stream,
        item: Item,
    ) -> Result<(Ticket, Output), SubmitError> {
        if let Some(error) = self.failed {
            return Err(SubmitError::Failed(error));
        }
        if let Item::Write { extent, data } = &item {
            assert_eq!(
                data.len(),
                usize::from(extent.blocks) * BLOCK_BYTES,
                "write buffer does not match its extent"
            );
        }
        if let Entry::Write(extent) | Entry::Punch(extent) = item.entry()
            && !extent.is_valid()
        {
            return Err(SubmitError::BadExtent);
        }
        if Layout::of(&[item.entry()]).record_blocks() > self.max_record_blocks {
            return Err(SubmitError::TooLarge);
        }
        let ticket = self.enqueue(stream, item);
        Ok((ticket, self.start_round(ctx)))
    }

    /// Starts a checkpoint from `snapshots` (each index's pieces, at most
    /// [`max_blob_bytes`](Self::max_blob_bytes) each), which must reflect exactly the records up to
    /// [`durable_lsn`](Self::durable_lsn). Ends with [`LogEvent::CheckpointDone`] or
    /// [`LogEvent::CheckpointFailed`].
    pub fn begin_checkpoint(
        &mut self,
        ctx: &mut Context<'_, impl BufPool>,
        snapshots: Vec<(u64, Vec<Vec<u8>>)>,
    ) -> Result<Output, SubmitError> {
        if let Some(error) = self.failed {
            return Err(SubmitError::Failed(error));
        }
        if self.checkpoint.is_some() {
            return Err(SubmitError::Busy);
        }
        if snapshots
            .iter()
            .flat_map(|(_, pieces)| pieces)
            .any(|piece| piece.len() > self.max_blob_bytes)
            || manifest_len(&snapshots) > self.max_blob_bytes
        {
            return Err(SubmitError::TooLarge);
        }
        let mut checkpoint = Checkpoint {
            base: Manifest {
                base_lsn: self.durable_lsn,
                base_seq: self.open.iter().flatten().map(|o| o.seq).max().unwrap_or(0),
                starts: self.open.map(|o| o.map(|o| (o.index, o.next_block))),
                trees: Vec::new(),
            },
            replay_bytes_at_begin: self.replay_bytes,
            pending: BTreeMap::new(),
            refs: Vec::new(),
            manifest: None,
        };
        for (tree, (owner, pieces)) in snapshots.into_iter().enumerate() {
            checkpoint.refs.push((owner, vec![None; pieces.len()]));
            for (piece, bytes) in pieces.into_iter().enumerate() {
                let item = Item::Blob {
                    kind: BlobKind::SNAPSHOT,
                    owner,
                    bytes,
                };
                let ticket = self.enqueue(Stream::Cold, item);
                checkpoint.pending.insert(ticket, (tree, piece));
            }
        }
        self.checkpoint = Some(checkpoint);
        self.enqueue_manifest_when_ready();
        Ok(self.start_round(ctx))
    }

    /// Advances rounds on their disk completions and retries after pool exhaustion.
    #[allow(
        clippy::large_types_passed_by_value,
        reason = "sans-IO on_event takes Event by value"
    )]
    pub fn on_event(&mut self, ctx: &mut Context<'_, impl BufPool>, ev: Event) -> Output {
        match ev {
            Event::DiskDone(io, result) if self.round.as_ref().is_some_and(|r| r.io == io) => {
                if let Err(error) = result {
                    return self.fail(ctx, error);
                }
                let round = self.round.as_mut().expect("matched above");
                if !round.flushing {
                    round.flushing = true;
                    return Output {
                        actions: vec![Action::DiskFsync(io)],
                        events: Vec::new(),
                    };
                }
                let round = self.round.take().expect("matched above");
                ctx.pool.free(round.buf);
                let mut out = Output::default();
                if let Work::Record {
                    stream,
                    opened,
                    record,
                    tickets,
                    bytes,
                } = round.work
                {
                    self.open[stream.index()] = Some(opened);
                    self.durable_lsn = record.pos.lsn;
                    self.replay_bytes += bytes;
                    let pos = record.pos;
                    let done = self.note_checkpoint_progress(pos, &tickets);
                    out.events.push(LogEvent::Durable { record, tickets });
                    out.events.extend(done.map(LogEvent::CheckpointDone));
                }
                out.extend(self.start_round(ctx));
                out
            }
            Event::Timer(timer) if timer == self.config.retry_timer => self.start_round(ctx),
            _ => Output::default(),
        }
    }

    fn io(&mut self) -> IoId {
        let io = IoId(self.next_io);
        self.next_io += 1;
        io
    }

    fn enqueue(&mut self, stream: Stream, item: Item) -> Ticket {
        let ticket = Ticket(self.next_ticket);
        self.next_ticket += 1;
        self.queues[stream.index()].push_back((ticket, item));
        ticket
    }

    fn enqueue_manifest_when_ready(&mut self) {
        let Some(checkpoint) = &self.checkpoint else {
            return;
        };
        if !checkpoint.pending.is_empty() || checkpoint.manifest.is_some() {
            return;
        }
        let manifest = Manifest {
            trees: checkpoint
                .refs
                .iter()
                .map(|(owner, refs)| (*owner, refs.iter().flatten().copied().collect()))
                .collect(),
            ..checkpoint.base.clone()
        };
        let ticket = self.enqueue(
            Stream::Cold,
            Item::Blob {
                kind: BlobKind::MANIFEST,
                owner: 0,
                bytes: manifest.encode(),
            },
        );
        self.checkpoint.as_mut().expect("checked above").manifest = Some(ticket);
    }

    fn note_checkpoint_progress(
        &mut self,
        pos: RecordPos,
        tickets: &[Ticket],
    ) -> Option<RecordPos> {
        let checkpoint = self.checkpoint.as_mut()?;
        if checkpoint.manifest.is_some_and(|m| tickets.contains(&m)) {
            self.replay_bytes -= checkpoint.replay_bytes_at_begin;
            self.manifest = Some(pos);
            self.checkpoint = None;
            return Some(pos);
        }
        for (entry, ticket) in tickets.iter().enumerate() {
            if let Some((tree, piece)) = checkpoint.pending.remove(ticket) {
                checkpoint.refs[tree].1[piece] = Some(BlobRef {
                    record: pos,
                    entry: u16::try_from(entry).expect("entries per record fit u16"),
                });
            }
        }
        self.enqueue_manifest_when_ready();
        None
    }

    fn fail(&mut self, ctx: &mut Context<'_, impl BufPool>, error: DiskError) -> Output {
        self.failed = Some(error);
        let checkpoint = self.checkpoint.take();
        let mut tickets = Vec::new();
        if let Some(round) = self.round.take() {
            ctx.pool.free(round.buf);
            if let Work::Record { tickets: t, .. } = round.work {
                tickets.extend(t);
            }
        }
        for queue in &mut self.queues {
            tickets.extend(drain(ctx, queue));
        }
        let mut events = vec![LogEvent::Failed { tickets, error }];
        events.extend(checkpoint.map(|_| LogEvent::CheckpointFailed(error)));
        Output {
            actions: Vec::new(),
            events,
        }
    }

    fn pick_stream(&self) -> Option<Stream> {
        match (
            !self.queues[Stream::Hot.index()].is_empty(),
            !self.queues[Stream::Cold.index()].is_empty(),
        ) {
            (true, true) if self.last == Stream::Hot => Some(Stream::Cold),
            (true, _) => Some(Stream::Hot),
            (false, true) => Some(Stream::Cold),
            (false, false) => None,
        }
    }

    /// Picks the stream for the next round and where its record goes; with the disk full, fails
    /// that stream's queue and tries the other.
    fn target(&mut self, ctx: &mut Context<'_, impl BufPool>, out: &mut Output) -> Option<Target> {
        let segment_blocks = self.superblock.geometry.segment_blocks();
        loop {
            let stream = self.pick_stream()?;
            let queue = &self.queues[stream.index()];
            let first = Layout::of(&[queue[0].1.entry()]).record_blocks();
            if let Some(o) = self.open[stream.index()]
                && segment_blocks - o.next_block >= first
            {
                return Some(Target {
                    stream,
                    index: o.index,
                    seq: o.seq,
                    offset: o.next_block,
                    roll: false,
                });
            }
            if let Some(&index) = self.free.first() {
                return Some(Target {
                    stream,
                    index,
                    seq: self.next_seq,
                    offset: 1,
                    roll: true,
                });
            }
            let tickets = drain(ctx, &mut self.queues[stream.index()]);
            let checkpoint_lost = self.checkpoint.as_ref().is_some_and(|c| {
                tickets
                    .iter()
                    .any(|t| c.pending.contains_key(t) || c.manifest == Some(*t))
            });
            out.events.push(LogEvent::Failed {
                tickets,
                error: DiskError::NoSpace,
            });
            if checkpoint_lost {
                self.checkpoint = None;
                out.events
                    .push(LogEvent::CheckpointFailed(DiskError::NoSpace));
            }
        }
    }

    /// Encodes as many queued items as fit at `t`, after a segment header if `t` opens a segment.
    /// Returns how many items it took, the record's header and the bytes to write.
    fn assemble(
        &self,
        ctx: &Context<'_, impl BufPool>,
        t: Target,
    ) -> (usize, RecordHeader, Vec<u8>) {
        let queue = &self.queues[t.stream.index()];
        let segment_blocks = self.superblock.geometry.segment_blocks();
        let cap = (segment_blocks - t.offset).min(self.max_record_blocks);
        let mut builder = LayoutBuilder::default();
        let mut count = 0;
        for (_, item) in queue.iter().take(MAX_ENTRIES) {
            let next = builder.with(&item.entry());
            if next.layout().record_blocks() > cap {
                break;
            }
            builder = next;
            count += 1;
        }
        let entries: Vec<Entry> = queue.iter().take(count).map(|(_, i)| i.entry()).collect();
        let layout = builder.layout();

        // ponytail: the record is assembled in a heap image and copied into the pool; a vectored
        // DiskWrite would avoid both copies once a driver needs it.
        let lead = usize::from(t.roll);
        let mut image = vec![0u8; (lead + layout.record_blocks() as usize) * BLOCK_BYTES];
        if t.roll {
            SegmentHeader {
                log_id: self.superblock.log_id,
                seq: t.seq,
                index: t.index,
                stream: t.stream,
                manifest: self.manifest,
            }
            .encode(&mut image[..BLOCK_BYTES]);
        }
        let data_at = (lead + layout.header_blocks as usize) * BLOCK_BYTES;
        for ((_, item), span) in queue.iter().zip(record::data_spans(&entries)) {
            let dest = &mut image[data_at + span.start..data_at + span.end];
            match item {
                Item::Write { data, .. } => dest.copy_from_slice(ctx.pool.as_slice(*data)),
                Item::Blob { bytes, .. } => dest.copy_from_slice(bytes),
                Item::Punch(_) => {}
            }
        }
        let header = record::encode(
            self.superblock.log_id,
            t.seq,
            t.offset,
            self.next_lsn,
            entries,
            &mut image[lead * BLOCK_BYTES..],
        );
        (count, header, image)
    }

    fn start_round(&mut self, ctx: &mut Context<'_, impl BufPool>) -> Output {
        let mut out = Output::default();
        if self.round.is_some() || self.failed.is_some() {
            return out;
        }
        let Some(t) = self.target(ctx, &mut out) else {
            return out;
        };
        let (count, header, image) = self.assemble(ctx, t);
        let Ok(buf) = ctx.pool.alloc_from_slice(&image) else {
            out.actions.push(Action::SetTimer(
                self.config.retry_timer,
                ctx.now.saturating_add(self.config.retry_delay),
            ));
            return out;
        };

        let mut tickets = Vec::with_capacity(count);
        let mut blobs = Vec::new();
        for (ticket, item) in self.queues[t.stream.index()].drain(..count) {
            tickets.push(ticket);
            match item {
                Item::Write { data, .. } => ctx.pool.free(data),
                Item::Blob { bytes, .. } => blobs.push(bytes),
                Item::Punch(_) => {}
            }
        }
        if t.roll {
            self.free.remove(&t.index);
            self.next_seq += 1;
        }
        let pos = RecordPos {
            segment: t.index,
            block: t.offset,
            lsn: self.next_lsn,
        };
        self.next_lsn += 1;
        self.last = t.stream;
        let io = self.io();
        let geometry = self.superblock.geometry;
        out.actions.push(Action::DiskWrite(
            io,
            geometry.lba(t.index, t.offset - u32::from(t.roll)),
            buf,
        ));
        self.round = Some(Round {
            io,
            flushing: false,
            buf,
            work: Work::Record {
                stream: t.stream,
                opened: OpenSegment {
                    index: t.index,
                    seq: t.seq,
                    next_block: t.offset + header.layout().record_blocks(),
                },
                record: LogRecord {
                    pos,
                    lba: geometry.lba(t.index, t.offset),
                    header,
                    blobs,
                },
                tickets,
                bytes: image.len() as u64,
            },
        });
        out
    }
}

/// Bytes of the manifest that would list `snapshots`; its size does not depend on where they land.
fn manifest_len(snapshots: &[(u64, Vec<Vec<u8>>)]) -> usize {
    let placeholder = BlobRef {
        record: RecordPos {
            segment: 0,
            block: 0,
            lsn: 0,
        },
        entry: 0,
    };
    Manifest {
        trees: snapshots
            .iter()
            .map(|(owner, pieces)| (*owner, vec![placeholder; pieces.len()]))
            .collect(),
        ..Manifest::default()
    }
    .encode()
    .len()
}

/// Largest blob payload that fits in a record of `max_record_blocks`.
fn max_blob_bytes(max_record_blocks: u32) -> usize {
    let fits = |blocks: u32| {
        let len = u32::try_from(blocks as usize * BLOCK_BYTES).unwrap_or(u32::MAX);
        Layout::of(&[Entry::Blob {
            kind: BlobKind::SNAPSHOT,
            owner: 0,
            len,
        }])
        .record_blocks()
            <= max_record_blocks
    };
    let blocks = (1..max_record_blocks).rev().find(|&b| fits(b)).unwrap_or(0);
    blocks as usize * BLOCK_BYTES
}

fn drain(ctx: &mut Context<'_, impl BufPool>, queue: &mut VecDeque<(Ticket, Item)>) -> Vec<Ticket> {
    queue
        .drain(..)
        .map(|(ticket, item)| {
            if let Item::Write { data, .. } = item {
                ctx.pool.free(data);
            }
            ticket
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::buf::test_pool::VecPool;
    use crate::event::Tick;
    use crate::id::{ChunkId, InodeId, PoolId, VolumeId};
    use crate::log::manifest::Manifest;
    use crate::log::record::{Expected, decode};
    use crate::log::segment::Geometry;

    const GIB_BLOCKS: u64 = 1 << 18;

    struct Harness {
        pool: VecPool,
        log: GroupCommit,
        trace: Vec<Action>,
        writes: Vec<(Lba, Vec<u8>)>,
        events: Vec<LogEvent>,
    }

    impl Harness {
        fn new(disk_blocks: u64, config: Config) -> Self {
            let mut pool = VecPool::new(usize::MAX);
            let superblock = Superblock {
                log_id: 77,
                geometry: Geometry::for_disk(disk_blocks).unwrap(),
            };
            let (log, actions) = GroupCommit::format(
                &mut Context::new(Tick(0), &mut pool),
                config,
                superblock,
                IoId(1),
            )
            .unwrap();
            let mut h = Self {
                pool,
                log,
                trace: actions,
                writes: Vec::new(),
                events: Vec::new(),
            };
            h.complete();
            h.complete();
            h.trace.clear();
            h.writes.clear();
            h
        }

        fn with<R>(
            &mut self,
            f: impl FnOnce(&mut GroupCommit, &mut Context<'_, VecPool>) -> R,
        ) -> R {
            f(&mut self.log, &mut Context::new(Tick(0), &mut self.pool))
        }

        fn absorb(&mut self, out: Output) {
            for action in &out.actions {
                if let Action::DiskWrite(_, lba, buf) = action {
                    self.writes.push((*lba, self.pool.as_slice(*buf).to_vec()));
                }
            }
            self.trace.extend(out.actions);
            self.events.extend(out.events);
        }

        fn submit(&mut self, stream: Stream, item: Item) -> Ticket {
            let (ticket, out) = self.with(|log, ctx| log.submit(ctx, stream, item)).unwrap();
            self.absorb(out);
            ticket
        }

        fn write(&mut self, chunk: u32, blocks: u16) -> Ticket {
            let data = self
                .pool
                .alloc_from_slice(&vec![
                    u8::try_from(chunk).unwrap();
                    usize::from(blocks) * BLOCK_BYTES
                ])
                .unwrap();
            self.submit(
                Stream::Hot,
                Item::Write {
                    extent: extent(chunk, blocks),
                    data,
                },
            )
        }

        fn blob(&mut self, stream: Stream, owner: u64, len: usize) -> Ticket {
            self.submit(
                stream,
                Item::Blob {
                    kind: BlobKind(9),
                    owner,
                    bytes: vec![1; len],
                },
            )
        }

        /// Completes the newest disk action in the trace.
        fn complete(&mut self) {
            let io = match self.trace.last() {
                Some(Action::DiskWrite(io, ..) | Action::DiskFsync(io)) => *io,
                other => panic!("no disk action to complete: {other:?}"),
            };
            let out = self.with(|log, ctx| log.on_event(ctx, Event::DiskDone(io, Ok(()))));
            self.absorb(out);
        }

        fn round(&mut self) {
            self.complete();
            self.complete();
        }

        fn durable(&self) -> Vec<(u64, Vec<Ticket>)> {
            self.events
                .iter()
                .filter_map(|e| match e {
                    LogEvent::Durable { record, tickets } => {
                        Some((record.pos.lsn, tickets.clone()))
                    }
                    _ => None,
                })
                .collect()
        }

        fn written(&self, n: usize) -> (Lba, Vec<u8>) {
            self.writes[n].clone()
        }
    }

    fn extent(chunk: u32, blocks: u16) -> Extent {
        Extent {
            pool: PoolId(1),
            chunk: ChunkId::new(VolumeId(1), InodeId(2), chunk),
            version: 1,
            first_block: 0,
            blocks,
        }
    }

    fn kinds(trace: &[Action]) -> Vec<&'static str> {
        trace
            .iter()
            .map(|a| match a {
                Action::DiskWrite(..) => "write",
                Action::DiskFsync(_) => "fsync",
                Action::SetTimer(..) => "timer",
                _ => "other",
            })
            .collect()
    }

    #[test]
    fn format_writes_and_flushes_the_superblock() {
        let mut pool = VecPool::new(usize::MAX);
        let superblock = Superblock {
            log_id: 5,
            geometry: Geometry::for_disk(GIB_BLOCKS).unwrap(),
        };
        let mut ctx = Context::new(Tick(0), &mut pool);
        let (mut log, actions) =
            GroupCommit::format(&mut ctx, Config::default(), superblock, IoId(10)).unwrap();
        let Action::DiskWrite(io, Lba(0), buf) = actions[0] else {
            panic!("{actions:?}");
        };
        assert_eq!(Superblock::decode(ctx.pool.as_slice(buf)), Ok(superblock));
        let out = log.on_event(&mut ctx, Event::DiskDone(io, Ok(())));
        assert_eq!(out.actions, vec![Action::DiskFsync(IoId(10))]);
        let out = log.on_event(&mut ctx, Event::DiskDone(io, Ok(())));
        assert_eq!(out, Output::default());
        assert_eq!(pool.live(), 0);
    }

    #[test]
    fn producers_share_one_write_and_one_flush_per_round() {
        let mut h = Harness::new(GIB_BLOCKS, Config::default());
        let first = h.write(1, 1);
        let waiting = [
            h.write(2, 2),
            h.blob(Stream::Hot, 10, 100),
            h.blob(Stream::Hot, 11, 5000),
        ];
        let punch = h.submit(Stream::Hot, Item::Punch(extent(3, 1024)));
        assert_eq!(kinds(&h.trace), ["write"]);
        h.round();
        h.round();
        assert_eq!(kinds(&h.trace), ["write", "fsync", "write", "fsync"]);
        assert_eq!(
            h.durable(),
            vec![
                (1, vec![first]),
                (2, vec![waiting[0], waiting[1], waiting[2], punch]),
            ]
        );
        assert_eq!(h.pool.live(), 0);

        let (lba, image) = h.written(0);
        assert_eq!(lba, Lba(64));
        let segment = SegmentHeader::decode(&image).unwrap();
        assert_eq!(
            (segment.index, segment.seq, segment.stream),
            (1, 1, Stream::Hot)
        );
        let expected = Expected {
            log_id: 77,
            segment_seq: 1,
            offset: 1,
            max_blocks: 63,
        };
        let header = decode(&image[BLOCK_BYTES..], &expected).unwrap();
        assert_eq!(header.entries, vec![Entry::Write(extent(1, 1))]);
        assert!(image[2 * BLOCK_BYTES..].iter().all(|&b| b == 1));

        let (lba, image) = h.written(1);
        assert_eq!(lba, Lba(64 + 3));
        let header = decode(
            &image,
            &Expected {
                offset: 3,
                max_blocks: 61,
                ..expected
            },
        )
        .unwrap();
        assert_eq!((header.lsn, header.entries.len()), (2, 4));
    }

    #[test]
    fn streams_alternate_and_open_their_own_segments() {
        let mut h = Harness::new(GIB_BLOCKS, Config::default());
        let hot = [h.write(1, 1), h.write(2, 1)];
        let cold = h.blob(Stream::Cold, 1, 10);
        h.round();
        let hot_after = h.write(3, 1);
        h.round();
        h.round();
        h.round();
        assert_eq!(
            h.durable(),
            vec![
                (1, vec![hot[0]]),
                (2, vec![cold]),
                (3, vec![hot[1], hot_after]),
            ]
        );
        let lbas: Vec<Lba> = (0..3).map(|n| h.written(n).0).collect();
        assert_eq!(lbas, [Lba(64), Lba(128), Lba(64 + 3)]);
    }

    #[test]
    fn records_stop_at_max_record_bytes() {
        let config = Config {
            max_record_bytes: 3 * BLOCK_BYTES,
            ..Config::default()
        };
        let mut h = Harness::new(GIB_BLOCKS, config);
        let first = h.write(1, 1);
        let rest = [h.write(2, 1), h.write(3, 1), h.write(4, 1)];
        assert_eq!(
            h.with(|log, ctx| log.submit(
                ctx,
                Stream::Hot,
                Item::Write {
                    extent: extent(5, 3),
                    data: IoBuf::new(0, 0, 3 * BLOCK_BYTES)
                }
            )),
            Err(SubmitError::TooLarge)
        );
        for _ in 0..3 {
            h.round();
        }
        assert_eq!(
            h.durable(),
            vec![
                (1, vec![first]),
                (2, vec![rest[0], rest[1]]),
                (3, vec![rest[2]])
            ]
        );
        assert_eq!(h.log.max_blob_bytes(), 2 * BLOCK_BYTES);
    }

    #[test]
    fn a_full_segment_rolls_and_a_full_disk_fails_with_no_space() {
        let mut h = Harness::new(16, Config::default());
        assert_eq!(h.log.free_segments().len(), 3);
        let mut tickets = Vec::new();
        for chunk in 0..3 {
            tickets.push(h.write(chunk, 2));
            h.round();
        }
        let lbas: Vec<Lba> = (0..3).map(|n| h.written(n).0).collect();
        assert_eq!(lbas, [Lba(4), Lba(8), Lba(12)]);
        let late = h.write(9, 1);
        assert_eq!(
            h.events.last(),
            Some(&LogEvent::Failed {
                tickets: vec![late],
                error: DiskError::NoSpace
            })
        );
        assert_eq!(h.pool.live(), 0);
        let one_block = h.write(10, 1);
        assert!(
            matches!(h.events.last(), Some(LogEvent::Failed { tickets, .. }) if *tickets == [one_block])
        );
    }

    #[test]
    fn extents_that_would_not_decode_are_refused() {
        let mut h = Harness::new(GIB_BLOCKS, Config::default());
        let past_chunk_end = Extent {
            first_block: 1000,
            blocks: 100,
            ..extent(1, 1)
        };
        for item in [Item::Punch(past_chunk_end), Item::Punch(extent(1, 0))] {
            assert_eq!(
                h.with(|log, ctx| log.submit(ctx, Stream::Hot, item)),
                Err(SubmitError::BadExtent)
            );
        }
        assert_eq!(h.trace, Vec::new());
    }

    #[test]
    fn a_manifest_too_large_for_a_record_is_refused() {
        let config = Config {
            max_record_bytes: 3 * BLOCK_BYTES,
            ..Config::default()
        };
        let mut h = Harness::new(GIB_BLOCKS, config);
        let snapshots = vec![(1, vec![vec![1]; 500])];
        assert_eq!(
            h.with(|log, ctx| log.begin_checkpoint(ctx, snapshots)),
            Err(SubmitError::TooLarge)
        );
        assert_eq!(h.trace, Vec::new());
    }

    #[test]
    fn a_checkpoint_failed_by_a_full_disk_can_be_retried() {
        let mut h = Harness::new(16, Config::default());
        for chunk in 0..3 {
            h.write(chunk, 2);
            h.round();
        }
        let out = h
            .with(|log, ctx| log.begin_checkpoint(ctx, vec![(1, vec![vec![1; 10]])]))
            .unwrap();
        assert_eq!(
            out.events.last(),
            Some(&LogEvent::CheckpointFailed(DiskError::NoSpace))
        );
        assert!(
            h.with(|log, ctx| log.begin_checkpoint(ctx, Vec::new()))
                .is_ok()
        );
    }

    #[test]
    fn a_disk_error_fails_the_round_the_queue_and_later_submits() {
        let mut h = Harness::new(GIB_BLOCKS, Config::default());
        let in_round = h.write(1, 1);
        let queued = h.write(2, 1);
        let out =
            h.with(|log, ctx| log.on_event(ctx, Event::DiskDone(IoId(2), Err(DiskError::Io))));
        assert_eq!(
            out.events,
            vec![LogEvent::Failed {
                tickets: vec![in_round, queued],
                error: DiskError::Io
            }]
        );
        assert_eq!(h.pool.live(), 0);
        assert_eq!(
            h.with(|log, ctx| log.submit(ctx, Stream::Hot, Item::Punch(extent(1, 1)))),
            Err(SubmitError::Failed(DiskError::Io))
        );
    }

    #[test]
    fn an_exhausted_pool_retries_on_the_timer() {
        let config = Config {
            retry_timer: TimerId(4),
            retry_delay: 50,
            ..Config::default()
        };
        let mut h = Harness::new(GIB_BLOCKS, config);
        h.pool.capacity = 2 * BLOCK_BYTES;
        let ticket = h.write(1, 1);
        assert_eq!(h.trace, vec![Action::SetTimer(TimerId(4), Tick(50))]);
        h.pool.capacity = usize::MAX;
        let out = h.with(|log, ctx| log.on_event(ctx, Event::Timer(TimerId(4))));
        h.absorb(out);
        assert_eq!(kinds(&h.trace), ["timer", "write"]);
        h.round();
        assert_eq!(h.durable(), vec![(1, vec![ticket])]);
    }

    #[test]
    fn a_checkpoint_writes_snapshots_then_a_manifest_that_the_next_segment_points_to() {
        let config = Config {
            max_replay_bytes: 8 * BLOCK_BYTES as u64,
            ..Config::default()
        };
        let mut h = Harness::new(GIB_BLOCKS, config);
        h.write(1, 3);
        h.round();
        assert!(!h.log.checkpoint_due());
        h.write(2, 3);
        h.round();
        assert!(h.log.checkpoint_due());
        let in_flight = h.write(3, 1);

        let snapshots = vec![(5, vec![vec![7; 10], vec![8; 20]]), (6, Vec::new())];
        let out = h
            .with(|log, ctx| log.begin_checkpoint(ctx, snapshots))
            .unwrap();
        assert_eq!(
            h.with(|log, ctx| log.begin_checkpoint(ctx, Vec::new())),
            Err(SubmitError::Busy)
        );
        h.absorb(out);
        assert!(!h.log.checkpoint_due());
        h.round();
        h.round();
        h.round();
        let done = h.events.iter().find_map(|e| match e {
            LogEvent::CheckpointDone(pos) => Some(*pos),
            _ => None,
        });
        assert_eq!(
            h.durable().iter().map(|d| d.0).collect::<Vec<_>>(),
            [1, 2, 3, 4, 5]
        );
        assert_eq!(h.durable()[2].1, vec![in_flight]);
        let manifest_pos = done.unwrap();
        assert_eq!(manifest_pos.lsn, 5);
        assert_eq!(h.log.manifest(), Some(manifest_pos));

        let LogEvent::Durable { record, .. } = &h.events[4] else {
            panic!("{:?}", h.events[4]);
        };
        let manifest = Manifest::decode(&record.blobs[0]).unwrap();
        assert_eq!(manifest.base_lsn, 2);
        assert_eq!(manifest.base_seq, 1);
        assert_eq!(manifest.starts, [Some((1, 1 + 4 + 4)), None]);
        let snapshot_record = RecordPos {
            segment: 2,
            block: 1,
            lsn: 4,
        };
        assert_eq!(
            manifest.trees,
            vec![
                (
                    5,
                    vec![
                        BlobRef {
                            record: snapshot_record,
                            entry: 0
                        },
                        BlobRef {
                            record: snapshot_record,
                            entry: 1
                        },
                    ]
                ),
                (6, Vec::new()),
            ]
        );

        let mut h2 = h;
        while h2.log.free_segments().first() == Some(&3) {
            h2.write(4, 40);
            h2.round();
        }
        let roll = h2
            .writes
            .iter()
            .find(|(lba, _)| *lba == Lba(3 * 64))
            .unwrap();
        assert_eq!(
            SegmentHeader::decode(&roll.1).unwrap().manifest,
            Some(manifest_pos)
        );
    }
}
