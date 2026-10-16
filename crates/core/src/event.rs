//! Sans-IO event, action, and driver-contract types.

use std::collections::BTreeMap;
use std::fmt;

use crate::buf::{BufPool, IoBuf};

/// Size of a single logical disk block in bytes.
pub const BLOCK_BYTES: usize = 4096;

/// Maximum byte length of an inline control message.
pub const MAX_CONTROL_MSG_BYTES: usize = 256;

/// Monotonic timestamp in microseconds (1 tick = 1 µs).
#[repr(transparent)]
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Tick(pub u64);

impl Tick {
    /// Origin tick zero.
    pub const ZERO: Self = Self(0);

    /// One microsecond in ticks.
    pub const ONE_MICROS: u64 = 1;

    /// One millisecond in ticks (1,000 µs).
    pub const ONE_MILLIS: u64 = 1_000;

    /// One second in ticks (1,000,000 µs).
    pub const ONE_SECOND: u64 = 1_000_000;

    /// Wraps a raw microsecond count.
    pub const fn new(raw: u64) -> Self {
        Self(raw)
    }

    /// Constructs a tick from microseconds.
    pub const fn from_micros(micros: u64) -> Self {
        Self(micros)
    }

    /// Constructs a tick from milliseconds.
    pub const fn from_millis(millis: u64) -> Self {
        Self(millis.saturating_mul(Self::ONE_MILLIS))
    }

    /// Constructs a tick from seconds.
    pub const fn from_secs(secs: u64) -> Self {
        Self(secs.saturating_mul(Self::ONE_SECOND))
    }

    /// Returns the timestamp in microseconds.
    pub const fn as_micros(self) -> u64 {
        self.0
    }

    /// Returns the timestamp in milliseconds (truncated).
    pub const fn as_millis(self) -> u64 {
        self.0 / Self::ONE_MILLIS
    }

    /// Returns the timestamp in seconds (truncated).
    pub const fn as_secs(self) -> u64 {
        self.0 / Self::ONE_SECOND
    }

    /// Returns the raw tick value.
    pub const fn as_u64(self) -> u64 {
        self.0
    }

    /// Advances the tick by `delta`, saturating at [`u64::MAX`].
    #[must_use]
    pub const fn saturating_add(self, delta: u64) -> Self {
        Self(self.0.saturating_add(delta))
    }

    /// Elapsed ticks since `earlier`, saturating at zero.
    pub const fn saturating_sub(self, earlier: Self) -> u64 {
        self.0.saturating_sub(earlier.0)
    }
}

/// Execution context passed to sans-IO state machines.
pub struct Context<'a, P: BufPool + ?Sized> {
    /// Current monotonic timestamp in ticks (1 tick = 1 µs).
    pub now: Tick,
    /// Driver-owned buffer pool.
    pub pool: &'a mut P,
}

impl<'a, P: BufPool + ?Sized> Context<'a, P> {
    /// Creates an execution context for `now` with access to `pool`.
    pub fn new(now: Tick, pool: &'a mut P) -> Self {
        Self { now, pool }
    }
}

/// Identifier correlating a disk action with its [`Event::DiskDone`] completion.
#[repr(transparent)]
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct IoId(pub u64);

impl IoId {
    /// Wraps a raw I/O identifier.
    pub const fn new(raw: u64) -> Self {
        Self(raw)
    }

    /// Returns the raw identifier value.
    pub const fn as_u64(self) -> u64 {
        self.0
    }
}

/// Identifier correlating an [`Action::SetTimer`] with its [`Event::Timer`] firing.
#[repr(transparent)]
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct TimerId(pub u64);

impl TimerId {
    /// Wraps a raw timer identifier.
    pub const fn new(raw: u64) -> Self {
        Self(raw)
    }

    /// Returns the raw identifier value.
    pub const fn as_u64(self) -> u64 {
        self.0
    }
}

/// Cluster-unique identifier of a node.
#[repr(transparent)]
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct NodeId(pub u32);

impl NodeId {
    /// Wraps a raw node identifier.
    pub const fn new(raw: u32) -> Self {
        Self(raw)
    }

    /// Returns the raw node identifier value.
    pub const fn as_u32(self) -> u32 {
        self.0
    }
}

/// Logical block address on a disk in 4 KiB blocks.
#[repr(transparent)]
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Lba(pub u64);

impl Lba {
    /// Wraps a raw 4 KiB block index.
    pub const fn new(raw: u64) -> Self {
        Self(raw)
    }

    /// Returns the raw 4 KiB block index.
    pub const fn as_u64(self) -> u64 {
        self.0
    }

    /// Returns the byte offset of this block on disk.
    pub const fn byte_offset(self) -> u64 {
        self.0.saturating_mul(BLOCK_BYTES as u64)
    }

    /// Advances the block address by `blocks` 4 KiB blocks.
    #[must_use]
    pub const fn advance(self, blocks: u64) -> Self {
        Self(self.0.saturating_add(blocks))
    }
}

/// Error returned when a control message exceeds [`MAX_CONTROL_MSG_BYTES`].
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ControlMsgTooLarge {
    /// Length in bytes of the rejected control payload.
    pub len: usize,
}

impl fmt::Display for ControlMsgTooLarge {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "control message of {} bytes exceeds inline limit of {} bytes",
            self.len, MAX_CONTROL_MSG_BYTES
        )
    }
}

impl core::error::Error for ControlMsgTooLarge {}

/// Inline buffer holding a control message of at most [`MAX_CONTROL_MSG_BYTES`] bytes.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ControlMsg {
    bytes: [u8; MAX_CONTROL_MSG_BYTES],
    len: u16,
}

impl ControlMsg {
    /// Empty inline control message.
    pub const EMPTY: Self = Self {
        bytes: [0u8; MAX_CONTROL_MSG_BYTES],
        len: 0,
    };

    /// Copies `slice` into an inline control message if its length is at most 256 bytes.
    pub fn new(slice: &[u8]) -> Result<Self, ControlMsgTooLarge> {
        if slice.len() > MAX_CONTROL_MSG_BYTES {
            return Err(ControlMsgTooLarge { len: slice.len() });
        }
        let mut bytes = [0u8; MAX_CONTROL_MSG_BYTES];
        bytes[..slice.len()].copy_from_slice(slice);
        let len = u16::try_from(slice.len()).unwrap_or(u16::MAX);
        Ok(Self { bytes, len })
    }

    /// Length of the control message in bytes.
    pub const fn len(&self) -> usize {
        self.len as usize
    }

    /// Returns true if the control message is empty.
    pub const fn is_empty(&self) -> bool {
        self.len == 0
    }

    /// Returns the valid byte slice of the control message.
    pub fn as_slice(&self) -> &[u8] {
        &self.bytes[..self.len()]
    }
}

impl Default for ControlMsg {
    fn default() -> Self {
        Self::EMPTY
    }
}

impl fmt::Debug for ControlMsg {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_tuple("ControlMsg").field(&self.as_slice()).finish()
    }
}

/// Wire message carried by [`Event::NetRecv`] and [`Action::NetSend`].
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Msg {
    /// Inline control header or control message of at most 256 bytes.
    pub control: ControlMsg,
    /// Optional driver-owned buffer handle carrying bulk data.
    pub data: Option<IoBuf>,
}

impl Msg {
    /// Constructs an inline control-only message with no data buffer.
    pub fn control(bytes: &[u8]) -> Result<Self, ControlMsgTooLarge> {
        Ok(Self {
            control: ControlMsg::new(bytes)?,
            data: None,
        })
    }

    /// Constructs a control-only message from an existing [`ControlMsg`].
    pub const fn from_control(control: ControlMsg) -> Self {
        Self {
            control,
            data: None,
        }
    }

    /// Constructs a message pairing an inline [`ControlMsg`] with a driver-owned [`IoBuf`].
    pub const fn with_data(control: ControlMsg, data: IoBuf) -> Self {
        Self {
            control,
            data: Some(data),
        }
    }

    /// Constructs a data-only message carrying a driver-owned [`IoBuf`].
    pub const fn data_only(data: IoBuf) -> Self {
        Self {
            control: ControlMsg::EMPTY,
            data: Some(data),
        }
    }
}

/// Error reported by a disk operation in [`Event::DiskDone`].
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum DiskError {
    /// Underlying device or file I/O failure.
    Io,
    /// Checksum or on-disk format corruption detected.
    Corrupt,
    /// Disk or pool capacity limit reached.
    NoSpace,
    /// LBA range or buffer length is invalid.
    InvalidRange,
}

impl fmt::Display for DiskError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Io => f.write_str("disk I/O error"),
            Self::Corrupt => f.write_str("corrupt disk block or record"),
            Self::NoSpace => f.write_str("no space left on disk"),
            Self::InvalidRange => f.write_str("invalid disk LBA range or buffer size"),
        }
    }
}

impl core::error::Error for DiskError {}

/// Completion status of a disk action delivered by [`Event::DiskDone`].
pub type DiskResult = Result<(), DiskError>;

/// Input event delivered to a sans-IO state machine.
#[allow(
    clippy::large_enum_variant,
    reason = "driver contract keeps <= 256 B control messages inline without heap allocation"
)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Event {
    /// Message received from a peer node.
    NetRecv(NodeId, Msg),
    /// Connection to a peer failed; every outstanding request to that peer fails.
    PeerLost(NodeId),
    /// Disk read, write, flush, or trim completed.
    DiskDone(IoId, DiskResult),
    /// Scheduled timer fired at its deadline.
    Timer(TimerId),
}

/// Output action emitted by a sans-IO state machine for a driver to execute.
#[allow(
    clippy::large_enum_variant,
    reason = "driver contract keeps <= 256 B control messages inline without heap allocation"
)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Action {
    /// Send a message to a peer node.
    NetSend(NodeId, Msg),
    /// Read from disk starting at `Lba` into the buffer named by `IoBuf`.
    DiskRead(IoId, Lba, IoBuf),
    /// Write the buffer named by `IoBuf` to disk starting at `Lba`.
    DiskWrite(IoId, Lba, IoBuf),
    /// Flush previously written disk blocks to durable media.
    DiskFsync(IoId),
    /// Discard (TRIM) `u64` 4 KiB blocks starting at `Lba`.
    DiskTrim(IoId, Lba, u64),
    /// Arm a timer to fire [`Event::Timer`] at `Tick`.
    SetTimer(TimerId, Tick),
}

/// Identifier of an outbound request tracked by [`PeerRequests`].
#[repr(transparent)]
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct RequestId(pub u64);

/// Reason an outbound peer request failed.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum PeerRequestFailure {
    /// Connection to the target peer failed with [`Event::PeerLost`].
    PeerLost,
    /// Core-managed request deadline expired with [`Event::Timer`].
    Timeout,
}

/// Outbound peer request failed by [`PeerRequests::on_event`].
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct FailedPeerRequest {
    /// Identifier of the failed request.
    pub id: RequestId,
    /// Target peer of the failed request.
    pub peer: NodeId,
    /// Deadline timer associated with the request.
    pub timer: TimerId,
    /// Optional data buffer handle carried by the request.
    pub data: Option<IoBuf>,
    /// Failure cause.
    pub reason: PeerRequestFailure,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct InFlightEntry {
    peer: NodeId,
    timer: TimerId,
    data: Option<IoBuf>,
}

/// Deterministic tracker for outbound peer requests, deadlines, and [`Event::PeerLost`] failures.
#[derive(Clone, Debug, Default)]
pub struct PeerRequests {
    next_request_id: u64,
    in_flight: BTreeMap<RequestId, InFlightEntry>,
    by_timer: BTreeMap<TimerId, RequestId>,
}

impl PeerRequests {
    /// Creates an empty outbound request tracker.
    pub const fn new() -> Self {
        Self {
            next_request_id: 0,
            in_flight: BTreeMap::new(),
            by_timer: BTreeMap::new(),
        }
    }

    /// Registers an outbound request to `peer` with a deadline on the caller-allocated `timer`.
    pub fn send(
        &mut self,
        now: Tick,
        timeout_ticks: u64,
        timer: TimerId,
        peer: NodeId,
        msg: Msg,
    ) -> (RequestId, [Action; 2]) {
        assert!(
            !self.by_timer.contains_key(&timer),
            "{timer:?} already guards an in-flight request"
        );
        let id = RequestId(self.next_request_id);
        self.next_request_id += 1;

        let deadline = now.saturating_add(timeout_ticks);
        self.in_flight.insert(
            id,
            InFlightEntry {
                peer,
                timer,
                data: msg.data,
            },
        );
        self.by_timer.insert(timer, id);

        let actions = [
            Action::NetSend(peer, msg),
            Action::SetTimer(timer, deadline),
        ];
        (id, actions)
    }

    /// Marks `id` completed and returns its associated data buffer handle, if any.
    pub fn complete(&mut self, id: RequestId) -> Option<Option<IoBuf>> {
        let entry = self.in_flight.remove(&id)?;
        self.by_timer.remove(&entry.timer);
        Some(entry.data)
    }

    /// Total number of requests currently in flight across all peers.
    pub fn in_flight_len(&self) -> usize {
        self.in_flight.len()
    }

    /// Number of requests currently in flight to `peer`.
    pub fn in_flight_for_peer(&self, peer: NodeId) -> usize {
        self.in_flight.values().filter(|e| e.peer == peer).count()
    }

    /// Processes [`Event::PeerLost`] and [`Event::Timer`], failing matching in-flight requests.
    pub fn on_event(&mut self, ev: Event) -> Vec<FailedPeerRequest> {
        match ev {
            Event::PeerLost(peer) => {
                let ids: Vec<RequestId> = self
                    .in_flight
                    .iter()
                    .filter_map(|(&id, entry)| (entry.peer == peer).then_some(id))
                    .collect();
                let mut failed = Vec::with_capacity(ids.len());
                for id in ids {
                    if let Some(entry) = self.in_flight.remove(&id) {
                        self.by_timer.remove(&entry.timer);
                        failed.push(FailedPeerRequest {
                            id,
                            peer: entry.peer,
                            timer: entry.timer,
                            data: entry.data,
                            reason: PeerRequestFailure::PeerLost,
                        });
                    }
                }
                failed
            }
            Event::Timer(timer) => {
                let Some(id) = self.by_timer.remove(&timer) else {
                    return Vec::new();
                };
                let Some(entry) = self.in_flight.remove(&id) else {
                    return Vec::new();
                };
                vec![FailedPeerRequest {
                    id,
                    peer: entry.peer,
                    timer: entry.timer,
                    data: entry.data,
                    reason: PeerRequestFailure::Timeout,
                }]
            }
            Event::NetRecv(_, _) | Event::DiskDone(_, _) => Vec::new(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn control_messages_enforce_256_byte_inline_limit() {
        let empty = ControlMsg::new(&[]).unwrap();
        assert!(empty.is_empty());
        assert_eq!(empty.len(), 0);

        let max_bytes = [0xABu8; MAX_CONTROL_MSG_BYTES];
        let max_msg = ControlMsg::new(&max_bytes).unwrap();
        assert_eq!(max_msg.len(), MAX_CONTROL_MSG_BYTES);
        assert_eq!(max_msg.as_slice(), &max_bytes);

        let oversized = [0xCDu8; MAX_CONTROL_MSG_BYTES + 1];
        let err = ControlMsg::new(&oversized).unwrap_err();
        assert_eq!(
            err,
            ControlMsgTooLarge {
                len: MAX_CONTROL_MSG_BYTES + 1
            }
        );
    }

    #[test]
    fn data_travels_only_as_iobuf_handles() {
        let buf = IoBuf::new(3, 1, BLOCK_BYTES);
        let ctrl = ControlMsg::new(b"write-req").unwrap();
        let net_msg = Msg::with_data(ctrl, buf);

        let send = Action::NetSend(NodeId::new(2), net_msg);
        let disk_write = Action::DiskWrite(IoId::new(9), Lba::new(64), buf);
        let disk_read = Action::DiskRead(IoId::new(10), Lba::new(64), buf);

        assert_eq!(
            send,
            Action::NetSend(
                NodeId::new(2),
                Msg {
                    control: ctrl,
                    data: Some(buf),
                }
            )
        );
        assert_eq!(
            disk_write,
            Action::DiskWrite(IoId::new(9), Lba::new(64), buf)
        );
        assert_eq!(
            disk_read,
            Action::DiskRead(IoId::new(10), Lba::new(64), buf)
        );
    }

    #[test]
    fn peer_lost_fails_all_outstanding_requests_for_that_peer_only() {
        let mut tracker = PeerRequests::new();
        let peer_a = NodeId::new(10);
        let peer_b = NodeId::new(20);
        let buf1 = IoBuf::new(0, 1, 4096);
        let buf2 = IoBuf::new(1, 1, 4096);

        let (req1, actions1) = tracker.send(
            Tick::new(100),
            50,
            TimerId::new(1),
            peer_a,
            Msg::with_data(ControlMsg::new(b"r1").unwrap(), buf1),
        );
        assert_eq!(
            actions1,
            [
                Action::NetSend(
                    peer_a,
                    Msg::with_data(ControlMsg::new(b"r1").unwrap(), buf1)
                ),
                Action::SetTimer(TimerId::new(1), Tick::new(150)),
            ]
        );

        let (req2, actions2) = tracker.send(
            Tick::new(110),
            50,
            TimerId::new(2),
            peer_a,
            Msg::control(b"r2").unwrap(),
        );
        assert_eq!(
            actions2,
            [
                Action::NetSend(peer_a, Msg::control(b"r2").unwrap()),
                Action::SetTimer(TimerId::new(2), Tick::new(160)),
            ]
        );

        let (req3, _actions3) = tracker.send(
            Tick::new(115),
            50,
            TimerId::new(3),
            peer_b,
            Msg::with_data(ControlMsg::new(b"r3").unwrap(), buf2),
        );

        assert_eq!(tracker.in_flight_for_peer(peer_a), 2);
        assert_eq!(tracker.in_flight_for_peer(peer_b), 1);

        let failed = tracker.on_event(Event::PeerLost(peer_a));
        assert_eq!(
            failed,
            vec![
                FailedPeerRequest {
                    id: req1,
                    peer: peer_a,
                    timer: TimerId::new(1),
                    data: Some(buf1),
                    reason: PeerRequestFailure::PeerLost,
                },
                FailedPeerRequest {
                    id: req2,
                    peer: peer_a,
                    timer: TimerId::new(2),
                    data: None,
                    reason: PeerRequestFailure::PeerLost,
                },
            ]
        );
        assert_eq!(tracker.in_flight_for_peer(peer_a), 0);
        assert_eq!(tracker.in_flight_for_peer(peer_b), 1);

        assert_eq!(tracker.on_event(Event::Timer(TimerId::new(1))), Vec::new());

        let timed_out = tracker.on_event(Event::Timer(TimerId::new(3)));
        assert_eq!(
            timed_out,
            vec![FailedPeerRequest {
                id: req3,
                peer: peer_b,
                timer: TimerId::new(3),
                data: Some(buf2),
                reason: PeerRequestFailure::Timeout,
            }]
        );
        assert_eq!(tracker.in_flight_len(), 0);
    }

    #[test]
    fn completed_request_ignores_later_timer_and_peer_lost() {
        let mut tracker = PeerRequests::new();
        let peer = NodeId::new(5);
        let buf = IoBuf::new(2, 1, 4096);
        let (req, _actions) = tracker.send(
            Tick::new(10),
            25,
            TimerId::new(1),
            peer,
            Msg::data_only(buf),
        );

        assert_eq!(tracker.on_event(Event::Timer(TimerId::new(2))), Vec::new());
        assert_eq!(tracker.in_flight_len(), 1);

        assert_eq!(tracker.complete(req), Some(Some(buf)));
        assert_eq!(tracker.complete(req), None);
        assert_eq!(tracker.on_event(Event::Timer(TimerId::new(1))), Vec::new());
        assert_eq!(tracker.on_event(Event::PeerLost(peer)), Vec::new());
    }

    #[test]
    fn tick_time_unit_conversions() {
        assert_eq!(Tick::ONE_MICROS, 1);
        assert_eq!(Tick::ONE_MILLIS, 1_000);
        assert_eq!(Tick::ONE_SECOND, 1_000_000);

        let t = Tick::from_secs(2);
        assert_eq!(t.as_secs(), 2);
        assert_eq!(t.as_millis(), 2_000);
        assert_eq!(t.as_micros(), 2_000_000);
        assert_eq!(t.as_u64(), 2_000_000);

        let ms = Tick::from_millis(15);
        assert_eq!(ms.as_micros(), 15_000);
        assert_eq!(ms.as_millis(), 15);

        let us = Tick::from_micros(950);
        assert_eq!(us.as_micros(), 950);
        assert_eq!(us.as_millis(), 0);
    }
}
