//! Outbound peer requests: deadlines, [`Event::PeerLost`] failures and data-buffer release.

use std::collections::BTreeMap;

use crate::buf::IoBuf;
use crate::event::{Action, Event, Msg, NodeId, Tick, TimerId};

/// Caller-allocated identifier of an outbound request tracked by [`PeerRequests`].
#[repr(transparent)]
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct RequestId(pub u64);

/// Reason an outbound peer request failed.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum PeerRequestFailure {
    /// Connection to the target peer failed with [`Event::PeerLost`]; the driver has released the data buffer.
    PeerLost,
    /// Deadline expired; the driver may still hold the data buffer until a reply or [`Event::PeerLost`].
    Timeout,
}

/// Outbound request in flight to a peer.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct PeerRequest {
    /// Target peer of the request.
    pub peer: NodeId,
    /// Timer guarding the request deadline.
    pub timer: TimerId,
    /// Tick at which the request times out.
    pub deadline: Tick,
    /// Optional data buffer handle carried by the request.
    pub data: Option<IoBuf>,
}

/// Outbound peer request failed by [`PeerRequests::on_event`].
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct FailedPeerRequest {
    /// Identifier of the failed request.
    pub id: RequestId,
    /// The failed request.
    pub request: PeerRequest,
    /// Failure cause.
    pub reason: PeerRequestFailure,
}

/// Tracks outbound peer requests; a timed-out one stays until `complete` or `PeerLost` frees its buffer.
#[derive(Clone, Debug, Default)]
pub struct PeerRequests {
    in_flight: BTreeMap<RequestId, PeerRequest>,
    by_timer: BTreeMap<TimerId, RequestId>,
}

impl PeerRequests {
    /// Creates an empty outbound request tracker.
    pub const fn new() -> Self {
        Self {
            in_flight: BTreeMap::new(),
            by_timer: BTreeMap::new(),
        }
    }

    /// Registers request `id`, already encoded in `msg`, with a deadline on the caller-allocated `timer`.
    pub fn send(
        &mut self,
        now: Tick,
        timeout_ticks: u64,
        id: RequestId,
        timer: TimerId,
        peer: NodeId,
        msg: Msg,
    ) -> [Action; 2] {
        assert!(
            !self.in_flight.contains_key(&id),
            "{id:?} is already in flight"
        );
        assert!(
            !self.by_timer.contains_key(&timer),
            "{timer:?} already guards an in-flight request"
        );
        let deadline = now.saturating_add(timeout_ticks);
        self.in_flight.insert(
            id,
            PeerRequest {
                peer,
                timer,
                deadline,
                data: msg.data,
            },
        );
        self.by_timer.insert(timer, id);
        [
            Action::NetSend(peer, msg),
            Action::SetTimer(timer, deadline),
        ]
    }

    /// Marks `id` answered and returns it, or `None` if it is not tracked; its data buffer is then free.
    pub fn complete(&mut self, id: RequestId) -> Option<PeerRequest> {
        let request = self.in_flight.remove(&id)?;
        if self.by_timer.get(&request.timer) == Some(&id) {
            self.by_timer.remove(&request.timer);
        }
        Some(request)
    }

    /// Total number of tracked requests, timed-out ones included.
    pub fn in_flight_len(&self) -> usize {
        self.in_flight.len()
    }

    /// Number of tracked requests to `peer`, timed-out ones included.
    pub fn in_flight_for_peer(&self, peer: NodeId) -> usize {
        self.in_flight.values().filter(|r| r.peer == peer).count()
    }

    /// Processes [`Event::PeerLost`] and [`Event::Timer`] at `now`, failing matching requests.
    pub fn on_event(&mut self, now: Tick, ev: Event) -> Vec<FailedPeerRequest> {
        match ev {
            Event::PeerLost(peer) => {
                let ids: Vec<RequestId> = self
                    .in_flight
                    .iter()
                    .filter_map(|(&id, r)| (r.peer == peer).then_some(id))
                    .collect();
                ids.into_iter()
                    .filter_map(|id| {
                        self.complete(id).map(|request| FailedPeerRequest {
                            id,
                            request,
                            reason: PeerRequestFailure::PeerLost,
                        })
                    })
                    .collect()
            }
            Event::Timer(timer) => match self.by_timer.get(&timer) {
                Some(&id) if now >= self.in_flight[&id].deadline => {
                    self.by_timer.remove(&timer);
                    vec![FailedPeerRequest {
                        id,
                        request: self.in_flight[&id],
                        reason: PeerRequestFailure::Timeout,
                    }]
                }
                _ => Vec::new(),
            },
            Event::NetRecv(_, _) | Event::DiskDone(_, _) => Vec::new(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::event::ControlMsg;

    fn request(peer: NodeId, timer: u64, deadline: u64, data: Option<IoBuf>) -> PeerRequest {
        PeerRequest {
            peer,
            timer: TimerId(timer),
            deadline: Tick(deadline),
            data,
        }
    }

    #[test]
    fn peer_lost_fails_all_outstanding_requests_for_that_peer_only() {
        let mut tracker = PeerRequests::new();
        let peer_a = NodeId(10);
        let peer_b = NodeId(20);
        let buf1 = IoBuf::new(0, 1, 4096);
        let buf2 = IoBuf::new(1, 1, 4096);

        let msg1 = Msg::with_data(ControlMsg::new(b"r1").unwrap(), buf1);
        let actions1 = tracker.send(Tick(100), 50, RequestId(1), TimerId(1), peer_a, msg1);
        assert_eq!(
            actions1,
            [
                Action::NetSend(peer_a, msg1),
                Action::SetTimer(TimerId(1), Tick(150)),
            ]
        );

        let msg2 = Msg::control(b"r2").unwrap();
        tracker.send(Tick(110), 50, RequestId(2), TimerId(2), peer_a, msg2);
        tracker.send(
            Tick(115),
            50,
            RequestId(3),
            TimerId(3),
            peer_b,
            Msg::with_data(ControlMsg::new(b"r3").unwrap(), buf2),
        );

        assert_eq!(tracker.in_flight_for_peer(peer_a), 2);
        assert_eq!(tracker.in_flight_for_peer(peer_b), 1);

        let failed = tracker.on_event(Tick(120), Event::PeerLost(peer_a));
        assert_eq!(
            failed,
            vec![
                FailedPeerRequest {
                    id: RequestId(1),
                    request: request(peer_a, 1, 150, Some(buf1)),
                    reason: PeerRequestFailure::PeerLost,
                },
                FailedPeerRequest {
                    id: RequestId(2),
                    request: request(peer_a, 2, 160, None),
                    reason: PeerRequestFailure::PeerLost,
                },
            ]
        );
        assert_eq!(tracker.in_flight_for_peer(peer_a), 0);
        assert_eq!(tracker.in_flight_for_peer(peer_b), 1);

        assert_eq!(
            tracker.on_event(Tick(150), Event::Timer(TimerId(1))),
            Vec::new()
        );
    }

    #[test]
    fn timed_out_request_keeps_its_buffer_until_reply_or_peer_lost() {
        let mut tracker = PeerRequests::new();
        let peer = NodeId(5);
        let buf1 = IoBuf::new(0, 1, 4096);
        let buf2 = IoBuf::new(1, 1, 4096);
        tracker.send(
            Tick(100),
            50,
            RequestId(1),
            TimerId(1),
            peer,
            Msg::data_only(buf1),
        );
        tracker.send(
            Tick(100),
            50,
            RequestId(2),
            TimerId(2),
            peer,
            Msg::data_only(buf2),
        );

        let timed_out = tracker.on_event(Tick(150), Event::Timer(TimerId(1)));
        assert_eq!(
            timed_out,
            vec![FailedPeerRequest {
                id: RequestId(1),
                request: request(peer, 1, 150, Some(buf1)),
                reason: PeerRequestFailure::Timeout,
            }]
        );
        assert_eq!(tracker.in_flight_len(), 2);
        assert_eq!(
            tracker.on_event(Tick(160), Event::Timer(TimerId(1))),
            Vec::new()
        );

        assert_eq!(
            tracker.complete(RequestId(1)),
            Some(request(peer, 1, 150, Some(buf1)))
        );

        tracker.on_event(Tick(150), Event::Timer(TimerId(2)));
        tracker.send(
            Tick(155),
            50,
            RequestId(3),
            TimerId(2),
            peer,
            Msg::control(b"r3").unwrap(),
        );
        let lost = tracker.on_event(Tick(160), Event::PeerLost(peer));
        assert_eq!(
            lost.iter().map(|f| (f.id, f.reason)).collect::<Vec<_>>(),
            vec![
                (RequestId(2), PeerRequestFailure::PeerLost),
                (RequestId(3), PeerRequestFailure::PeerLost),
            ]
        );
        assert_eq!(lost[0].request.data, Some(buf2));
        assert_eq!(tracker.in_flight_len(), 0);
    }

    #[test]
    fn completed_request_ignores_later_timer_and_peer_lost() {
        let mut tracker = PeerRequests::new();
        let peer = NodeId(5);
        let buf = IoBuf::new(2, 1, 4096);
        tracker.send(
            Tick(10),
            25,
            RequestId(7),
            TimerId(1),
            peer,
            Msg::data_only(buf),
        );

        assert_eq!(
            tracker.on_event(Tick(40), Event::Timer(TimerId(2))),
            Vec::new()
        );
        assert_eq!(tracker.in_flight_len(), 1);

        assert_eq!(
            tracker.complete(RequestId(7)),
            Some(request(peer, 1, 35, Some(buf)))
        );
        assert_eq!(tracker.complete(RequestId(7)), None);
        assert_eq!(
            tracker.on_event(Tick(40), Event::Timer(TimerId(1))),
            Vec::new()
        );
        assert_eq!(
            tracker.on_event(Tick(40), Event::PeerLost(peer)),
            Vec::new()
        );
    }

    #[test]
    fn stale_timer_from_reused_id_does_not_fail_new_request() {
        let mut tracker = PeerRequests::new();
        let peer = NodeId(5);
        let timer = TimerId(1);
        let msg = Msg::control(b"r").unwrap();

        tracker.send(Tick(100), 50, RequestId(1), timer, peer, msg);
        tracker.complete(RequestId(1));
        tracker.send(Tick(130), 50, RequestId(2), timer, peer, msg);

        assert_eq!(tracker.on_event(Tick(150), Event::Timer(timer)), Vec::new());
        assert_eq!(tracker.in_flight_len(), 1);

        let timed_out = tracker.on_event(Tick(180), Event::Timer(timer));
        assert_eq!(timed_out.len(), 1);
        assert_eq!(timed_out[0].id, RequestId(2));
    }
}
