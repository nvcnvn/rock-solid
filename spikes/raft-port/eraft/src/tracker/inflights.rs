// Copyright 2019 The etcd Authors
// Ported from go.etcd.io/raft v3.7.0 (b867cf13f6bc0dae21204302df97bc2355c3af55),
// tracker/inflights.go. Licensed under the Apache License 2.0.

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(crate) struct Inflight {
    pub(crate) index: u64,
    pub(crate) bytes: u64,
}

/// Limits the number and byte size of in-flight MsgApp messages to a follower.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Inflights {
    pub(crate) start: usize,
    pub(crate) count: usize,
    pub(crate) bytes: u64,
    pub(crate) size: usize,
    pub(crate) max_bytes: u64,
    pub(crate) buffer: Vec<Inflight>,
}

impl Inflights {
    /// Allows up to `size` messages and `max_bytes` bytes (0 = no byte limit).
    pub fn new(size: usize, max_bytes: u64) -> Inflights {
        Inflights {
            size,
            max_bytes,
            ..Default::default()
        }
    }

    /// Records a new message carrying entries up to `index` with `bytes` bytes.
    pub fn add(&mut self, index: u64, bytes: u64) {
        if self.full() {
            panic!("cannot add into a Full inflights");
        }
        let mut next = self.start + self.count;
        let size = self.size;
        if next >= size {
            next -= size;
        }
        if next >= self.buffer.len() {
            self.grow();
        }
        self.buffer[next] = Inflight { index, bytes };
        self.count += 1;
        self.bytes += bytes;
    }

    fn grow(&mut self) {
        let mut new_size = self.buffer.len() * 2;
        if new_size == 0 {
            new_size = 1;
        } else if new_size > self.size {
            new_size = self.size;
        }
        self.buffer.resize(new_size, Inflight::default());
    }

    /// Frees the inflights smaller or equal to `to`.
    pub fn free_le(&mut self, to: u64) {
        if self.count == 0 || to < self.buffer[self.start].index {
            return;
        }
        let mut idx = self.start;
        let mut i = 0;
        let mut bytes = 0;
        while i < self.count {
            if to < self.buffer[idx].index {
                break;
            }
            bytes += self.buffer[idx].bytes;
            idx += 1;
            if idx >= self.size {
                idx -= self.size;
            }
            i += 1;
        }
        self.count -= i;
        self.bytes -= bytes;
        self.start = idx;
        if self.count == 0 {
            self.start = 0;
        }
    }

    /// True if no more messages can be sent.
    pub fn full(&self) -> bool {
        self.count == self.size || (self.max_bytes != 0 && self.bytes >= self.max_bytes)
    }

    /// Number of in-flight messages.
    pub fn count(&self) -> usize {
        self.count
    }

    pub(crate) fn reset(&mut self) {
        self.start = 0;
        self.count = 0;
        self.bytes = 0;
    }
}
