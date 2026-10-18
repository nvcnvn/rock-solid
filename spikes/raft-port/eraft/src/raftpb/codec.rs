// Copyright The etcd Authors
// Ported from go.etcd.io/raft v3.7.0 (b867cf13f6bc0dae21204302df97bc2355c3af55),
// raftpb/raft.proto. Licensed under the Apache License 2.0.
//
// Deviation: Go uses generated protobuf code. This hand-written codec covers
// only ConfChange and ConfChangeV2, the two messages raft itself encodes into
// entry data. It is wire-compatible with the Go encoding for the fields the Go
// callers set (type, node_id and transition are always written).

use std::fmt;

use super::{ConfChange, ConfChangeSingle, ConfChangeTransition, ConfChangeType, ConfChangeV2};

/// A malformed protobuf payload.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DecodeError(pub String);

impl fmt::Display for DecodeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

pub(crate) fn varint_len(mut v: u64) -> u64 {
    let mut n = 1;
    while v >= 0x80 {
        v >>= 7;
        n += 1;
    }
    n
}

fn put_varint(buf: &mut Vec<u8>, mut v: u64) {
    while v >= 0x80 {
        buf.push((v as u8) | 0x80);
        v >>= 7;
    }
    buf.push(v as u8);
}

fn put_bytes(buf: &mut Vec<u8>, field: u64, b: &[u8]) {
    put_varint(buf, (field << 3) | 2);
    put_varint(buf, b.len() as u64);
    buf.extend_from_slice(b);
}

fn put_uint(buf: &mut Vec<u8>, field: u64, v: u64) {
    put_varint(buf, field << 3);
    put_varint(buf, v);
}

struct Reader<'a> {
    b: &'a [u8],
}

enum Field<'a> {
    Varint(u64),
    Bytes(&'a [u8]),
}

impl<'a> Reader<'a> {
    fn varint(&mut self) -> Result<u64, DecodeError> {
        let mut v = 0u64;
        for i in 0..10 {
            let Some((&c, rest)) = self.b.split_first() else {
                return Err(DecodeError("proto: unexpected EOF".into()));
            };
            self.b = rest;
            v |= u64::from(c & 0x7f) << (7 * i);
            if c < 0x80 {
                return Ok(v);
            }
        }
        Err(DecodeError("proto: integer overflow".into()))
    }

    fn next(&mut self) -> Result<Option<(u64, Field<'a>)>, DecodeError> {
        if self.b.is_empty() {
            return Ok(None);
        }
        let key = self.varint()?;
        let (num, wire) = (key >> 3, key & 7);
        let f = match wire {
            0 => Field::Varint(self.varint()?),
            2 => {
                let n = self.varint()? as usize;
                if n > self.b.len() {
                    return Err(DecodeError("proto: unexpected EOF".into()));
                }
                let (v, rest) = self.b.split_at(n);
                self.b = rest;
                Field::Bytes(v)
            }
            1 => {
                if self.b.len() < 8 {
                    return Err(DecodeError("proto: unexpected EOF".into()));
                }
                self.b = &self.b[8..];
                Field::Varint(0)
            }
            5 => {
                if self.b.len() < 4 {
                    return Err(DecodeError("proto: unexpected EOF".into()));
                }
                self.b = &self.b[4..];
                Field::Varint(0)
            }
            _ => {
                return Err(DecodeError(format!(
                    "proto: cannot parse invalid wire-format data (wire type {wire})"
                )));
            }
        };
        Ok(Some((num, f)))
    }
}

fn change_type(v: u64) -> Result<ConfChangeType, DecodeError> {
    ConfChangeType::from_i32(v)
        .ok_or_else(|| DecodeError(format!("proto: invalid ConfChangeType {v}")))
}

pub(crate) fn encode_conf_change(c: &ConfChange) -> Vec<u8> {
    let mut b = Vec::new();
    if c.id != 0 {
        put_uint(&mut b, 1, c.id);
    }
    put_uint(&mut b, 2, c.change_type as u64);
    put_uint(&mut b, 3, c.node_id);
    if !c.context.is_empty() {
        put_bytes(&mut b, 4, &c.context);
    }
    b
}

pub(crate) fn decode_conf_change(data: &[u8]) -> Result<ConfChange, DecodeError> {
    let mut c = ConfChange::default();
    let mut r = Reader { b: data };
    while let Some((num, f)) = r.next()? {
        match (num, f) {
            (1, Field::Varint(v)) => c.id = v,
            (2, Field::Varint(v)) => c.change_type = change_type(v)?,
            (3, Field::Varint(v)) => c.node_id = v,
            (4, Field::Bytes(v)) => c.context = v.to_vec(),
            _ => {}
        }
    }
    Ok(c)
}

pub(crate) fn encode_conf_change_v2(c: &ConfChangeV2) -> Vec<u8> {
    let mut b = Vec::new();
    put_uint(&mut b, 1, c.transition as u64);
    for cc in &c.changes {
        let mut sub = Vec::new();
        put_uint(&mut sub, 1, cc.change_type as u64);
        put_uint(&mut sub, 2, cc.node_id);
        put_bytes(&mut b, 2, &sub);
    }
    if !c.context.is_empty() {
        put_bytes(&mut b, 3, &c.context);
    }
    b
}

pub(crate) fn decode_conf_change_v2(data: &[u8]) -> Result<ConfChangeV2, DecodeError> {
    let mut c = ConfChangeV2::default();
    let mut r = Reader { b: data };
    while let Some((num, f)) = r.next()? {
        match (num, f) {
            (1, Field::Varint(v)) => {
                c.transition = match v {
                    0 => ConfChangeTransition::ConfChangeTransitionAuto,
                    1 => ConfChangeTransition::ConfChangeTransitionJointImplicit,
                    2 => ConfChangeTransition::ConfChangeTransitionJointExplicit,
                    _ => {
                        return Err(DecodeError(format!(
                            "proto: invalid ConfChangeTransition {v}"
                        )));
                    }
                }
            }
            (2, Field::Bytes(sub)) => {
                let mut cc = ConfChangeSingle::default();
                let mut sr = Reader { b: sub };
                while let Some((n, f)) = sr.next()? {
                    match (n, f) {
                        (1, Field::Varint(v)) => cc.change_type = change_type(v)?,
                        (2, Field::Varint(v)) => cc.node_id = v,
                        _ => {}
                    }
                }
                c.changes.push(cc);
            }
            (3, Field::Bytes(v)) => c.context = v.to_vec(),
            _ => {}
        }
    }
    Ok(c)
}
