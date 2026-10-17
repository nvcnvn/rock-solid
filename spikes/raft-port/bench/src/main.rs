//! P4-P7 benchmark: raft-rs 0.7.0 and the etcd-raft port in the identical loop.
//! Usage: raft-port-bench raftrs|port|sim-port|sizes [--groups N --hb MS --elect A-B --idle S --write S ...]

mod common;
mod harness;
mod pr;
mod rr;

#[global_allocator]
static ALLOC: common::CountingAlloc = common::CountingAlloc;

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let o = common::Opts::parse(&args[2..]);
    match args[1].as_str() {
        "raftrs" => harness::bench::<rr::RrReplica>("raft-rs", &o),
        "port" => harness::bench::<pr::PortReplica>("eraft", &o),
        "sim-port" => harness::sim::<pr::PortReplica>("eraft", &o),
        "sizes" => sizes(),
        x => panic!("unknown mode {x}"),
    }
}

fn sizes() {
    use std::mem::size_of;
    println!(
        "P5 struct sizes (bytes): eraft RawNode<MemoryStorage> {}, Raft {}, Progress {}, Message {}, Entry {}, MemoryStorage {}",
        size_of::<eraft::RawNode<eraft::MemoryStorage>>(),
        size_of::<eraft::Raft<eraft::MemoryStorage>>(),
        size_of::<eraft::tracker::Progress>(),
        size_of::<eraft::raftpb::Message>(),
        size_of::<eraft::raftpb::Entry>(),
        size_of::<eraft::MemoryStorage>(),
    );
    println!(
        "P5 struct sizes (bytes): raft-rs RawNode<MemStorage> {}, Raft {}, Progress {}, Message {}, Entry {}",
        size_of::<raft::RawNode<raft::storage::MemStorage>>(),
        size_of::<raft::Raft<raft::storage::MemStorage>>(),
        size_of::<raft::Progress>(),
        size_of::<raft::prelude::Message>(),
        size_of::<raft::prelude::Entry>(),
    );
}
