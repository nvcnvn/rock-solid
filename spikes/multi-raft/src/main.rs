mod common;
mod or;
mod rr;

#[global_allocator]
static ALLOC: common::CountingAlloc = common::CountingAlloc;

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let o = common::Opts::parse(&args[2..]);
    match args[1].as_str() {
        "raftrs" => rr::bench(&o),
        "openraft" => or::bench(&o),
        "raftrs-sim" => rr::sim(&o),
        "openraft-sim" => or::sim(&o),
        x => panic!("unknown mode {x}"),
    }
}
