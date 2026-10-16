use fjall::{KeyspaceCreateOptions, PersistMode};

fn main() {
    let path = std::env::args().nth(1).unwrap_or_else(|| "x.fjall".into());
    let db = fjall::Database::builder(&path).open().unwrap();
    let ks = db.keyspace("t", KeyspaceCreateOptions::default).unwrap();
    let mut b = db.batch().durability(Some(PersistMode::SyncData));
    b.insert(&ks, "k", "v");
    b.commit().unwrap();
    println!("{:?}", ks.get("k").unwrap().map(|v| v.to_vec()));
}
