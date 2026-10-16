use redb::{ReadableDatabase, TableDefinition};

const T: TableDefinition<&[u8], &[u8]> = TableDefinition::new("t");

fn main() {
    let path = std::env::args().nth(1).unwrap_or_else(|| "x.redb".into());
    let db = redb::Database::create(&path).unwrap();
    let tx = db.begin_write().unwrap();
    tx.open_table(T).unwrap().insert(b"k".as_slice(), b"v".as_slice()).unwrap();
    tx.commit().unwrap();
    let rt = db.begin_read().unwrap().open_table(T).unwrap();
    println!("{:?}", rt.get(b"k".as_slice()).unwrap().map(|v| v.value().to_vec()));
}
