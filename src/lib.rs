
//mod error;
//mod skiplist;
mod memtable;
mod wal;
//use error::*;
use crate::{memtable::MemTable, wal::WAL};
use std::fs::{create_dir, create_dir_all};
use std::{path::PathBuf, time::{SystemTime, UNIX_EPOCH}};

pub struct AnchorDB{
    dir: PathBuf,
    mem_table: MemTable,
    wal: WAL
}

pub struct DBEntry{
    key: Vec<u8>,
    value: Vec<u8>,
    timestamp: u128,
}
impl DBEntry {
    pub fn key(&self) -> &[u8] {
      &self.key
    }
  
    pub fn value(&self) -> &[u8] {
      &self.value
    }
  
    pub fn timestamp(&self) -> u128 {
      self.timestamp
    }
  }

impl AnchorDB{
    pub fn new()->AnchorDB{
        let dir = PathBuf::from("./anchor/wal/");
        if !dir.exists() {
            println!("Creating new WAL directory");
            create_dir_all(&dir).unwrap();
        }
        let dir = PathBuf::from(dir);
        let (wal,mem_table) = WAL::load_dir(&dir).unwrap();
        AnchorDB{
            dir,
            mem_table,
            wal
        }
    }

    pub fn get(&self, key:&[u8])->Option<DBEntry>{
        if let Some(entry) = self.mem_table.get(key){
            return Some(DBEntry{
                key: entry.key.clone(),
                value: entry.value.as_ref().unwrap().clone(),
                timestamp: entry.timestamp.clone()
            })
        }
        None
    }

    pub fn put(&mut self, key:&[u8],value:&[u8])->Result<usize,usize>{
        let timestamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_micros();

        let wal_res = self.wal.set(key, value, timestamp);
        if wal_res.is_err() {
        return Err(0);
        }
        if self.wal.flush_to_disk().is_err() {
        return Err(0);
        }

        self.mem_table.insert(key, value, timestamp);
        Ok(1)
    }
}