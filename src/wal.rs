
use std::{fs::{read_dir, remove_file, OpenOptions}, io::{self, BufReader, BufWriter, Read, Write}, mem, time::{SystemTime, UNIX_EPOCH}};

use crate::memtable::{MemTable, MemTableEntry};
use std::fs::File;
use std::path::{Path,PathBuf};

pub struct WAL{
    file: BufWriter<File>,
    path: PathBuf
}

impl WAL{
    pub fn new(dir:&Path) -> io::Result<WAL>{
        let timestamp = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_micros();
        let path = Path::new(dir).join(timestamp.to_string()+".wal");
        let file = OpenOptions::new().append(true).create(true).open(&path)?;
        let file = BufWriter::new(file);
        Ok(WAL{path,file})
    }

    pub fn load(path:&Path) -> io::Result<WAL>{
        let file = OpenOptions::new().append(true).open(path)?;
        let file = BufWriter::new(file);
        Ok(WAL{path:path.to_owned(),file})
    }

    pub fn set(&mut self,key: &[u8], value:&[u8],timestamsp:u128)->io::Result<()>{
        self.file.write_all(&key.len().to_le_bytes())?;
        self.file.write_all(&(false as u8).to_le_bytes())?;
        self.file.write_all(&value.len().to_le_bytes())?;
        self.file.write_all(key)?;
        self.file.write_all(value)?;
        self.file.write_all(&timestamsp.to_le_bytes())?;
        Ok(())
    }

    pub fn delete(&mut self, key:&[u8], timestamp: u128)->io::Result<()>{
        self.file.write_all(&key.len().to_le_bytes())?;
        self.file.write_all(&(true as u8).to_le_bytes())?;
        self.file.write(key)?;
        self.file.write_all(&timestamp.to_le_bytes())?;
        Ok(())
    }

    pub fn flush_to_disk(&mut self)-> io::Result<()>{
        self.file.flush()
    }

    pub fn load_dir(path: &Path)-> io::Result<(WAL,MemTable)>{
        let mut files = Vec::new();
        for file in read_dir(path).unwrap(){
            let path = file.unwrap().path();
            print!("{:?}",path);
            if path.extension().unwrap() == "wal"{
                files.push(path);
            }
        }
        files.sort();
        let mut mem_table = MemTable::new();
        let mut wal = WAL::new(path)?;
        for wal_file in files.iter() {
            if let Ok(old_wal) = WAL::load(wal_file) {
            for entry in old_wal.into_iter() {
                if entry.data.deleted {
                mem_table.delete(entry.data.key.as_slice(), entry.data.timestamp);
                wal.delete(entry.data.key.as_slice(), entry.data.timestamp)?;
                } else {
                mem_table.insert(
                    entry.data.key.as_slice(),
                    entry.data.value.as_ref().unwrap().as_slice(),
                    entry.data.timestamp,
                );
                wal.set(
                    entry.data.key.as_slice(),
                    entry.data.value.unwrap().as_slice(),
                    entry.data.timestamp,
                )?;
                }
            }
            }
        }
        wal.flush_to_disk().unwrap();
        files.into_iter().for_each(|f| remove_file(f).unwrap());

        Ok((wal, mem_table))
    }
}

pub struct WALEntry{
    data: MemTableEntry
}

pub struct WALIter{
    // Key size (8B), Tomstone (1B), Val SIZE (8b), key, value, timestamp(16B)
    reader: BufReader<File>
}

impl IntoIterator for WAL {
    type IntoIter = WALIter;
    type Item = WALEntry;
  
    fn into_iter(self) -> WALIter {
        WALIter::new(self.path).unwrap()
    }
  }

  
impl WALIter{
    pub fn new(path: PathBuf)-> io::Result<WALIter>{
        let file = OpenOptions::new().read(true).open(path)?;
        let reader = BufReader::new(file);
        Ok(WALIter { reader })
    }
}
impl Iterator for WALIter{
    type Item = WALEntry;

    fn next(&mut self)-> Option<WALEntry>{
        let mut len_buf = [0;8];
        if self.reader.read_exact(&mut len_buf).is_err(){
            return None;
        }
        let key_size = usize::from_le_bytes(len_buf);
        let mut bool_buf = [0;1];
        if self.reader.read_exact(&mut bool_buf).is_err(){
            return None;
        }
        let is_deleted = bool_buf[0]!=0;
        let mut key = vec![0; key_size];
        let mut value = None;
        if is_deleted{
            if self.reader.read_exact(&mut key).is_err(){
                return None;
            }
        } else {
            if self.reader.read_exact(&mut len_buf).is_err(){
                return None;
            }
            let value_size = usize::from_le_bytes(len_buf);
            if self.reader.read_exact(&mut key).is_err(){
                return None;
            }
            let mut value_buf = vec![0;value_size];
            if self.reader.read_exact(&mut value_buf).is_err(){
                return None;
            }
            value = Some(value_buf);
        }
        let mut timestamp_buf = [0;16];
        if self.reader.read_exact(&mut timestamp_buf).is_err(){
            return None;
        }
        let timestamp = u128::from_le_bytes(timestamp_buf);
        Some(WALEntry { 
            data: MemTableEntry{
            key,
            value,
            timestamp,
            deleted:is_deleted
            }
        })
    }
}