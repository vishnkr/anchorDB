
pub struct MemTable{
    entries: Vec<MemTableEntry>,
    size: usize
}

pub struct MemTableEntry{
pub key: Vec<u8>,
pub value: Option<Vec<u8>>,
pub timestamp: u128,
pub deleted: bool,
}

impl MemTable{
    pub fn new() -> MemTable{
        MemTable {
            entries: Vec::new(),
            size:0
        }
    }

    fn get_key_index(&self, key: &[u8])-> Result<usize,usize>{
        self.entries.binary_search_by_key(&key, |entry| entry.key.as_slice())
    }

    pub fn get(&self, key: &[u8])-> Option<&MemTableEntry>{
        if let Ok(idx) = self.get_key_index(key){
            return Some(&self.entries[idx]);
        }
        None
    }

    pub fn insert(&mut self, key: &[u8], value: &[u8],timestamp:u128){
        let mt_entry = MemTableEntry{
            key: key.to_owned(),
            value:Some(value.to_owned()),
            timestamp,
            deleted: false
        };
        match self.get_key_index(key){
            Ok(idx)=>{
                if let Some(v) = self.entries[idx].value.as_ref(){
                    if value.len()<v.len(){
                        self.size -= v.len()-value.len();
                    } else {
                        self.size += value.len()-v.len();
                    }
                }
                self.entries[idx]=mt_entry;
            }
            Err(idx)=>{
                self.size += key.len() + value.len() + 16 + 1;
                self.entries.insert(idx, mt_entry)
            }
        }
    }

    pub fn delete(&mut self, key:&[u8], timestamp:u128){
        let mt_entry = MemTableEntry{
            key: key.to_owned(),
            value: None,
            timestamp: timestamp,
            deleted:true
        };
         match self.get_key_index(key){
            Ok(idx)=>{
                if let Some(v) = self.entries[idx].value.as_ref(){
                    self.size-=v.len();
                }
                self.entries[idx] = mt_entry;
            },
            Err(idx)=>{
                self.size += key.len() + 16 + 1;
                self.entries.insert(idx,mt_entry);
            }
         }
    }
}