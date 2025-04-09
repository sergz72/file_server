use std::collections::HashMap;
use std::fs;
use std::io::Error;
use std::path::PathBuf;
use std::sync::RwLock;
use crate::database::{Database, KeyValue};

pub struct Databases {
    base_path: PathBuf,
    hash_divider: usize,
    data: HashMap<String, RwLock<Database>>
}

impl Databases {
    pub fn new(base_folder: String, hash_divider: usize) -> Result<Databases, Error> {
        let base_path = PathBuf::from(base_folder);
        let contents = fs::read_dir(&base_path)?;
        let mut result = HashMap::new();
        for entry_result in contents {
            let entry = entry_result?;
            if entry.file_type()?.is_dir() {
                result.insert(entry.file_name().into_string().unwrap(),
                              RwLock::new(Database::new(entry.path(), hash_divider)?));
            }
        }
        Ok(Databases{data: result, base_path, hash_divider})
    }

    pub fn get(&self, database: String, key1: usize, key2: usize) -> (u32, Vec<KeyValue>) {
        match self.data.get(&database) {
            Some(data) => {
                let lock = data.read().unwrap();
                let value = lock.get(key1, key2);
                let result = value.iter()
                    .map(|v|KeyValue{key: v.key, version: v.version, value: v.value.clone()})
                    .collect();
                (lock.get_version(), result)
            },
            None => (1, Vec::new())
        }
    }

    pub fn get_last(&self, database: String, key1: usize, key2: usize) -> (u32, Option<KeyValue>) {
        match self.data.get(&database) {
            Some(data) => {
                let lock = data.read().unwrap();
                let value = lock.get_last(key1, key2);
                let result = value
                    .map(|v|KeyValue{key: v.key, version: v.version, value: v.value.clone()});
                (lock.get_version(), result)
            },
            None => (1, None)
        }
    }
    
    pub fn get_file_version(&self, database: String, key: usize) -> (u32, Option<u32>) {
        match self.data.get(&database) {
            Some(data) => {
                let lock = data.read().unwrap();
                (lock.get_version(), lock.get_file_version(key))
            },
            None => (1, None)
        }
    }
    
    pub fn set(&mut self, database: String, expected_version: u32, data: Vec<KeyValue>)
        -> Result<(), Error> {
        match self.data.get(&database) {
            Some(db) => {
                let mut lock = db.write().unwrap();
                lock.set(expected_version, data)
            },
            None => {
                let path = self.base_path.join(&database);
                fs::create_dir(&path)?;
                let mut db = Database::new(path, self.hash_divider)?;
                db.set(expected_version, data)?;
                self.data.insert(database.clone(), RwLock::new(db));
                Ok(())
            }
        }
    }
}