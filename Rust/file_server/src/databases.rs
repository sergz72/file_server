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

    pub fn get(&self, database: String, key1: usize, key2: usize) -> Vec<KeyValue> {
        match self.data.get(&database) {
            Some(data) => {
                let lock = data.read().unwrap();
                let value = lock.get(key1, key2);
                value.iter()
                    .map(|v|KeyValue{key: v.key, value: v.value.clone()})
                    .collect()
            },
            None => Vec::new()
        }
    }

    pub fn set(&mut self, database: String, data: Vec<KeyValue>) -> Result<(), Error> {
        match self.data.get(&database) {
            Some(db) => {
                let mut lock = db.write().unwrap();
                lock.set(data)
            },
            None => {
                let path = self.base_path.join(&database);
                fs::create_dir(&path)?;
                let mut db = Database::new(path, self.hash_divider)?;
                db.set(data)?;
                self.data.insert(database.clone(), RwLock::new(db));
                Ok(())
            }
        }
    }
}