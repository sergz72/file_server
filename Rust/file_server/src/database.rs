use std::collections::BTreeMap;
use std::fs;
use std::io::{Error, ErrorKind};
use std::path::PathBuf;

pub struct KeyValue {
    pub key: usize,
    pub value: Vec<u8>,
}

pub struct KeyValueRef<'a> {
    pub key: usize,
    pub value: &'a Vec<u8>,
}

pub struct Database {
    base_folder: PathBuf,
    hash_divider: usize,
    data: BTreeMap<usize, Vec<u8>>
}

impl KeyValue {
    pub fn from(data: &[u8]) -> Result<Vec<KeyValue>, Error> {
        todo!()
    }
    
    pub fn to_binary(&self) -> Vec<u8> {
        todo!()
    }
}

impl Database {
    pub fn new(base_folder: PathBuf, hash_divider: usize) -> Result<Database, Error> {
        let data = load_data(&base_folder)?;
        Ok(Database{base_folder, hash_divider, data})
    }

    pub fn get(&self, key1: usize, key2: usize) -> Vec<KeyValueRef> {
        self.data.range(key1..=key2)
            .map(|(k, value)|KeyValueRef{key: *k, value})
            .collect()
    }

    pub fn set(&mut self, data: Vec<KeyValue>) -> Result<(), Error> {
        for kv in data {
            self.save(kv.key, &kv.value)?;
            self.data.insert(kv.key, kv.value);
        }
        Ok(())
    }

    fn save(&self, key: usize, value: &Vec<u8>) -> Result<(), Error> {
        let file_path = self.build_file_path(key)?;
        fs::write(file_path, value)
    }

    fn build_file_path(&self, key: usize) -> Result<PathBuf, Error> {
        let folder_name = (key / self.hash_divider).to_string();
        let folder = self.base_folder.join(folder_name);
        if !fs::exists(&folder)? {
            fs::create_dir(&folder)?;
        };
        Ok(folder.join(key.to_string()))
    }
}

fn load_data(base_folder: &PathBuf) -> Result<BTreeMap<usize, Vec<u8>>, Error> {
    let contents = fs::read_dir(base_folder)?;
    let mut result = BTreeMap::new();
    for entry_result in contents {
        let entry = entry_result?;
        if entry.file_type()?.is_dir() {
            let files = fs::read_dir(entry.path())?;
            for file_result in files {
                let file = file_result?;
                if file.file_type()?.is_file() {
                    let key = file.file_name().into_string().unwrap().parse::<usize>()
                        .map_err(|e|Error::new(ErrorKind::InvalidData, e))?;
                    let data = fs::read(file.path())?;
                    result.insert(key, data);
                }
            }
        }
    }
    Ok(result)
}