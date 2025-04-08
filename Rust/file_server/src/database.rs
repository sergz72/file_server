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
    version: u32,
    data: BTreeMap<usize, Vec<u8>>
}

impl KeyValue {
    pub fn from(data: Vec<u8>) -> Result<Vec<KeyValue>, Error> {
        let l = data.len();
        if l < 4 {
            return Err(Error::new(ErrorKind::InvalidInput, "data is too short"));
        }
        let mut length = u32::from_le_bytes(data[0..4].try_into().unwrap());
        let mut idx = 4;
        let mut result = Vec::new();
        while length > 0 {
            if l < idx + 8 {
                return Err(Error::new(ErrorKind::InvalidInput, "data is too short"));
            }
            let key = u32::from_le_bytes(data[idx..idx + 4].try_into().unwrap()) as usize;
            idx += 4;
            let value_length = u32::from_le_bytes(data[idx..idx + 4].try_into().unwrap()) as usize;
            idx += 4;
            let value = Vec::from(&data[idx..idx + value_length]);
            idx += value_length;
            length -= 1;
            result.push(KeyValue{key, value});
        }
        if idx != l {
            return Err(Error::new(ErrorKind::InvalidInput, "incorrect data size"));
        }
        Ok(result)
    }
    
    pub fn to_binary(&self) -> Vec<u8> {
        let mut result = Vec::new();
        result.extend_from_slice(&(self.key as u32).to_le_bytes());
        result.extend_from_slice(&(self.value.len() as u32).to_le_bytes());
        result.extend_from_slice(&self.value);
        result
    }
}

impl Database {
    pub fn new(base_folder: PathBuf, hash_divider: usize) -> Result<Database, Error> {
        let data = load_data(&base_folder)?;
        Ok(Database{base_folder, hash_divider, data, version: 1})
    }

    pub fn get_version(&self) -> u32 {
        self.version
    }
    
    pub fn get(&self, key1: usize, key2: usize) -> Vec<KeyValueRef> {
        self.data.range(key1..=key2)
            .map(|(k, value)|KeyValueRef{key: *k, value})
            .collect()
    }

    pub fn get_last(&self, key: usize) -> Option<KeyValueRef> {
        self.data.range(0..=key).last()
            .map(|(k, value)|KeyValueRef{key: *k, value})
    }
    
    pub fn set(&mut self, expected_version: u32, data: Vec<KeyValue>) -> Result<(), Error> {
        if expected_version != self.version {
            return Err(Error::new(ErrorKind::InvalidData, "version mismatch"));
        }
        self.version += 1;
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