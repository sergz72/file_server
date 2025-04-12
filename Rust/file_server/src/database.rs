use std::collections::BTreeMap;
use std::fs;
use std::io::{Error, ErrorKind, Write};
use std::path::PathBuf;

#[derive(Clone)]
pub struct KeyValue {
    pub key: usize,
    pub version: u32,
    pub value: Vec<u8>,
}

pub struct KeyValueRef<'a> {
    pub key: usize,
    pub version: u32,
    pub value: &'a Vec<u8>,
}

struct File {
    version: u32,
    data: Vec<u8>
}

pub struct Database {
    base_folder: PathBuf,
    hash_divider: usize,
    version: u32,
    data: BTreeMap<usize, File>
}

impl KeyValue {
    pub fn from(data: &[u8]) -> Result<Vec<KeyValue>, Error> {
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
            result.push(KeyValue{key, version: 0, value});
        }
        if idx != l {
            return Err(Error::new(ErrorKind::InvalidInput, "incorrect data size"));
        }
        Ok(result)
    }
    
    pub fn to_binary(&self) -> Vec<u8> {
        let mut result = Vec::new();
        result.extend_from_slice(&self.version.to_le_bytes());
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
            .map(|(k, value)|KeyValueRef{key: *k, version: value.version, value: &value.data})
            .collect()
    }

    pub fn get_last(&self, key1: usize, key2: usize) -> Option<KeyValueRef> {
        self.data.range(key1..=key2).last()
            .map(|(k, value)|KeyValueRef{key: *k, version: value.version, value: &value.data})
    }
    
    pub fn set(&mut self, expected_version: u32, data: Vec<KeyValue>) -> Result<(), Error> {
        if expected_version != self.version {
            return Err(Error::new(ErrorKind::InvalidData, "version mismatch"));
        }
        self.version += 1;
        for kv in data {
            if kv.value.len() != 0 {
                let version = self.data.get(&kv.key).map(|f| f.version).unwrap_or(0) + 1;
                self.save(kv.key, version, &kv.value)?;
                self.data.insert(kv.key, File { version, data: kv.value });
            } else {
                self.delete(kv.key)?;
                self.data.remove(&kv.key);
            }
        }
        Ok(())
    }
    
    pub fn get_file_version(&self, key: usize) -> Option<u32> {
        self.data.get(&key).map(|f|f.version)
    }

    fn delete(&self, key: usize) -> Result<(), Error> {
        let file_path = self.build_file_path(key)?;
        if file_path.try_exists()? {
            fs::remove_file(file_path)
        } else {
            Ok(())
        }
    }

    fn save(&self, key: usize, version: u32, value: &Vec<u8>) -> Result<(), Error> {
        let file_path = self.build_file_path(key)?;
        let mut file = fs::File::create(file_path)?;
        file.write(&version.to_le_bytes())?;
        file.write(value)?;
        Ok(())
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

fn load_data(base_folder: &PathBuf) -> Result<BTreeMap<usize, File>, Error> {
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
                    let version = u32::from_le_bytes(data[0..4].try_into().unwrap());
                    result.insert(key, File{version, data: data[4..].to_vec()});
                }
            }
        }
    }
    Ok(result)
}

#[cfg(test)]
mod tests {
    use std::collections::{HashMap, HashSet};
    use std::{fs, panic};
    use std::io::Error;
    use std::path::PathBuf;
    use rand::rngs::OsRng;
    use rand::TryRngCore;
    use crate::database::{Database, KeyValue};

    const TEST_DB_PATH: &str = "/mnt/ramdisk/test_database";

    #[test]
    fn test_database() -> Result<(), Error>{
        panic::set_hook(Box::new(|v| {
            let info = v.to_string();
            println!("{}", info);
            fs::remove_dir_all(TEST_DB_PATH);
        }));
        fs::create_dir(TEST_DB_PATH)?;
        let mut database = Database::new(PathBuf::from(TEST_DB_PATH), 10000)?;
        let mut files = build_files(1000)?;
        database.set(1, files.clone())?;
        compare_database(&database, &files);
        let mut set = modify_files(&mut files, 100);
        //delete operation
        set[0].value = Vec::new();
        let idx = files.iter().position(|f|f.key == set[0].key).unwrap();
        files.remove(idx);
        database.set(2, set)?;
        compare_database(&database, &files);
        let database2 = Database::new(PathBuf::from(TEST_DB_PATH), 10000)?;
        compare_database(&database2, &files);
        fs::remove_dir_all(TEST_DB_PATH)
    }

    fn modify_files(files: &mut Vec<KeyValue>, mut count: usize) -> Vec<KeyValue> {
        let mut result = Vec::new();
        let mut key_set: HashSet<usize> = HashSet::new();
        while count > 0 {
            let mut idx = (OsRng.try_next_u32().unwrap() as usize) % files.len();
            while key_set.contains(&idx) {
                idx = (OsRng.try_next_u32().unwrap() as usize) % files.len();
            }
            key_set.insert(idx);
            let new_value = KeyValue{key: files[idx].key, version: 2, value: build_random_data()};
            files[idx] = new_value.clone();
            result.push(new_value);
            count -= 1;
        }
        result
    }

    fn compare_database(database: &Database, files: &Vec<KeyValue>) {
        let files_map: HashMap<usize, KeyValue> = files.iter().map(|f|(f.key, f.clone())).collect();
        let data = database.get(0, usize::MAX);
        assert_eq!(data.len(), files.len());
        for item in data {
            let file_option = files_map.get(&item.key);
            assert!(file_option.is_some());
            let file = file_option.unwrap();
            assert_eq!(file.version, item.version);
            assert_eq!(file.value.len(), item.value.len(), "file {} length mismatch", item.key);
            for i in 0..file.value.len() {
                assert_eq!(file.value[i], item.value[i], "array elements are not equal at index {}", i);
            }
        }
    }

    fn build_random_data() -> Vec<u8> {
        let size = OsRng.try_next_u32().unwrap() as usize % 1000 + 100;
        let mut data = vec![0u8; size];
        OsRng.try_fill_bytes(&mut data).unwrap();
        data
    }
    
    fn build_files(mut number_of_files: usize) -> Result<Vec<KeyValue>, Error> {
        let mut result = Vec::new();
        let mut key_set: HashSet<usize> = HashSet::new();
        while number_of_files > 0 {
            let mut key = OsRng.try_next_u32().unwrap() as usize;
            while key_set.contains(&key) {
                key = OsRng.try_next_u32().unwrap() as usize;
            }
            key_set.insert(key);
            let data= build_random_data();
            result.push(KeyValue{key, version: 1, value: data});
            number_of_files -= 1;
        }
        Ok(result)
    }
}