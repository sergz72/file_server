use std::io::{Error, ErrorKind, Read};
use std::sync::RwLock;
use bzip2::read::BzDecoder;
use smart_home_common::user_message_processor::CommandProcessor;
use crate::database::KeyValue;
use crate::databases::Databases;

pub struct UserCommandProcessor {
    data: RwLock<Databases>
}

impl CommandProcessor for UserCommandProcessor {
    fn check_message_length(&self, length: usize) -> bool {
        length > 10
    }

    fn execute(&self, command: Vec<u8>) -> Result<Vec<u8>, Error> {
        match command[0] {
            0 => self.run_get_command(&command[1..]),
            1 => self.run_set_command(&command[1..]),
            _ => Err(Error::new(ErrorKind::InvalidInput, "Invalid command"))
        }
    }
}

impl UserCommandProcessor {
    pub fn new(base_folder: String, hash_divider: usize) -> Result<Box<UserCommandProcessor>, Error> {
        let data = RwLock::new(Databases::new(base_folder, hash_divider)?);
        Ok(Box::new(UserCommandProcessor{ data }))
    }

    fn run_get_command(&self, command: &[u8]) -> Result<Vec<u8>, Error> {
        let (database, idx) = get_database_name(command)?;
        if idx + 8 < command.len() {
            return Err(Error::new(ErrorKind::InvalidInput, "Invalid get command length"));
        }
        let mut buffer32 = [0u8; 4];
        buffer32.clone_from_slice(&command[idx..idx+4]);
        let from = u32::from_le_bytes(buffer32);
        buffer32.clone_from_slice(&command[idx+4..idx+8]);
        let to = u32::from_le_bytes(buffer32);
        
        let lock = self.data.read().unwrap();
        let result = lock.get(database, from as usize, to as usize);
        
        let mut data = Vec::new();
        data.push(0); // no error
        data.extend_from_slice(&(result.len() as u32).to_le_bytes());
        for kv in result {
            data.extend_from_slice(&kv.to_binary());
        }
        Ok(data)
    }

    fn run_set_command(&self, command: &[u8]) -> Result<Vec<u8>, Error> {
        let (database, idx) = get_database_name(command)?;
        let decompressed = decompress(&command[idx..])?;
        let data = KeyValue::from(decompressed)?;
        let mut lock = self.data.write().unwrap();
        lock.set(database, data)?;
        Ok(vec![0]) // no error
    }
}

fn decompress(data: &[u8]) -> Result<Vec<u8>, Error> {
    let mut decompressor = BzDecoder::new(data);
    let mut result = Vec::new();
    decompressor.read_to_end(&mut result)?;
    Ok(result)
}

fn get_database_name(command: &[u8]) -> Result<(String, usize), Error> {
    let length = command[0] as usize;
    let name = String::from_utf8(command[1..length+1].to_vec())
        .map_err(|e| Error::new(ErrorKind::InvalidData, e))?;
    Ok((name, length + 1))
}
