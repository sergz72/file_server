use std::io::{Error, ErrorKind};
use std::sync::RwLock;
use smart_home_common::user_message_processor::CommandProcessor;
use crate::database::KeyValue;
use crate::databases::Databases;

pub struct UserCommandProcessor {
    data: RwLock<Databases>
}

impl CommandProcessor for UserCommandProcessor {
    fn check_message_length(&self, length: usize) -> bool {
        length > 6
    }

    fn execute(&self, command: Vec<u8>) -> Result<Vec<u8>, Error> {
        match command[0] {
            0 => self.run_get_command(&command[1..]),
            1 => self.run_set_command(&command[1..]),
            2 => self.run_get_last_command(&command[1..]),
            3 => self.run_get_file_version_command(&command[1..]),
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
        let (database, from, to) = parse_get_command_parameters(command)?;
        
        let lock = self.data.read().unwrap();
        let (version, result) = lock.get(database, from, to);
        
        let mut data = Vec::new();
        data.push(0); // no error
        data.extend_from_slice(&version.to_le_bytes());
        data.extend_from_slice(&(result.len() as u32).to_le_bytes());
        for kv in result {
            data.extend_from_slice(&kv.to_binary());
        }
        Ok(data)
    }

    fn run_get_last_command(&self, command: &[u8]) -> Result<Vec<u8>, Error> {
        let (database, from, to) = parse_get_command_parameters(command)?;

        let lock = self.data.read().unwrap();
        let (version, result) = lock.get_last(database, from, to);

        let mut data = Vec::new();
        data.push(0); // no error
        data.extend_from_slice(&version.to_le_bytes());
        if let Some(kv) = result {
            data.push(1);
            data.extend_from_slice(&kv.to_binary());
        } else {
            data.push(0);
        }
        Ok(data)
    }

    fn run_get_file_version_command(&self, command: &[u8]) -> Result<Vec<u8>, Error> {
        let (database, key) = parse_get_file_version_command_parameters(command)?;

        let lock = self.data.read().unwrap();
        let (db_version, file_version) = lock.get_file_version(database, key);

        let mut data = Vec::new();
        data.push(0); // no error
        data.extend_from_slice(&db_version.to_le_bytes());
        data.extend_from_slice(&file_version.unwrap_or(0).to_le_bytes());
        Ok(data)
    }
    
    fn run_set_command(&self, command: &[u8]) -> Result<Vec<u8>, Error> {
        let (database, mut idx) = get_database_name(command)?;
        let expected_version = u32::from_le_bytes(command[idx..idx+4].try_into().unwrap());
        idx += 4;
        let data = KeyValue::from(&command[idx..])?;
        let mut lock = self.data.write().unwrap();
        lock.set(database, expected_version, data)?;
        Ok(vec![0]) // no error
    }
}

fn parse_get_command_parameters(command: &[u8]) -> Result<(String, usize, usize), Error> {
    let (database, idx) = get_database_name(command)?;
    if idx + 8 != command.len() {
        return Err(Error::new(ErrorKind::InvalidInput, "Invalid get command length"));
    }
    let mut buffer32 = [0u8; 4];
    buffer32.clone_from_slice(&command[idx..idx+4]);
    let from = u32::from_le_bytes(buffer32) as usize;
    buffer32.clone_from_slice(&command[idx+4..idx+8]);
    let to = u32::from_le_bytes(buffer32) as usize;
    Ok((database, from, to))
}

fn parse_get_file_version_command_parameters(command: &[u8]) -> Result<(String, usize), Error> {
    let (database, idx) = get_database_name(command)?;
    if idx + 4 != command.len() {
        return Err(Error::new(ErrorKind::InvalidInput, "Invalid get command length"));
    }
    let mut buffer32 = [0u8; 4];
    buffer32.clone_from_slice(&command[idx..idx+4]);
    let key = u32::from_le_bytes(buffer32) as usize;
    Ok((database, key))
}

fn get_database_name(command: &[u8]) -> Result<(String, usize), Error> {
    let length = command[0] as usize;
    let name = String::from_utf8(command[1..length+1].to_vec())
        .map_err(|e| Error::new(ErrorKind::InvalidData, e))?;
    Ok((name, length + 1))
}
