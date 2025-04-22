extern crate core;

mod configuration;
mod command_processor;
mod databases;
mod database;

use std::collections::{HashMap, HashSet};
use std::io::{Error, ErrorKind};
use smart_home_common::base_server::BaseServer;
use smart_home_common::keys::read_key_file32;
use smart_home_common::user_message_processor::build_message_processor;
use crate::command_processor::UserCommandProcessor;
use crate::configuration::{load_configuration, User};

pub struct UserWithKey {
    pub id: u32,
    pub name: String,
    pub key: [u8; 32],
    pub databases_rw: HashSet<String>,
    pub databases_r: HashSet<String>
}

impl UserWithKey {
    pub(crate) fn validate_access(&self, db_name: &String, get_request: bool) -> Result<(), Error> {
        let mut ok = self.databases_rw.contains(db_name);
        if get_request {
            ok |= self.databases_r.contains(db_name);
        }
        if ok {
            Ok(())
        } else {
            Err(Error::new(ErrorKind::InvalidInput, 
                           format!("Database access error. User {} Database name {}", self.name, db_name)))
        }
    }
}

impl UserWithKey {
    fn from(user: &User, key: [u8; 32]) -> UserWithKey {
        UserWithKey{id: user.id, name: user.name.clone(), key, databases_rw: user.databases_rw.clone(),
                    databases_r: user.databases_r.clone()}
    }
}

fn main() -> Result<(), Error> {
    let ini_file_name = &std::env::args().nth(1).expect("no file name given");
    let config = load_configuration(ini_file_name)?;
    let mut user_map = HashMap::new();
    for user in &config.users {
        let key = read_key_file32(&user.key_file_name)?;
        user_map.insert(user.id, UserWithKey::from(user, key));
    }
    let message_processor =
        build_message_processor(UserCommandProcessor::new(config.base_folder.clone(),
                                                          config.hash_divider, user_map)?, false)?;
    let udp_server =
        Box::leak(Box::new(BaseServer::new(true, config.port_number,
                                           message_processor.clone(), 0,
                                           "udp_server".to_string())?));
    udp_server.start();
    Ok(())
}
