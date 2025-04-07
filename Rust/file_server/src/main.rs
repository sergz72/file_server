mod configuration;
mod command_processor;
mod databases;
mod database;

use std::io::Error;
use smart_home_common::base_server::BaseServer;
use smart_home_common::keys::read_key_file32;
use smart_home_common::user_message_processor::build_message_processor;
use crate::command_processor::UserCommandProcessor;
use crate::configuration::load_configuration;

fn main() -> Result<(), Error> {
    let ini_file_name = &std::env::args().nth(1).expect("no file name given");
    let config = load_configuration(ini_file_name)?;
    let key = read_key_file32(&config.key_file_name)?;
    let message_processor =
        build_message_processor(key,
                                UserCommandProcessor::new(config.base_folder.clone(), config.hash_divider)?)?;
    let udp_server =
        Box::leak(Box::new(BaseServer::new(true, config.port_number,
                                           message_processor.clone(), 0,
                                           "udp_server".to_string())?));
    udp_server.start();
    Ok(())
}
