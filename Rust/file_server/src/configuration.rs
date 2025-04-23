use std::collections::{HashMap, HashSet};
use std::fs::File;
use std::io::{BufReader, Error, ErrorKind};
use serde::Deserialize;

#[derive(Debug, Deserialize, Clone)]
#[allow(dead_code)]
pub struct User {
    #[serde(rename = "Id")]
    pub id: u32,
    #[serde(rename = "Name")]
    pub name: String,
    #[serde(rename = "KeyFileName")]
    pub key_file_name: String,
    #[serde(rename = "Databases")]
    pub databases: HashMap<String, String>,
}

#[derive(Debug, Deserialize)]
#[allow(dead_code)]
pub struct Configuration {
    #[serde(rename = "PortNumber")]
    pub port_number: u16,
    #[serde(rename = "BaseFolder")]
    pub base_folder: String,
    #[serde(rename = "HashDivider")]
    pub hash_divider: usize,
    #[serde(rename = "Users")]
    pub users: Vec<User>
}

pub fn load_configuration(ini_file_name: &String) -> Result<Configuration, Error> {
    let file = File::open(ini_file_name)?;
    let reader = BufReader::new(file);
    let config: Configuration = serde_json::from_reader(reader)?;

    if config.users.len() == 0 || config.port_number == 0 ||
        config.base_folder.is_empty()
    {
        return Err(Error::new(
            ErrorKind::InvalidData,
            "incorrect configuration parameters",
        ));
    }
    let mut user_set = HashSet::new();
    for user in &config.users {
        if user.key_file_name.is_empty() || user.name.is_empty() {
            return Err(Error::new(
                ErrorKind::InvalidData,
                "incorrect users configuration section",
            ));
        }
        if !user_set.insert(user.id) {
            return Err(Error::new(
                ErrorKind::InvalidData,
                format!("duplicate user with id {}", user.id)
            ));
        }
    }

    Ok(config)
}

#[cfg(test)]
mod tests {
    use crate::configuration::load_configuration;

    #[test]
    fn test_load_configuration() {
        let result = load_configuration(&"test_resources/testConfiguration.json".to_string());
        assert!(!result.is_err(), "Configuration load error {}", result.unwrap_err());
        let config = result.unwrap();
        assert_eq!(config.users.len(), 1);
        assert_eq!(config.port_number, 59999, "incorrect PortNumber value");
        assert_eq!(config.base_folder, "/tmp", "incorrect base folder");
        assert_eq!(config.hash_divider, 10000, "incorrect hash divider");
        assert_eq!(config.users.len(), 1, "incorrect number of users");
        let user = &config.users[0];
        assert_eq!(user.id, 11223344);
        assert_eq!(user.key_file_name, "key.dat", "incorrect key file name");
        assert_eq!(user.name, "User1", "incorrect user name");
        assert_eq!(user.databases.len(), 2, "incorrect databases rw");
        assert!(user.databases.contains_key("db1"), "incorrect databases rw(db1)");
        assert!(user.databases.contains_key("db2"), "incorrect databases rw(db1)");
        assert_eq!(user.databases.get("db1").unwrap(), "rw", "incorrect db1 value");
        assert_eq!(user.databases.get("db2").unwrap(), "r", "incorrect db2 value");
    }
}