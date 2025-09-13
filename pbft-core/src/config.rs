use std::{
    collections::HashMap, fs, time::Duration
};
use thiserror::Error;
use crypto::generate_production_keypair;
use crypto::{PublicKey, SecretKey};
use serde::{Deserialize, Serialize};
use serde::de::DeserializeOwned;
use std::fs::{OpenOptions};
use std::io::BufWriter;
use std::io::Write as _;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Config {
    pub node_config: PbftNodeConfig,
    pub checkpoint_frequency: u64,
    #[serde(default = "default_view_change_timeout")]
    pub view_change_timeout: Duration,
    pub response_urls: Vec<String>,
    pub executor_config: ExecutorConfig,
}

fn default_view_change_timeout() -> Duration {
    Duration::from_secs(10)
}

#[derive(Debug, Clone, Serialize, Deserialize, Copy, PartialEq, Eq, Hash)]
pub struct NodeId(pub u64);

impl std::fmt::Display for NodeId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PbftNodeConfig {
    pub self_id: NodeId,
    pub private_key_path: String,
    pub nodes: Vec<NodeConfig>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodeConfig {
    pub id: NodeId,
    pub addr: String,
    pub public_key: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExecutorConfig {
    #[serde(default = "GenericDefault::<5>::value")]
    pub max_requeue_attempts_on_failure: u32,
}

impl PbftNodeConfig {
    pub fn get_keypair(&self) -> Secret {
        Secret::read(&self.private_key_path).unwrap()
    }

    pub fn trusted_pub_keys(&self) -> HashMap<&str, NodeId> {
        self.nodes
            .iter()
            .map(|node| (node.public_key.as_str(), node.id))
            .collect()
    }
}

struct GenericDefault<const U: u32>;

impl<const U: u32> GenericDefault<U> {
    fn value() -> u32 {
        U
    }
}

#[derive(Error, Debug)]
pub enum ConfigError {
    #[error("Failed to read config file '{file}': {message}")]
    ReadError { file: String, message: String },

    #[error("Failed to write config file '{file}': {message}")]
    WriteError { file: String, message: String },
}

pub trait Export: Serialize + DeserializeOwned {
    fn read(path: &str) -> Result<Self, ConfigError> {
        let reader = || -> Result<Self, std::io::Error> {
            let data = fs::read(path)?;
            Ok(serde_json::from_slice(data.as_slice())?)
        };
        reader().map_err(|e| ConfigError::ReadError {
            file: path.to_string(),
            message: e.to_string(),
        })
    }

    fn write(&self, path: &str) -> Result<(), ConfigError> {
        let writer = || -> Result<(), std::io::Error> {
            let file = OpenOptions::new().create(true).write(true).open(path)?;
            let mut writer = BufWriter::new(file);
            let data = serde_json::to_string_pretty(self).unwrap();
            writer.write_all(data.as_ref())?;
            writer.write_all(b"\n")?;
            Ok(())
        };
        writer().map_err(|e| ConfigError::WriteError {
            file: path.to_string(),
            message: e.to_string(),
        })
    }
}

#[derive(Serialize, Deserialize)]
pub struct Secret {
    pub name: PublicKey,
    pub secret: SecretKey,
}

impl Secret {
    pub fn new() -> Self {
        let (name, secret) = generate_production_keypair();
        Self { name, secret }
    }
}

impl Export for Secret {}

impl Default for Secret {
    fn default() -> Self {
        let (name, secret) = generate_production_keypair();
        Self { name, secret }
    }
}
