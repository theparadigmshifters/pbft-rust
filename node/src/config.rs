use std::{
    path::PathBuf,
};
use serde::{Deserialize};

#[derive(Deserialize, Debug, Clone)]
pub struct AppConfig {
    pub pbft_config: pbft_core::Config,
}

impl AppConfig {
    pub fn new(config_file: Option<PathBuf>) -> std::result::Result<Self, config::ConfigError> {
        let mut config_builder = config::Config::builder();

        if let Some(config_file) = config_file {
            config_builder = config_builder.add_source(config::File::from(config_file));
        }

        let config = config_builder
            .add_source(
                config::Environment::with_prefix("KV_STORE")
                    .separator(".")
                    .prefix_separator("_"),
            )
            .build()?;

        config.try_deserialize()
    }
}
