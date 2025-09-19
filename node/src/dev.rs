use pbft_core::config::NodeId;
use crate::config::AppConfig;

pub fn dev_config(replica_self_id: u64, start_port: u16) -> AppConfig {
    let pbft_config = pbft_core::dev::dev_config(NodeId(replica_self_id), start_port);

    AppConfig {
        pbft_config,
    }
}
