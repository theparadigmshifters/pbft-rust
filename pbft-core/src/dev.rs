use std::{collections::HashMap, time::Duration};
use std::str::FromStr;
use libp2p::PeerId;

use crate::{
    config::{CommitteeConfig, ExecutorConfig, NodeConfig, NodeId, PbftNodeConfig},
    Config,
};

pub const DEV_VIEW_CHANGE_TIMEOUT: Duration = Duration::from_secs(30);

pub static DEV_PUBLIC_KEYS: [&str; 4] = [
    "tZt4D/AgndQedbcVa+3pC6oLf07h58fH/TrEhdTYEevqP8UbRq3+jGxho2iDIe6B",
    "uTQdAPWpTn4EjRBRRR/e0d3I3VSMfnRCBEK6d30TjEARWp6W3VuzjFm78AKOV75s",
    "ouNPzVAZcsHDDilWtaTYDni2NcURa1g6xKzxsmFXW7AHhHhOxBmMsm9SZQgC7w+D",
    "pp1qZVSldRX2U+GAuIqaamiWYsC5BTCVwChNxlI2m+Avsy8I9R2xzZy8IX3wXJUV",
];

pub fn dev_config(self_id: NodeId, start_port: u16) -> Config {
    // Create temp file with private key
    let private_key_path = format!("../.node-{}.json", self_id.0);

    let cfg = Config {
        view_change_timeout: DEV_VIEW_CHANGE_TIMEOUT,
        node_config: PbftNodeConfig {
            self_id,
            private_key_path: private_key_path.into(),
            replica_address: format!("http://localhost:{}", start_port + self_id.0 as u16),
            nodes: vec![
                NodeConfig {
                    id: NodeId(0),
                    public_key: DEV_PUBLIC_KEYS[0].to_string(),
                },
                NodeConfig {
                    id: NodeId(1),
                    public_key: DEV_PUBLIC_KEYS[1].to_string(),
                },
                NodeConfig {
                    id: NodeId(2),
                    public_key: DEV_PUBLIC_KEYS[2].to_string(),
                },
                NodeConfig {
                    id: NodeId(3),
                    public_key: DEV_PUBLIC_KEYS[3].to_string(),
                },
            ],
        },
        committee_config: CommitteeConfig {
            committee: HashMap::from([
                (PeerId::from_str("12D3KooWCzLEA2ttgMryB4ZfaiuY5es5kqVMFfS94dCW2jZe8Tqh").unwrap(), NodeId(0)),
                (PeerId::from_str("12D3KooW9zmCkaoQ4i5VQ87iZF2azP2hvYTKk2oD1eArP5au1y96").unwrap(), NodeId(1)),
                (PeerId::from_str("12D3KooWM2T9d3gDtf9FAEoxpGmg6KKzDXdqnhHjtMjBpUbtkABb").unwrap(), NodeId(2)),
                (PeerId::from_str("12D3KooWQ8gqtWAwieojh1vkVMychjUe2uzqk4QJwrbxewkKjE5o").unwrap(), NodeId(3)),
            ]),
        },
        checkpoint_frequency: 10,
        executor_config: ExecutorConfig {
            max_requeue_attempts_on_failure: 5,
        },
    };

    cfg
}
