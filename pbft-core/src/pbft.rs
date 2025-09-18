use std::sync::{Arc};
use crypto::{PublicKey, Signature};

use crate::{
    api::ProposeBlockMsgBroadcast, broadcast::Broadcaster, config::NodeId, pbft_executor::{quorum_size, PbftExecutor}, replica_client::ReplicaClient, ProtocolMessage
};

pub struct Pbft {
    nodes_config: crate::config::PbftNodeConfig,
    pbft_executor: PbftExecutor,
}

impl Pbft {
    pub fn new(
        config: crate::Config,
    ) -> Result<Self, crate::error::Error> {
        let keypair = Arc::new(config.node_config.get_keypair());

        let broadcaster = Arc::new(Broadcaster::new(
            config.node_config.self_id,
            config.node_config.nodes.clone(),
            keypair.clone(),
        ));
        let replica_client = Arc::new(ReplicaClient::new(config.node_config.replica_address.clone()));
        let pbft_executor =
            PbftExecutor::new(config.clone(), keypair, broadcaster.clone(), replica_client);

        Ok(Self {
            pbft_executor,
            nodes_config: config.node_config,
        })
    }

    pub async fn start(
        &self,
        executor_rx_cancel: tokio::sync::broadcast::Receiver<()>,
    ) {
        self.pbft_executor.run(executor_rx_cancel).await;
    }

    pub fn quorum_size(&self) -> usize {
        quorum_size(self.nodes_config.nodes.len())
    }

    pub fn handle_propose_block_broadcast(
        &self,
        sender_id: u64,
        message: ProposeBlockMsgBroadcast,
    ) -> Result<(), crate::error::Error> {
        self.pbft_executor
            .queue_request_broadcast(sender_id, message);
        Ok(())
    }

    pub fn handle_consensus_message(
        &self,
        sender_id: u64,
        message: ProtocolMessage,
    ) -> Result<(), crate::error::Error> {
        self.pbft_executor
            .queue_protocol_message(sender_id, message);
        Ok(())
    }

    pub fn verify_request_signature(
        &self,
        replica_id: u64,
        signature: &str,
        msg: &[u8],
    ) -> Result<(), crate::error::Error> {
        if replica_id > self.nodes_config.nodes.len() as u64 {
            return Err(crate::error::Error::InvalidReplicaID {
                replica_id: NodeId(replica_id),
            });
        }
        let peer = &self.nodes_config.nodes[replica_id as usize];

        let public_key = PublicKey::decode_base64(&peer.public_key).map_err(
            crate::error::Error::base64_error("failed to parse public key from bytes"),
        )?;

        let signature = Signature::decode_base64(&signature).map_err(
            crate::error::Error::base64_error("failed to parse signature from bytes"),
        )?;

        let is_ok = public_key.verify_signature(msg, &signature);
        if !is_ok {
            return Err(crate::error::Error::InvalidSignature);
        }
        Ok(())
    }

    pub fn get_state(&self) -> crate::api::PbftNodeState {
        self.pbft_executor.get_state()
    }
}
