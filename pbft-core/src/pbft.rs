use std::sync::{Arc};
use tokio::sync::mpsc;
use tracing::{error};
use crate::{
    api::{P2pMessage}, config::NodeId, p2p_node::P2pLibp2p, pbft_executor::{quorum_size, Event, PbftExecutor}, replica_client::ReplicaClient
};

pub struct Pbft {
    nodes_config: crate::config::PbftNodeConfig,
    pbft_executor: PbftExecutor,
}

impl Pbft {
    pub fn new(
        config: crate::Config,
        p2p: P2pLibp2p,
        mut msg_rx: mpsc::UnboundedReceiver<(NodeId, Vec<u8>)>,
    ) -> Result<Self, crate::error::Error> {
        let keypair = Arc::new(config.node_config.get_keypair());

        let replica_client = ReplicaClient::new(config.node_config.replica_address.clone());
        let pbft_executor =
            PbftExecutor::new(config.clone(), keypair, p2p, Arc::new(replica_client));
        let event_tx = pbft_executor.event_tx.clone();
        tokio::spawn(async move {
            while let Some((sender_id, raw_msg)) = msg_rx.recv().await {
                match serde_json::from_slice::<P2pMessage>(&raw_msg) {
                    Ok(P2pMessage::Proposal(proposal_msg)) => {
                        if event_tx.send(Event::ProposeBlockBroadcast(sender_id, proposal_msg).into()).await.is_err() {
                            error!("Failed to send ProposeBlockBroadcast event");
                        }
                    }
                    Ok(P2pMessage::Consensus(consensus_msg)) => {
                        if event_tx.send(Event::ProtocolMessage(sender_id, consensus_msg.message).into()).await.is_err() {
                            error!("Failed to send ProtocolMessage event");
                        }
                    }
                    Err(e) => {
                        error!("Failed to deserialize P2P message: {}", e);
                    }
                }
            }
        });

        Ok(Self {
            pbft_executor,
            nodes_config: config.node_config,
        })
    }

    pub async fn start(
        &mut self,
        executor_rx_cancel: tokio::sync::broadcast::Receiver<()>,
    ) {
        self.pbft_executor.run(executor_rx_cancel).await;
    }

    pub fn quorum_size(&self) -> usize {
        quorum_size(self.nodes_config.nodes.len())
    }
}
