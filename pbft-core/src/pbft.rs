use std::sync::{Arc};
use tokio::sync::mpsc;
use tracing::{error, info};
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
        mut rx_cancel: tokio::sync::broadcast::Receiver<()>,
    ) -> Result<Self, crate::error::Error> {
        let keypair = Arc::new(config.node_config.get_keypair());

        let replica_client = ReplicaClient::new(config.node_config.replica_address.clone());
        let pbft_executor =
            PbftExecutor::new(config.clone(), keypair, p2p, Arc::new(replica_client));
        let event_tx = pbft_executor.event_tx.clone();
        
        tokio::spawn(async move {
            loop {
                tokio::select! {
                    msg_result = msg_rx.recv() => {
                        match msg_result {
                            Some((sender_id, raw_msg)) => {
                                match serde_json::from_slice::<P2pMessage>(&raw_msg) {
                                    Ok(P2pMessage::Proposal(proposal_msg)) => {
                                        if event_tx.send(Event::ProposeBlockBroadcast(sender_id, proposal_msg).into()).await.is_err() {
                                            error!("Failed to send ProposeBlockBroadcast event");
                                            break;
                                        }
                                    }
                                    Ok(P2pMessage::Consensus(consensus_msg)) => {
                                        if event_tx.send(Event::ProtocolMessage(sender_id, consensus_msg.message).into()).await.is_err() {
                                            error!("Failed to send ProtocolMessage event");
                                            break;
                                        }
                                    }
                                    Err(e) => {
                                        error!("Failed to deserialize P2P message: {}", e);
                                    }
                                }
                            }
                            None => {
                                error!("Message channel closed, stopping message processor");
                                break;
                            }
                        }
                    }
                    _ = rx_cancel.recv() => {
                        info!("Received cancel signal, stopping message processor");
                        break;
                    }
                }
            }
            info!("Message processor stopped");
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
