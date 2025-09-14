use std::sync::{Arc};
use crypto::{PublicKey, Signature};
use tracing::{debug, info};

use crate::{
    api::ClientRequestBroadcast,
    broadcast::Broadcaster,
    config::NodeId,
    pbft_executor::quorum_size,
    pbft_executor::{self, PbftExecutor},
    ClientRequest, OperationAck, OperationStatus, ProtocolMessage,
};

pub struct Pbft {
    nodes_config: crate::config::PbftNodeConfig,
    pbft_executor: PbftExecutor,
    broadcaster: Arc<Broadcaster>,
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
        let pbft_executor =
            PbftExecutor::new(config.clone(), keypair, broadcaster.clone());

        Ok(Self {
            pbft_executor,
            broadcaster,
            nodes_config: config.node_config,
        })
    }

    pub async fn start(
        &self,
        executor_rx_cancel: tokio::sync::broadcast::Receiver<()>,
        backup_rx_cancel: tokio::sync::broadcast::Receiver<()>,
    ) {
        tokio::select! {
            _ = self.pbft_executor.run(executor_rx_cancel) => {
                info!("pbft executor loop exited");
            }
            _ = self.pbft_executor.run_backup_queue_watcher(backup_rx_cancel) => {
                info!("pbft backup queue watcher exited");
            }
        }
    }

    pub fn quorum_size(&self) -> usize {
        quorum_size(self.nodes_config.nodes.len())
    }

    pub async fn handle_client_request(
        &self,
        request: ClientRequest,
    ) -> Result<OperationAck, crate::error::Error> {
        match self.pbft_executor.handle_client_request(request.clone())? {
            crate::pbft_executor::ClientRequestResult::NotLeader(pbft_executor::NotLeader {
                leader_id,
            }) => {
                debug!(
                    leader_id = leader_id.0,
                    "forwarding client request to leader"
                );
                // We forward the request here instead of an executor since we
                // are awaiting the async operation.
                let ack = self
                    .broadcaster
                    .forward_to_node(request, leader_id)
                    .await
                    .map_err(crate::error::Error::broadcast_error(
                        "failed to forward client request to leader",
                    ))?;
                Ok(ack)
            }
            crate::pbft_executor::ClientRequestResult::AlreadyAccepted(handled_req) => {
                let ack = OperationAck {
                    client_request: handled_req.request.request,
                    leader_id: handled_req.leader_id,
                    sequence_number: handled_req.request.sequence,
                    status: OperationStatus::AlreadyHandled(handled_req.request.result),
                };
                Ok(ack)
            }
            crate::pbft_executor::ClientRequestResult::Accepted(sequence) => {
                let op_ack = OperationAck {
                    client_request: request.clone(),
                    leader_id: self.nodes_config.self_id.0,
                    sequence_number: sequence,
                    status: OperationStatus::Accepted,
                };
                Ok(op_ack)
            }
        }
    }

    pub fn handle_client_request_broadcast(
        &self,
        sender_id: u64,
        message: ClientRequestBroadcast,
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
