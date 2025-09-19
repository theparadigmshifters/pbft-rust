use anyhow::Result;
use libp2p::request_response::{Config, Event as RequestResponseEvent, Message as RequestResponseMessage, ProtocolSupport};
use libp2p::swarm::NetworkBehaviour;
use libp2p::{PeerId, StreamProtocol};
use serde::{Deserialize, Serialize};
use tracing::warn;
use std::collections::HashMap;
use std::sync::Arc;

use crate::config::{CommitteeConfig, NodeId};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MsgRequest {
    pub payload: Vec<u8>,
}

#[derive(Debug)]
pub enum MsgEvent {
    MessageReceived { payload: Vec<u8> },
    MessageSent { success: bool },
}

type MsgCodec = libp2p::request_response::cbor::Behaviour<MsgRequest, ()>;
type OnMsgCallback = Arc<dyn Fn(NodeId, Vec<u8>) -> Result<()> + Sync + Send>;

pub struct MsgProtocol {
    committee_config: CommitteeConfig,
    request_response: MsgCodec,
    on_msg_callback: Option<OnMsgCallback>,
    id_to_peer: HashMap<NodeId, PeerId>,
    disconnect_on_error: bool,
    peers_to_disconnect: Vec<PeerId>,
}

impl MsgProtocol {
    pub fn new_with_callback(committee_config: CommitteeConfig, on_msg: impl Fn(NodeId, Vec<u8>) -> Result<()> + Send + Sync + 'static, disconnect_on_error: bool) -> Self {
        let protocol = StreamProtocol::new("/msg/1.0.0");
        let config = Config::default();

        Self {
            committee_config,
            request_response: libp2p::request_response::cbor::Behaviour::new([(protocol, ProtocolSupport::Full)], config),
            on_msg_callback: Some(Arc::new(on_msg)),
            id_to_peer: HashMap::new(),
            disconnect_on_error,
            peers_to_disconnect: Vec::new(),
        }
    }

    pub fn send_message(&mut self, to_id: Option<NodeId>, payload: Vec<u8>) -> Result<()> {
        let msg = MsgRequest { payload };

        if let Some(to) = to_id {
            if let Some(peer) = self.id_to_peer.get(&to) {
                self.request_response.send_request(peer, msg);
                Ok(())
            } else {
                Err(anyhow::anyhow!("Peer not found for: {:?}", to))
            }
        } else {
            let peers: Vec<PeerId> = self.id_to_peer.values().cloned().collect();
            if peers.is_empty() {
                return Err(anyhow::anyhow!("No peers to broadcast to"));
            }

            for peer in peers {
                self.request_response.send_request(&peer, msg.clone());
            }
            Ok(())
        }
    }

    pub fn register_peer(&mut self, peer_id: PeerId) {
        let id = self.committee_config.committee.get(&peer_id);
        match id {
            Some(id) => {
                       self.id_to_peer.insert(id.clone(), peer_id);
            },
            None => {
                warn!("Unknown peer connected: {}", peer_id);
                return;
            }
        }
    }

    pub fn unregister_peer(&mut self, peer_id: &PeerId) {
        let id = self.committee_config.committee.get(&peer_id).expect("PeerId should exist in p2p peers");
        self.id_to_peer.remove(&id);
    }

    pub fn get_peers_to_disconnect(&mut self) -> Vec<PeerId> {
        std::mem::take(&mut self.peers_to_disconnect)
    }
}

impl NetworkBehaviour for MsgProtocol {
    type ConnectionHandler = <MsgCodec as NetworkBehaviour>::ConnectionHandler;
    type ToSwarm = MsgEvent;

    fn handle_established_inbound_connection(
        &mut self, connection_id: libp2p::swarm::ConnectionId, peer: PeerId, local_addr: &libp2p::Multiaddr, remote_addr: &libp2p::Multiaddr,
    ) -> Result<libp2p::swarm::THandler<Self>, libp2p::swarm::ConnectionDenied> {
        self.register_peer(peer);
        self.request_response.handle_established_inbound_connection(connection_id, peer, local_addr, remote_addr)
    }

    fn handle_established_outbound_connection(
        &mut self, connection_id: libp2p::swarm::ConnectionId, peer: PeerId, addr: &libp2p::Multiaddr, role_override: libp2p::core::Endpoint, port_use: libp2p::core::transport::PortUse,
    ) -> Result<libp2p::swarm::THandler<Self>, libp2p::swarm::ConnectionDenied> {
        self.register_peer(peer);
        self.request_response.handle_established_outbound_connection(connection_id, peer, addr, role_override, port_use)
    }

    fn on_swarm_event(&mut self, event: libp2p::swarm::FromSwarm) {
        match &event {
            libp2p::swarm::FromSwarm::ConnectionClosed(info) => {
                self.unregister_peer(&info.peer_id);
            }
            _ => {}
        }
        self.request_response.on_swarm_event(event);
    }

    fn on_connection_handler_event(&mut self, peer_id: PeerId, connection_id: libp2p::swarm::ConnectionId, event: libp2p::swarm::THandlerOutEvent<Self>) {
        self.request_response.on_connection_handler_event(peer_id, connection_id, event);
    }

    fn poll(&mut self, cx: &mut std::task::Context<'_>) -> std::task::Poll<libp2p::swarm::ToSwarm<Self::ToSwarm, libp2p::swarm::THandlerInEvent<Self>>> {
        loop {
            match self.request_response.poll(cx) {
                std::task::Poll::Ready(libp2p::swarm::ToSwarm::GenerateEvent(event)) => match event {
                    RequestResponseEvent::Message { peer, message, .. } => match message {
                        RequestResponseMessage::Request { request, channel, .. } => {
                            let _ = self.request_response.send_response(channel, ());
                            
                            let id = self.committee_config.committee.get(&peer).expect("PeerId should exist in p2p peers");

                            if let Some(callback) = &self.on_msg_callback {
                                if let Err(e) = callback(id.clone(), request.payload.clone()) {
                                    warn!("Message processing failed from peer {}: {}", peer, e);
                                    if self.disconnect_on_error {
                                        self.peers_to_disconnect.push(peer);
                                    }
                                }
                            }

                            return std::task::Poll::Ready(libp2p::swarm::ToSwarm::GenerateEvent(MsgEvent::MessageReceived { payload: request.payload }));
                        }
                        RequestResponseMessage::Response { .. } => {
                            return std::task::Poll::Ready(libp2p::swarm::ToSwarm::GenerateEvent(MsgEvent::MessageSent { success: true }));
                        }
                    },
                    RequestResponseEvent::OutboundFailure { .. } => {
                        return std::task::Poll::Ready(libp2p::swarm::ToSwarm::GenerateEvent(MsgEvent::MessageSent { success: false }));
                    }
                    _ => {}
                },
                std::task::Poll::Ready(other) => return std::task::Poll::Ready(other.map_out(|_| unreachable!())),
                std::task::Poll::Pending => return std::task::Poll::Pending,
            }
        }
    }
}
