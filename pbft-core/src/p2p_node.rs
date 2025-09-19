use anyhow::Result;
use base64::{engine::general_purpose, Engine as _};
use futures::prelude::*;
use libp2p::{
    identify, identity, kad, noise, ping,
    swarm::{NetworkBehaviour, SwarmEvent},
    tcp, yamux, Multiaddr, PeerId,
};
use tracing::{info, warn, debug};
use std::collections::HashSet;
use std::num::NonZeroUsize;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use crate::{config::{CommitteeConfig, NodeId}, msg_protocol::{MsgEvent, MsgProtocol}};

#[derive(NetworkBehaviour)]
pub struct P2pBehaviour {
    ping: ping::Behaviour,
    kad: kad::Behaviour<kad::store::MemoryStore>,
    identify: identify::Behaviour,
    msg: MsgProtocol,
}

#[derive(Debug, Clone)]
pub struct P2pConfig {
    pub port: u16,
    pub bootstrap_nodes: Vec<String>,
    pub min_peers: u32,
    pub max_peers: u32,
    pub idle_timeout: Duration,
    pub ping_interval: Duration,
    pub bootstrap_interval: Duration,
    pub discovery_interval: Duration,
    pub disconnect_on_error: bool,
    pub private_key: Option<String>,
}

impl Default for P2pConfig {
    fn default() -> Self {
        Self {
            port: env_var_or("EON_PORT", 0),
            bootstrap_nodes: env_var_or("EON_BOOTSTRAP_NODES", String::new()).split(',').filter(|s| !s.is_empty()).map(|s| s.trim().to_string()).collect(),
            min_peers: env_var_or("EON_MIN_PEERS", 3),
            max_peers: env_var_or("EON_MAX_PEERS", 50),
            idle_timeout: Duration::from_secs(env_var_or("EON_IDLE_TIMEOUT", 600)),
            ping_interval: Duration::from_secs(env_var_or("EON_PING_INTERVAL", 30)),
            bootstrap_interval: Duration::from_secs(env_var_or("EON_BOOTSTRAP_INTERVAL", 300)),
            discovery_interval: Duration::from_secs(env_var_or("EON_DISCOVERY_INTERVAL", 60)),
            disconnect_on_error: env_var_or("EON_DISCONNECT_ON_ERROR", false),
            private_key: std::env::var("EON_PRIVATE_KEY").ok(),
        }
    }
}

impl P2pConfig {
    pub fn generate_private_key() -> String {
        let keypair = identity::Keypair::generate_ed25519();
        general_purpose::STANDARD.encode(keypair.to_protobuf_encoding().expect("Valid keypair"))
    }

    pub fn with_new_key() -> Self {
        Self { private_key: Some(Self::generate_private_key()), ..Default::default() }
    }
}

#[derive(Debug)]
enum NodeCommand {
    SendMessage(Option<NodeId>, Vec<u8>),
}

struct State {
    local_peer_id: PeerId,
    local_id: NodeId,
    discovered_peers: Arc<Mutex<HashSet<PeerId>>>,
    command_tx: mpsc::UnboundedSender<NodeCommand>,
}

pub struct P2pLibp2p {
    config: P2pConfig,
    committee_config: CommitteeConfig,
    state: Option<State>,
}

impl P2pLibp2p {
    pub fn default_from_committee(committee_config: CommitteeConfig) -> Self {
        Self::new(P2pConfig::default(), committee_config)
    }
}

impl P2pLibp2p {
    pub fn init(&mut self, inner_rx_cancel: tokio::sync::broadcast::Receiver<()>, on_msg: impl Fn(NodeId, Vec<u8>) -> Result<()> + Send + Sync + 'static) -> Result<()> {
        if self.state.is_some() {
            return Err(anyhow::anyhow!("Node already initialized"));
        }

        let keypair = self.create_keypair()?;
        let local_peer_id = PeerId::from(keypair.public());
        info!("Using peer ID: {}", local_peer_id);

        let (command_tx, command_rx) = mpsc::unbounded_channel();
        let discovered_peers = Arc::new(Mutex::new(HashSet::new()));

        self.spawn_event_loop(keypair, inner_rx_cancel, command_rx, discovered_peers.clone(), on_msg)?;

        let local_id = self.committee_config.committee.get(&local_peer_id)
            .copied()
            .ok_or_else(|| anyhow::anyhow!("Local peer ID not found in committee config"))?;
        self.state = Some(State { local_peer_id, local_id, discovered_peers, command_tx });

        Ok(())
    }

    pub fn send(&mut self, to: Option<NodeId>, msg: Vec<u8>) -> Result<()> {
        let state = self.state.as_ref().ok_or_else(|| anyhow::anyhow!("Node not initialized"))?;

        let len = msg.len();
        state.command_tx.send(NodeCommand::SendMessage(to, msg))?;
        info!("Sent message to {:?}: {} bytes", to, len);
        Ok(())
    }

    pub fn broadcast_proposal(&mut self, proposal_msg: crate::api::ProposeBlockMsgBroadcast) -> Result<()> {
        if self.state.is_none() {
            return Err(anyhow::anyhow!("Node not initialized"));
        }
        let p2p_msg = crate::api::P2pMessage::from_proposal(proposal_msg);
        let serialized = serde_json::to_vec(&p2p_msg)
            .map_err(|e| anyhow::anyhow!("Failed to serialize operation: {}", e))?;

        self.send(None, serialized)?;

        info!("Broadcast proposal message");

        Ok(())
    }

    pub fn broadcast_consensus_message(&mut self, consensus_msg: crate::api::ProtocolMessageBroadcast) -> Result<()> {
        if self.state.is_none() {
            return Err(anyhow::anyhow!("Node not initialized"));
        }
        let consensus_msg = crate::api::P2pMessage::from_consensus(consensus_msg);
        let serialized = serde_json::to_vec(&consensus_msg)
            .map_err(|e| anyhow::anyhow!("Failed to serialize consensus message: {}", e))?;

        self.send(None, serialized)?;

        info!("Broadcast consensus message");

        Ok(())
    }
}

impl P2pLibp2p {
    pub fn new(config: P2pConfig, committee_config: CommitteeConfig) -> Self {
        Self { config, committee_config, state: None }
    }

    pub fn connected_peers(&self) -> Vec<PeerId> {
        self.state.as_ref().map(|s| s.discovered_peers.lock().unwrap().iter().cloned().collect()).unwrap_or_default()
    }

    pub fn discover_peers(&self) {
        if let Some(state) = &self.state {
            let _ = state.command_tx.send(NodeCommand::SendMessage(None, vec![]));
        }
    }

    pub fn get_private_key(&self) -> Option<String> {
        self.config.private_key.clone()
    }

    pub fn local_peer_id(&self) -> PeerId {
        self.state.as_ref().map(|s| s.local_peer_id).expect("Node not initialized")
    }

    pub fn local_id(&self) -> NodeId {
        self.state.as_ref().map(|s| s.local_id).expect("Node not initialized")
    }

    pub fn discovered_peers(&self) -> Vec<PeerId> {
        self.state.as_ref().map(|s| s.discovered_peers.lock().unwrap().iter().cloned().collect()).unwrap_or_default()
    }

    pub fn is_initialized(&self) -> bool {
        self.state.is_some()
    }

    fn create_keypair(&self) -> Result<identity::Keypair> {
        if let Some(ref private_key_b64) = self.config.private_key {
            let private_key_bytes = general_purpose::STANDARD.decode(private_key_b64).map_err(|e| anyhow::anyhow!("Failed to decode private key: {}", e))?;

            identity::Keypair::from_protobuf_encoding(&private_key_bytes).map_err(|e| anyhow::anyhow!("Failed to parse private key: {}", e))
        } else {
            let new_keypair = identity::Keypair::generate_ed25519();
            info!("Generated new keypair. To reuse this identity, set EON_PRIVATE_KEY={}", general_purpose::STANDARD.encode(new_keypair.to_protobuf_encoding().expect("Valid keypair")));
            Ok(new_keypair)
        }
    }

    fn spawn_event_loop(
        &self, keypair: identity::Keypair, mut shutdown_rx: tokio::sync::broadcast::Receiver<()>, mut command_rx: mpsc::UnboundedReceiver<NodeCommand>, discovered_peers: Arc<Mutex<HashSet<PeerId>>>,
        on_msg: impl Fn(NodeId, Vec<u8>) -> Result<()> + Send + Sync + 'static,
    ) -> Result<JoinHandle<()>> {
        let config = self.config.clone();
        let committee_config = self.committee_config.clone();
        let handle = tokio::spawn(async move {
            let mut swarm = Self::build_swarm(committee_config, keypair, &config, on_msg).await;

            if let Err(e) = Self::setup_listening(&mut swarm, config.port).await {
                warn!("Failed to setup listening: {}", e);
                return;
            }

            Self::bootstrap_network(&mut swarm, &config.bootstrap_nodes).await;

            let mut periodic_bootstrap = tokio::time::interval(config.bootstrap_interval);
            let mut peer_discovery = tokio::time::interval(config.discovery_interval);
            periodic_bootstrap.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            peer_discovery.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

            loop {
                tokio::select! {
                    _ = shutdown_rx.recv() => {
                        info!("Shutting down P2P node");
                        return;
                    }
                    _ = periodic_bootstrap.tick() => {
                        if !config.bootstrap_nodes.is_empty() {
                            debug!("Periodic bootstrap...");
                            let _ = swarm.behaviour_mut().kad.bootstrap();
                        }
                    }
                    _ = peer_discovery.tick() => {
                        Self::check_peer_count(&mut swarm, &config);
                    }
                    Some(cmd) = command_rx.recv() => {
                        Self::handle_command(&mut swarm, cmd);
                    }
                    swarm_event = swarm.select_next_some() => {
                        Self::handle_swarm_event(
                            &mut swarm,
                            swarm_event,
                            &config,
                            &discovered_peers,
                        ).await;
                    }
                }
            }
        });

        Ok(handle)
    }

    pub async fn wait_for_min_peers(&self) -> Result<()> {
        let timeout_duration = Duration::from_secs(300);
        let start_time = std::time::Instant::now();
        
        info!("Waiting for minimum {} peers to connect...", self.config.min_peers);
        
        while start_time.elapsed() < timeout_duration {
            let connected_count = self.connected_peers().len();
            
            if connected_count >= self.config.min_peers as usize {
                info!("✅ Minimum peer count reached: {}/{}", connected_count, self.config.min_peers);
                return Ok(());
            }
            
            debug!("Waiting for peers: connected={}, required={}, elapsed={}s", 
                  connected_count, 
                  self.config.min_peers,
                  start_time.elapsed().as_secs());
            
            tokio::time::sleep(Duration::from_secs(2)).await;
        }
        
        let connected_count = self.connected_peers().len();
        Err(anyhow::anyhow!(
            "Timeout waiting for minimum peers. Connected: {}, Required: {}", 
            connected_count, 
            self.config.min_peers
        ))
    }
    
    async fn build_swarm(committee_config: CommitteeConfig, keypair: identity::Keypair, config: &P2pConfig, on_msg: impl Fn(NodeId, Vec<u8>) -> Result<()> + Send + Sync + 'static) -> libp2p::Swarm<P2pBehaviour> {
        let local_peer_id = PeerId::from(keypair.public());

        libp2p::SwarmBuilder::with_existing_identity(keypair)
            .with_tokio()
            .with_tcp(tcp::Config::default(), noise::Config::new, yamux::Config::default)
            .unwrap()
            .with_behaviour(|key| {
                let mut kad_config = kad::Config::new(kad::PROTOCOL_NAME);
                kad_config.set_query_timeout(Duration::from_secs(60));
                kad_config.set_replication_factor(NonZeroUsize::new(3).unwrap());
                kad_config.set_parallelism(NonZeroUsize::new(3).unwrap());
                kad_config.disjoint_query_paths(true);

                let store = kad::store::MemoryStore::new(local_peer_id);
                let mut kademlia = kad::Behaviour::with_config(local_peer_id, store, kad_config);
                kademlia.set_mode(Some(kad::Mode::Server));

                Ok(P2pBehaviour {
                    ping: ping::Behaviour::new(ping::Config::new().with_interval(config.ping_interval)),
                    kad: kademlia,
                    identify: identify::Behaviour::new(identify::Config::new("/pbft-p2p/1.0.0".to_string(), key.public())),
                    msg: MsgProtocol::new_with_callback(committee_config, on_msg, config.disconnect_on_error),
                })
            })
            .unwrap()
            .with_swarm_config(|c| c.with_idle_connection_timeout(config.idle_timeout))
            .build()
    }

    async fn setup_listening(swarm: &mut libp2p::Swarm<P2pBehaviour>, port: u16) -> Result<()> {
        let listen_addr: Multiaddr = format!("/ip4/0.0.0.0/tcp/{}", port).parse()?;
        swarm.listen_on(listen_addr)?;
        Ok(())
    }

    async fn bootstrap_network(swarm: &mut libp2p::Swarm<P2pBehaviour>, bootstrap_nodes: &[String]) {
        for bootstrap_addr in bootstrap_nodes {
            if let Ok(addr) = bootstrap_addr.parse::<Multiaddr>() {
                if let Some(peer_id) = extract_peer_id(&addr) {
                    info!("Adding bootstrap node: {} at {}", peer_id, addr);
                    swarm.behaviour_mut().kad.add_address(&peer_id, addr.clone());
                }
                let _ = swarm.dial(addr);
            } else {
                warn!("Invalid bootstrap address: {}", bootstrap_addr);
            }
        }

        if !bootstrap_nodes.is_empty() {
            let _ = swarm.behaviour_mut().kad.bootstrap();
        }
    }

    fn check_peer_count(swarm: &mut libp2p::Swarm<P2pBehaviour>, config: &P2pConfig) {
        let connection_count = swarm.connected_peers().count();
        debug!("Connected peers: {} (min: {}, max: {})", connection_count, config.min_peers, config.max_peers);

        if connection_count < config.min_peers as usize {
            info!("Below minimum peers, starting peer discovery...");
            swarm.behaviour_mut().kad.get_closest_peers(PeerId::random());
        }
    }

    fn handle_command(swarm: &mut libp2p::Swarm<P2pBehaviour>, cmd: NodeCommand) {
        match cmd {
            NodeCommand::SendMessage(to, payload) => {
                if let Err(e) = swarm.behaviour_mut().msg.send_message(to, payload) {
                    warn!("Failed to send message: {}", e);
                }
            }
        }
    }

    async fn handle_swarm_event(swarm: &mut libp2p::Swarm<P2pBehaviour>, event: SwarmEvent<P2pBehaviourEvent>, config: &P2pConfig, discovered_peers: &Arc<Mutex<HashSet<PeerId>>>) {
        match event {
            SwarmEvent::NewListenAddr { address, .. } => {
                info!("Listening on {:?}", address);
            }
            SwarmEvent::ConnectionEstablished { peer_id, .. } => {
                info!("Connected to peer: {}", peer_id);
                discovered_peers.lock().unwrap().insert(peer_id);
                Self::enforce_max_peers(swarm, config);
            }
            SwarmEvent::ConnectionClosed { peer_id, .. } => {
                info!("Disconnected from peer: {}", peer_id);
                Self::check_peer_count(swarm, config);
            }
            SwarmEvent::Behaviour(P2pBehaviourEvent::Kad(event)) => {
                Self::handle_kad_event(swarm, event, config, discovered_peers).await;
            }
            SwarmEvent::Behaviour(P2pBehaviourEvent::Identify(identify::Event::Received { peer_id, info, .. })) => {
                debug!("Identified peer {} with {} addresses", peer_id, info.listen_addrs.len());
                discovered_peers.lock().unwrap().insert(peer_id);
                for addr in info.listen_addrs {
                    swarm.behaviour_mut().kad.add_address(&peer_id, addr);
                }
                if discovered_peers.lock().unwrap().len() > 1 {
                    let _ = swarm.behaviour_mut().kad.bootstrap();
                }
            }
            SwarmEvent::Behaviour(P2pBehaviourEvent::Msg(MsgEvent::MessageReceived { .. })) => {
                Self::disconnect_error_peers(swarm);
            }
            SwarmEvent::OutgoingConnectionError { peer_id, error, .. } => {
                if let Some(peer) = peer_id {
                    warn!("Failed to connect to {}: {}", peer, error);
                }
            }
            _ => {}
        }
    }

    async fn handle_kad_event(swarm: &mut libp2p::Swarm<P2pBehaviour>, event: kad::Event, config: &P2pConfig, discovered_peers: &Arc<Mutex<HashSet<PeerId>>>) {
        match event {
            kad::Event::RoutingUpdated { peer, addresses, .. } => {
                debug!("Kademlia routing updated for peer: {}", peer);
                discovered_peers.lock().unwrap().insert(peer);
                let addrs: Vec<Multiaddr> = addresses.iter().cloned().collect();
                Self::auto_connect_if_needed(swarm, config, peer, &addrs);
            }
            kad::Event::OutboundQueryProgressed { result: kad::QueryResult::GetClosestPeers(result), .. } => {
                if let Ok(ok) = result {
                    if !ok.peers.is_empty() {
                        info!("Found {} peers in DHT", ok.peers.len());
                        for peer_info in &ok.peers {
                            discovered_peers.lock().unwrap().insert(peer_info.peer_id);
                            Self::auto_connect_if_needed(swarm, config, peer_info.peer_id, &peer_info.addrs);
                        }
                    }
                }
            }
            _ => {}
        }
    }

    fn auto_connect_if_needed(swarm: &mut libp2p::Swarm<P2pBehaviour>, config: &P2pConfig, peer: PeerId, addresses: &[Multiaddr]) {
        let connection_count = swarm.connected_peers().count();
        if connection_count < config.min_peers as usize && connection_count < config.max_peers as usize && !swarm.is_connected(&peer) && !addresses.is_empty() {
            let addr = &addresses[0];
            info!("Auto-connecting to {} at {}", peer, addr);
            swarm.dial(addr.clone()).ok();
        }
    }

    fn enforce_max_peers(swarm: &mut libp2p::Swarm<P2pBehaviour>, config: &P2pConfig) {
        let connection_count = swarm.connected_peers().count();
        if connection_count > config.max_peers as usize {
            warn!("Exceeded maximum peer limit, disconnecting from a peer");
            let peer_to_disconnect = swarm.connected_peers().find(|&peer| !is_bootstrap_peer(peer, &config.bootstrap_nodes)).cloned();

            if let Some(peer_to_disconnect) = peer_to_disconnect {
                info!("Disconnecting from peer: {}", peer_to_disconnect);
                swarm.disconnect_peer_id(peer_to_disconnect).ok();
            }
        }
    }

    fn disconnect_error_peers(swarm: &mut libp2p::Swarm<P2pBehaviour>) {
        let peers_to_disconnect = swarm.behaviour_mut().msg.get_peers_to_disconnect();
        for peer in peers_to_disconnect {
            warn!("Disconnecting {} due to message error", peer);
            swarm.disconnect_peer_id(peer).ok();
        }
    }
}

fn env_var_or<T: std::str::FromStr>(key: &str, default: T) -> T {
    std::env::var(key).ok().and_then(|s| s.parse().ok()).unwrap_or(default)
}

fn extract_peer_id(addr: &Multiaddr) -> Option<PeerId> {
    addr.iter().find_map(|p| match p {
        libp2p::multiaddr::Protocol::P2p(id) => Some(id),
        _ => None,
    })
}

fn is_bootstrap_peer(peer: &PeerId, bootstrap_nodes: &[String]) -> bool {
    bootstrap_nodes.iter().any(|b| b.contains(&peer.to_string()))
}
