use std::{collections::HashMap, ops::Deref};
use base64::{Engine as _, engine::general_purpose};
use hex::decode;
use l0::Blk;
use zk::{AsBytes, Fr, ToHash};
use crate::config::{NodeId, Secret};
use crypto::{Digest, PublicKey, Signature};
use serde::{Deserialize, Serialize};

pub mod api;
pub mod config;
pub mod error;
pub mod replica_client;
pub mod replica_api;
pub(crate) mod message_store;
pub mod pbft;
pub mod pbft_executor;
pub mod p2p_node;
pub mod msg_protocol;
pub(crate) mod pbft_state;
pub use crate::config::Config;
pub use pbft::Pbft;
pub use pbft_state::ReplicaState;
pub const NULL_DIGEST: Digest = Digest([0; 32]);

pub mod dev;

pub type Result<T> = std::result::Result<T, error::Error>;

#[derive(Debug, Clone, Serialize, Deserialize, Hash, PartialEq, Eq, Default)]
pub struct Block {
   pub payload: String
}

#[derive(Debug, Clone, Serialize, Deserialize, Hash, PartialEq, Eq)]
pub struct ClientRequestId(pub uuid::Uuid);

impl ClientRequestId {
    pub fn new() -> Self {
        Self(uuid::Uuid::new_v4())
    }
}

impl Default for ClientRequestId {
    fn default() -> Self {
        Self::new()
    }
}

impl std::fmt::Display for ClientRequestId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, Hash, PartialEq, Eq)]
pub struct ProposeBlockMsg {
    pub block: String,
}

impl ProposeBlockMsg {
    pub fn digest(&self) -> Digest {
        let blk_bytes = decode(&self.block).unwrap();
        let blk: Blk = Blk::dec(&mut blk_bytes.into_iter()).unwrap();
        let digest = blk.hash();
        Digest::from_field(digest)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, Hash, PartialEq, Eq)]
pub struct AcceptedProposal {
    pub sequence: u64,
    pub proposal: ProposeBlockMsg,
    pub result: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OperationAck {
    pub client_request: ProposeBlockMsg,
    pub leader_id: u64,
    pub sequence_number: u64,
    pub status: OperationStatus,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum OperationStatus {
    Accepted,
    AlreadyHandled(bool),
}

pub trait ReplicaId {
    fn replica_id(&self) -> NodeId;
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ProtocolMessage {
    // TODO: Not all of those need to be signed, so maybe it does not make sense
    // to make them as such
    PrePrepare(SignedPrePrepare),
    Prepare(SignedPrepare),
    Commit(SignedCommit),
    Checkpoint(SignedCheckpoint),
    ViewChange(SignedViewChange),
    NewView(SignedNewView),
}

impl ProtocolMessage {
    pub fn message_type_str(&self) -> &'static str {
        match self {
            ProtocolMessage::PrePrepare(_) => "PrePrepare",
            ProtocolMessage::Prepare(_) => "Prepare",
            ProtocolMessage::Commit(_) => "Commit",
            ProtocolMessage::Checkpoint(_) => "Checkpoint",
            ProtocolMessage::ViewChange(_) => "ViewChange",
            ProtocolMessage::NewView(_) => "NewView",
        }
    }

    pub fn in_view(&self) -> Option<u64> {
        match self {
            ProtocolMessage::PrePrepare(m) => Some(m.metadata.view),
            ProtocolMessage::Prepare(m) => Some(m.metadata.view),
            ProtocolMessage::Commit(m) => Some(m.metadata.view),
            ProtocolMessage::Checkpoint(_m) => None,
            ProtocolMessage::ViewChange(_m) => None,
            ProtocolMessage::NewView(_m) => None,
        }
    }

    pub fn sequence(&self) -> Option<u64> {
        match self {
            ProtocolMessage::PrePrepare(m) => Some(m.metadata.sequence),
            ProtocolMessage::Prepare(m) => Some(m.metadata.sequence),
            ProtocolMessage::Commit(m) => Some(m.metadata.sequence),
            ProtocolMessage::Checkpoint(m) => Some(m.sequence),
            ProtocolMessage::ViewChange(_m) => None,
            ProtocolMessage::NewView(_m) => None,
        }
    }

    pub fn replica_id(&self) -> Option<NodeId> {
        match self {
            ProtocolMessage::PrePrepare(_m) => None,
            ProtocolMessage::Prepare(m) => Some(m.replica_id),
            ProtocolMessage::Commit(m) => Some(m.replica_id),
            ProtocolMessage::Checkpoint(m) => Some(m.replica_id),
            ProtocolMessage::ViewChange(m) => Some(m.replica_id),
            ProtocolMessage::NewView(_m) => None,
        }
    }

    pub fn is_new_view(&self) -> bool {
        matches!(self, ProtocolMessage::NewView(_))
    }
}

// TODO: This could be a macro, or proc macro
impl ReplicaId for Prepare {
    fn replica_id(&self) -> NodeId {
        self.replica_id
    }
}
impl ReplicaId for Commit {
    fn replica_id(&self) -> NodeId {
        self.replica_id
    }
}
impl ReplicaId for Checkpoint {
    fn replica_id(&self) -> NodeId {
        self.replica_id
    }
}
impl ReplicaId for ViewChange {
    fn replica_id(&self) -> NodeId {
        self.replica_id
    }
}

impl From<SignedPrepare> for ProtocolMessage {
    fn from(p: SignedPrepare) -> Self {
        ProtocolMessage::Prepare(p)
    }
}
impl From<SignedCommit> for ProtocolMessage {
    fn from(p: SignedCommit) -> Self {
        ProtocolMessage::Commit(p)
    }
}
impl From<SignedCheckpoint> for ProtocolMessage {
    fn from(p: SignedCheckpoint) -> Self {
        ProtocolMessage::Checkpoint(p)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Copy)]
pub enum MessageType {
    Prepare,
    Commit
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct MessageMeta {
    pub view: u64,
    pub sequence: u64,
    pub digest: Digest,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct PrePrepare {
    pub metadata: MessageMeta,
}

impl PrePrepare {
    pub fn is_null(&self) -> bool {
        self.metadata.digest == NULL_DIGEST
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct Prepare {
    pub message_type: MessageType,
    pub replica_id: NodeId,
    pub metadata: MessageMeta,
}

impl Prepare {
    pub fn message_type(&self) -> MessageType {
        self.message_type.clone()
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct Commit {
    pub message_type: MessageType,
    pub replica_id: NodeId,
    pub metadata: MessageMeta,
}

impl Commit {
    pub fn message_type(&self) -> MessageType {
        self.message_type.clone()
    }

    pub fn sum(&self) -> Vec<u8> {
        let field1 = Fr::from(self.message_type as u32);
        let field2 = Fr::from(self.replica_id.0);
        let field3 = Fr::from(self.metadata.view);
        let field4 = Fr::from(self.metadata.sequence);
        let field5 = self.metadata.digest.to_field();
        let elements = vec![field1, field2, field3, field4, field5];
        let h: Vec<u8> = elements.hash().enc().collect();
        h
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct CheckpointDigest(pub [u8; 16]);

impl std::fmt::Display for CheckpointDigest {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", general_purpose::STANDARD.encode(self.0))
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Checkpoint {
    pub replica_id: NodeId,
    pub sequence: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ViewChange {
    pub replica_id: NodeId,
    pub view: u64,

    // Last stable checkpoint for given replica. It is an Option in case we do
    // not have any checkpoints yet.
    pub last_stable_checkpoint: Option<ViewChangeCheckpoint>,

    // Proof for each prepared message (by sequence), containing at least 2f+1
    // Prepare messages from different replicas for a given message.
    // Each proof contains the pre-prepare message and the prepare messages by
    // public key of different replicas.
    pub prepared_proofs: HashMap<u64, PreparedProof>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ViewChangeCheckpoint {
    pub sequence: u64,
    // Map public key to signed checkpoint message by the given replica.
    pub checkpoint_proofs: HashMap<String, SignedCheckpoint>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct PreparedProof {
    pub pre_prepare: SignedPrePrepare,
    // Map public key to signed prepare message by the given replica.
    pub prepares: HashMap<String, SignedPrepare>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NewView {
    pub view: u64,
    // Proof of View Change messages received from different replicas
    pub view_change_messages: HashMap<String, SignedViewChange>,
    // Pre-prepare messages for those that were prepared in previous view, but
    // were not included in the last stable checkpoint.
    pub pre_prepares: Vec<SignedPrePrepare>,
}

impl NewView {
    pub fn latest_sequence(&self) -> u64 {
        self.pre_prepares
            .iter()
            .map(|pp| pp.metadata.sequence)
            .max()
            // We expect the vaule to be there since we always have at least one
            // PrePrepare message with NULL digest.
            .unwrap()
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct SignedMessage<T> {
    pub message: T,
    pub signature: Signature,
    pub pub_key: PublicKey,
}

impl<T: Serialize + 'static> SignedMessage<T> {
    pub fn new(message: T, keypair: &Secret) -> Result<Self> {
        let msg = if std::any::TypeId::of::<T>() == std::any::TypeId::of::<Commit>() {
            let commit_msg = unsafe {
                let ptr = &message as *const T as *const Commit;
                &*ptr
            };
            commit_msg.sum()
        } else {
            bincode::serialize(&message).map_err(
                crate::error::Error::bincode_error("failed to serialize message"),
            )?
        };

        let signature = keypair.secret.sign_msg(&msg);
        Ok(Self {
            message,
            signature,
            pub_key: keypair.name,
        })
    }

    pub fn verify(&self) -> Result<bool> {
        let msg = if std::any::TypeId::of::<T>() == std::any::TypeId::of::<Commit>() {
            let commit_msg = unsafe {
                let ptr = &self.message as *const T as *const Commit;
                &*ptr
            };
            commit_msg.sum()
        } else {
            bincode::serialize(&self.message).map_err(
                crate::error::Error::bincode_error("failed to serialize message"),
            )?
        };

        Ok(self.pub_key.verify_signature(&msg, &self.signature))
    }

    pub fn pub_key_base64(&self) -> String {
        self.pub_key.encode_base64()
    }
}

impl<T: Serialize + ReplicaId + 'static> SignedMessage<T> {
    pub fn verify_replica_signature(&self, replica_id: NodeId) -> Result<bool> {
        if self.replica_id() != replica_id {
            return Err(error::Error::InvalidMessageSignatureReplicaIdMismatch {
                expected: replica_id.0,
                actual: self.replica_id().0,
            });
        }
        self.verify()
    }
}

impl<T> Deref for SignedMessage<T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        &self.message
    }
}

pub type SignedPrePrepare = SignedMessage<PrePrepare>;
pub type SignedPrepare = SignedMessage<Prepare>;
pub type SignedCommit = SignedMessage<Commit>;
pub type SignedCheckpoint = SignedMessage<Checkpoint>;
pub type SignedViewChange = SignedMessage<ViewChange>;
pub type SignedNewView = SignedMessage<NewView>;

pub trait SignMessage<T: Serialize> {
    fn sign(self, keypair: &Secret) -> Result<SignedMessage<T>>;
}

impl<T: Serialize + 'static> SignMessage<T> for T {
    fn sign(self, keypair: &Secret) -> Result<SignedMessage<T>> {
        SignedMessage::new(self, keypair)
    }
}
