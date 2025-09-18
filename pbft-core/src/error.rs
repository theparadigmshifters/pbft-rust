use crypto::Digest;
use thiserror;

use crate::{config::NodeId, pbft_state::ReplicaState, MessageType};

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error(
        "Request digest does not match consensus message: expected {expected:?}, actual {actual:?}"
    )]
    InvalidDigest {
        expected: Digest,
        actual: Digest,
    },

    #[error("Replica in state {state:?} cannot handle message of type {message_type:?}")]
    ReplicaInStateCannotHandleMessage {
        state: ReplicaState,
        message_type: &'static str,
    },

    #[error("PrePrepare message not from leader {leader_id:?}, sender {sender_id:?}")]
    PrePrepareMessageNotFromLeader { leader_id: u64, sender_id: u64 },

    #[error("Message replica view {msg_view:?} does not match replica view {replica_view:?}")]
    MessageReplicaViewsMissmatch { msg_view: u64, replica_view: u64 },

    #[error("Message sequence outside watermarks range: sequence {sequence}, low watermark {low}, high watermark {high}")]
    MessageSequenceOutsideWatermarksRange { sequence: u64, low: u64, high: u64 },

    #[error("Different PrePrepare for view {view:?} and sequence {sequence:?} already accepted")]
    PrePrepareForViewAndSequenceAlreadyAccepted { view: u64, sequence: u64 },

    #[error("Prepare for view {view:?} and sequence {sequence:?} does not match digest")]
    PrepareForViewAndSequenceDoesNotMatchDigest {
        view: u64,
        sequence: u64,
        expected: Digest,
        actual: Digest,
    },

    #[error("Commit for view {view:?} and sequence {sequence:?} does not match digest")]
    CommitForViewAndSequenceDoesNotMatchDigest {
        view: u64,
        sequence: u64,
        expected: Digest,
        actual: Digest,
    },

    #[error("PrePrepare message without client request")]
    PrePrepareMessageWithoutClientRequest { view: u64, sequence: u64 },

    #[error("Replica private key does not match public key in config: actual {actual}, expected {expected}")]
    ReplicaPrivKeyDoesNotMatchPubKey { actual: String, expected: String },

    #[error("Hex error: {context}: {error}")]
    Base64Error {
        context: String,
        error: base64::DecodeError,
    },

    #[error("IO error: {context}: {error}")]
    IOError {
        context: String,
        error: std::io::Error,
    },

    #[error("Serde JSON error: {context}: {error}")]
    SerdeJSONError {
        context: String,
        error: serde_json::Error,
    },

    #[error("Bincode error: {context}: {error}")]
    BincodeError {
        context: String,
        error: bincode::Error,
    },

    #[error("Broadcast error: {context}: {error}")]
    BroadcastError {
        context: String,
        error: crate::broadcast::BroadcastError,
    },

    #[error("Invalid replica id: {replica_id}")]
    InvalidReplicaID { replica_id: NodeId },

    #[error("Invalid message type: expected {expected:?}, actual {actual:?}")]
    InvalidMessageType { expected: MessageType, actual: MessageType },

    #[error("Invalid signature")]
    InvalidSignature,

    #[error("Invalid view change message: {0}")]
    InvalidViewChange(String),

    #[error(
        "Invalid message signature: Replica Id mismatch: expected: {expected}, actual: {actual}"
    )]
    InvalidMessageSignatureReplicaIdMismatch { expected: u64, actual: u64 },

    #[error("Request sequence does not match Protocol message metadata: request: {request_seq}, protocol_msg: {protocol_msg_seq}")]
    RequestSequenceDoesNotMatchMetadata {
        request_seq: u64,
        protocol_msg_seq: u64,
    },
}

// TODO: Macro for this?
impl Error {
    pub fn base64_error(context: &str) -> impl FnOnce(base64::DecodeError) -> Self + '_ {
        move |error| Self::Base64Error {
            context: context.to_string(),
            error,
        }
    }

    pub fn io_error(context: &str) -> impl FnOnce(std::io::Error) -> Self + '_ {
        move |error| Self::IOError {
            context: context.to_string(),
            error,
        }
    }

    pub fn serde_json_error(context: &str) -> impl FnOnce(serde_json::Error) -> Self + '_ {
        move |error| Self::SerdeJSONError {
            context: context.to_string(),
            error,
        }
    }

    pub fn bincode_error(context: &str) -> impl FnOnce(bincode::Error) -> Self + '_ {
        move |error| Self::BincodeError {
            context: context.to_string(),
            error,
        }
    }

    pub fn broadcast_error(
        context: &str,
    ) -> impl FnOnce(crate::broadcast::BroadcastError) -> Self + '_ {
        move |error| Self::BroadcastError {
            context: context.to_string(),
            error,
        }
    }

    pub fn is_executor_retrieable_error(&self) -> bool {
        match self {
            Self::ReplicaInStateCannotHandleMessage { .. } => true,
            Self::MessageReplicaViewsMissmatch {
                msg_view,
                replica_view,
            } => msg_view > replica_view,
            _ => false,
        }
    }
}
