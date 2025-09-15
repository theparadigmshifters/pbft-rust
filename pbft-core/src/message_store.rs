use std::collections::{BTreeMap};

use crate::{
    AcceptedProposal, ProposeBlockMsg, MessageDigest,
    NULL_DIGEST,
};
type MessageLog = BTreeMap<u64, StoredMessage>;

pub enum StoredMessage {
    AcceptedProposal(AcceptedProposal),
    Null { sequence: u64 },
}

impl StoredMessage {
    pub fn digest(&self) -> MessageDigest {
        match self {
            StoredMessage::AcceptedProposal(req) => req.proposal.digest(),
            StoredMessage::Null { sequence: _ } => NULL_DIGEST,
        }
    }

    pub fn set_opreation_result(&mut self, result: bool) {
        match self {
            StoredMessage::AcceptedProposal(req) => req.result = result,
            StoredMessage::Null { sequence: _ } => (),
        }
    }
}

pub struct MessageStore {
    message_log: MessageLog,
}

impl MessageStore {
    pub fn new() -> Self {
        Self {
            message_log: BTreeMap::new(),
        }
    }

    pub fn insert_proposal(&mut self, sequence: u64, proposal: ProposeBlockMsg) {
        self.inner_insert(
            sequence,
            StoredMessage::AcceptedProposal(AcceptedProposal {
                sequence,
                proposal,
                result: false,
            }),
        );
    }

    pub fn insert_null(&mut self, sequence: u64) {
        self.inner_insert(sequence, StoredMessage::Null { sequence });
    }

    fn inner_insert(&mut self, sequence: u64, msg: StoredMessage) {
        if let Some(old_req) = self.message_log.insert(sequence, msg) {
            self.message_log.insert(sequence, old_req); // Put back the old request

            // TODO: not sure how to best handle this, it should not happen but
            // panic is probably not good...
            panic!(
                "Request with sequence already exists! Sequence: {}",
                sequence
            );
        }
    }

    pub fn get_by_seq_mut(&mut self, seq: u64) -> Option<&mut StoredMessage> {
        self.message_log.get_mut(&seq)
    }

    pub fn has_message(&self, seq: u64) -> bool {
        self.message_log.contains_key(&seq)
    }
}
