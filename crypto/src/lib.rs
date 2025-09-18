// Copyright(C) Facebook, Inc. and its affiliates.
use rand::{RngCore};
use zk::{AsBytes, Fr, ToHash};
use std::fmt;
use base64::{Engine as _, engine::general_purpose};
use serde::{ser, de, Serialize, Deserialize};
use std::array::TryFromSliceError;
use std::convert::{TryFrom, TryInto};

#[cfg(test)]
#[path = "tests/crypto_tests.rs"]
pub mod crypto_tests;

/// Represents a hash digest (32 bytes).
#[derive(Hash, PartialEq, Default, Eq, Clone, Deserialize, Serialize, Ord, PartialOrd)]
pub struct Digest(pub [u8; 32]);

impl Digest {
    pub fn to_vec(&self) -> Vec<u8> {
        self.0.to_vec()
    }

    pub fn size(&self) -> usize {
        self.0.len()
    }

    pub fn to_field(&self) -> Fr {
        Fr::dec(&mut self.to_vec().into_iter()).expect("Failed to convert Digest to Fr")
    }

    pub fn from_field(f: Fr) -> Self {
        let mut bytes = [0u8; 32];
        f.enc().enumerate().for_each(|(i, b)| bytes[i] = b);
        Digest(bytes)
    }
}

impl fmt::Debug for Digest {
    fn fmt(&self, f: &mut fmt::Formatter) -> Result<(), fmt::Error> {
        write!(f, "{}", general_purpose::STANDARD.encode(&self.0))
    }
}

impl fmt::Display for Digest {
    fn fmt(&self, f: &mut fmt::Formatter) -> Result<(), fmt::Error> {
        write!(f, "{}", general_purpose::STANDARD.encode(&self.0))
    }
}

impl AsRef<[u8]> for Digest {
    fn as_ref(&self) -> &[u8] {
        &self.0
    }
}

impl TryFrom<&[u8]> for Digest {
    type Error = TryFromSliceError;
    fn try_from(item: &[u8]) -> Result<Self, Self::Error> {
        Ok(Digest(item.try_into()?))
    }
}

/// This trait is implemented by all messages that can be hashed.
pub trait Hash {
    fn digest(&self) -> Digest;
}

/// Represents a public key (in bytes).
#[derive(Copy, Clone, Eq, PartialEq, Hash, Ord, PartialOrd)]
pub struct PublicKey(pub [u8; 48]);

impl PublicKey {
    pub fn encode_base64(&self) -> String {
        general_purpose::STANDARD.encode(&self.0)
    }

    pub fn decode_base64(s: &str) -> Result<Self, base64::DecodeError> {
        let bytes = general_purpose::STANDARD.decode(s)?;
        let array = bytes[..48]
            .try_into()
            .map_err(|_| base64::DecodeError::InvalidLength)?;
        Ok(Self(array))
    }

    pub fn to_hash(&self) -> Fr {
        let mut chunk1 = [0u8; 32];
        let mut chunk2 = [0u8; 32];
        chunk1[8..].copy_from_slice(&self.0[..24]);
        chunk2[8..].copy_from_slice(&self.0[24..]);
        let fr1 = Fr::dec(&mut chunk1.to_vec().into_iter()).expect("Failed to convert PublicKey to Fr");
        let fr2 = Fr::dec(&mut chunk2.to_vec().into_iter()).expect("Failed to convert PublicKey to Fr");
        (fr1, fr2).hash()
    }

    pub fn verify_signature(&self, msg: &[u8], sig: &Signature) -> bool {
        let dst = b"BLS_SIG_BLS12381G2_XMD:SHA-256_SSWU_RO_NUL_";
        let signature = match blst::min_pk::Signature::from_bytes(&sig.0) {
            Ok(sig) => sig,
            Err(_) => return false,
        };
        let pk = match blst::min_pk::PublicKey::from_bytes(&self.0) {
            Ok(public_key) => public_key,
            Err(_) => return false,
        };
        let err = signature.verify(true, &msg, dst, &[], &pk, true);
        if err != blst::BLST_ERROR::BLST_SUCCESS {
            return false
        }
        true
    }
}

impl Default for PublicKey {
    fn default() -> Self {
        PublicKey([0u8; 48])
    }
}

impl fmt::Debug for PublicKey {
    fn fmt(&self, f: &mut fmt::Formatter) -> Result<(), fmt::Error> {
        write!(f, "{}", self.encode_base64())
    }
}

impl fmt::Display for PublicKey {
    fn fmt(&self, f: &mut fmt::Formatter) -> Result<(), fmt::Error> {
        write!(f, "{}", self.encode_base64().get(0..16).unwrap())
    }
}

impl Serialize for PublicKey {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: ser::Serializer,
    {
        serializer.serialize_str(&self.encode_base64())
    }
}

impl<'de> Deserialize<'de> for PublicKey {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: de::Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        let value = Self::decode_base64(&s).map_err(|e| de::Error::custom(e.to_string()))?;
        Ok(value)
    }
}

impl AsRef<[u8]> for PublicKey {
    fn as_ref(&self) -> &[u8] {
        &self.0
    }
}

/// Represents a secret key (in bytes).
pub struct SecretKey([u8; 32]);

impl SecretKey {
    pub fn encode_base64(&self) -> String {
        general_purpose::STANDARD.encode(&self.0)
    }

    pub fn decode_base64(s: &str) -> Result<Self, base64::DecodeError> {
        let bytes = general_purpose::STANDARD.decode(s)?;
        let array = bytes[..32]
            .try_into()
            .map_err(|_| base64::DecodeError::InvalidLength)?;
        Ok(Self(array))
    }

    pub fn sign_msg(&self, msg: &[u8]) -> Signature {
        let dst = b"BLS_SIG_BLS12381G2_XMD:SHA-256_SSWU_RO_NUL_";
        let sk = blst::min_pk::SecretKey::from_bytes(&self.0).unwrap();
        Signature(sk.sign(&msg, dst, &[]).to_bytes())
    }
}

impl Serialize for SecretKey {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: ser::Serializer,
    {
        serializer.serialize_str(&self.encode_base64())
    }
}

impl<'de> Deserialize<'de> for SecretKey {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: de::Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        let value = Self::decode_base64(&s).map_err(|e| de::Error::custom(e.to_string()))?;
        Ok(value)
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Hash, Ord, PartialOrd)]
pub struct Signature(pub [u8; 96]);

impl Signature {
    pub fn encode_base64(&self) -> String {
        general_purpose::STANDARD.encode(&self.0[..])
    }

    pub fn decode_base64(s: &str) -> Result<Self, base64::DecodeError> {
        let bytes = general_purpose::STANDARD.decode(s)?;
        let array = bytes[..96]
            .try_into()
            .map_err(|_| base64::DecodeError::InvalidLength)?;
        Ok(Self(array))
    }
}

impl Serialize for Signature {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: ser::Serializer,
    {
        serializer.serialize_str(&self.encode_base64())
    }
}

impl<'de> Deserialize<'de> for Signature {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: de::Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        let value = Self::decode_base64(&s).map_err(|e| de::Error::custom(e.to_string()))?;
        Ok(value)
    }
}

impl Default for Signature {
    fn default() -> Self {
        Signature([0u8; 96])
    }
}

pub fn generate_production_keypair() -> (PublicKey, SecretKey) {
    // gen rand sk
    let mut rng = rand::thread_rng();
    let mut ikm = [0u8; 32];
    rng.fill_bytes(&mut ikm);
    let sk = blst::min_pk::SecretKey::key_gen(&ikm, &[]).unwrap();

    // calculate pk
    let pk = sk.sk_to_pk();
    (PublicKey(pk.to_bytes()), SecretKey(sk.to_bytes()))
}
