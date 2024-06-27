use std::fmt::Display;

use serde::Serializer;
use solana_sdk::commitment_config::CommitmentConfig;

#[derive(PartialEq, Eq, PartialOrd, Ord, Clone, Copy)]
#[repr(u8)]
pub enum Commitment {
    Processed = 0,
    Confirmed = 1,
    Finalized = 2,
}

impl From<CommitmentConfig> for Commitment {
    fn from(value: CommitmentConfig) -> Self {
        if value.is_finalized() {
            Self::Finalized
        } else if value.is_confirmed() {
            Self::Confirmed
        } else {
            Self::Processed
        }
    }
}

impl Display for Commitment {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Commitment::Processed => f.serialize_str("processed"),
            Commitment::Confirmed => f.serialize_str("confirmed"),
            Commitment::Finalized => f.serialize_str("finalized"),
        }
    }
}
