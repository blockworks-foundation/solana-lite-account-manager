use solana_sdk::clock::Slot;

use crate::commitment::Commitment;

#[derive(PartialEq, Eq, Debug, Clone, Copy)]
pub struct SlotInfo {
    pub slot: Slot,
    pub parent: Slot,
    pub root: Slot,
}


#[derive(PartialEq, Eq, Debug, Clone, Copy)]
pub struct SlotInfoWithCommitment {
    pub info: SlotInfo,
    pub commitment: Commitment,
}
