use solana_sdk::clock::Slot;

#[derive(PartialEq, Eq, Debug, Clone, Copy)]
pub struct SlotInfo {
    pub slot: Slot,
    pub parent: Slot,
    pub root: Slot,
}
