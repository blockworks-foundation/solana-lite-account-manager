use solana_sdk::pubkey::Pubkey;

#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Debug)]
pub struct PartialPubkey<const SIZE: usize> {
    partial_pubkey_bytes: [u8; SIZE],
}

impl<const SIZE: usize> From<Pubkey> for PartialPubkey<SIZE> {
    fn from(value: Pubkey) -> Self {
        Self {
            partial_pubkey_bytes: value.to_bytes()[0..SIZE].try_into().unwrap(),
        }
    }
}

impl<const SIZE: usize> From<&Pubkey> for PartialPubkey<SIZE> {
    fn from(value: &Pubkey) -> Self {
        Self {
            partial_pubkey_bytes: value.to_bytes()[0..SIZE].try_into().unwrap(),
        }
    }
}
