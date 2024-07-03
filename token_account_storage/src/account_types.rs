use serde::{Deserialize, Serialize};
use solana_sdk::pubkey::Pubkey;

#[derive(Debug, Serialize, Deserialize, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
#[repr(u8)]
pub enum Program {
    TokenProgram,
    Token2022Program,
}

#[derive(Debug, Serialize, Deserialize, Clone, Copy)]
#[repr(u8)]
pub enum TokenProgramAccountState {
    Uninitialized,
    Initialized,
    Frozen,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct TokenAccount {
    pub program: Program,
    pub pubkey: Pubkey,
    pub mint: u64,
    pub owner: Pubkey,
    pub amount: u64,
    pub state: TokenProgramAccountState,
    pub delegate: Option<(Pubkey, u64)>,
    pub is_native: Option<u64>,
    pub close_authority: Option<Pubkey>,
    pub additional_data: Option<Vec<u8>>,
    pub lamports: u64,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct MintAccount {
    pub program: Program,
    pub pubkey: Pubkey,
    pub lamports: u64,
    pub supply: u64,
    pub decimals: u8,
    pub is_initialized: bool,
    pub mint_authority: Option<Pubkey>,
    pub freeze_authority: Option<Pubkey>,
    pub additional_data: Option<Vec<u8>>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct MultiSig {
    pub program: Program,
    pub pubkey: Pubkey,
    pub lamports: u64,
    pub m: u8,
    pub n: u8,
    pub is_initialized: bool,
    pub signers: Vec<Pubkey>,
}
