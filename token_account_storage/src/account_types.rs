use serde::{Deserialize, Serialize};
use solana_sdk::pubkey::Pubkey;

#[derive(Debug, Serialize, Deserialize, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
#[repr(u8)]
pub enum Program {
    TokenProgram,
    Token2022Program,
}

#[derive(Debug, Serialize, Deserialize, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum TokenProgramAccountState {
    Uninitialized,
    Initialized,
    Frozen,
}

impl TokenProgramAccountState {
    pub fn into_spl_state(&self) -> spl_token::state::AccountState {
        match self {
            TokenProgramAccountState::Uninitialized => {
                spl_token::state::AccountState::Uninitialized
            }
            TokenProgramAccountState::Initialized => spl_token::state::AccountState::Initialized,
            TokenProgramAccountState::Frozen => spl_token::state::AccountState::Frozen,
        }
    }

    pub fn into_spl_2022_state(&self) -> spl_token_2022::state::AccountState {
        match self {
            TokenProgramAccountState::Uninitialized => {
                spl_token_2022::state::AccountState::Uninitialized
            }
            TokenProgramAccountState::Initialized => {
                spl_token_2022::state::AccountState::Initialized
            }
            TokenProgramAccountState::Frozen => spl_token_2022::state::AccountState::Frozen,
        }
    }
}

impl From<spl_token::state::AccountState> for TokenProgramAccountState {
    fn from(value: spl_token::state::AccountState) -> Self {
        match value {
            spl_token::state::AccountState::Uninitialized => Self::Uninitialized,
            spl_token::state::AccountState::Initialized => Self::Initialized,
            spl_token::state::AccountState::Frozen => Self::Frozen,
        }
    }
}

impl From<spl_token_2022::state::AccountState> for TokenProgramAccountState {
    fn from(value: spl_token_2022::state::AccountState) -> Self {
        match value {
            spl_token_2022::state::AccountState::Uninitialized => Self::Uninitialized,
            spl_token_2022::state::AccountState::Initialized => Self::Initialized,
            spl_token_2022::state::AccountState::Frozen => Self::Frozen,
        }
    }
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
