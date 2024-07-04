use solana_sdk::pubkey::Pubkey;

pub mod account_types;
pub mod inmemory_token_storage;
pub mod token_program_utils;

pub const TOKEN_PROGRAM_ID: Pubkey = spl_token::id();
pub const TOKEN_PROGRAM_2022_ID: Pubkey = spl_token_2022::id();
