use std::collections::HashSet;

use lite_account_manager_common::account_store_interface::AccountLoadingError;
use solana_sdk::pubkey::Pubkey;

use crate::account_types::{Program, TokenAccount, TokenAccountIndex};

// Interface to store token accounts
pub trait TokenAccountStorageInterface: Sync + Send {
    fn contains(&self, pubkey: &Pubkey) -> Option<TokenAccountIndex>;

    fn save_or_update(&self, token_account: TokenAccount) -> (TokenAccountIndex, bool);

    fn get_by_index(
        &self,
        indexes: HashSet<TokenAccountIndex>,
    ) -> Result<Vec<TokenAccount>, AccountLoadingError>;

    fn get_by_pubkey(&self, pubkey: &Pubkey) -> Option<TokenAccount>;

    fn delete(&self, pubkey: &Pubkey);

    fn create_snapshot(&self, program: Program) -> Result<Vec<Vec<u8>>, AccountLoadingError>;
}
