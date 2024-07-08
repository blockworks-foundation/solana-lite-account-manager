use std::collections::HashSet;

use async_trait::async_trait;
use lite_account_manager_common::account_store_interface::AccountLoadingError;
use solana_sdk::pubkey::Pubkey;

use crate::account_types::{Program, TokenAccount, TokenAccountIndex};

// Interface to store token accounts
#[async_trait]
pub trait TokenAccountStorageInterface: Sync + Send {
    async fn contains(&self, pubkey: &Pubkey) -> Option<TokenAccountIndex>;

    async fn save_or_update(&self, token_account: TokenAccount) -> (TokenAccountIndex, bool);

    async fn get_by_index(
        &self,
        indexes: HashSet<TokenAccountIndex>,
    ) -> Result<Vec<TokenAccount>, AccountLoadingError>;

    async fn get_by_pubkey(&self, pubkey: &Pubkey) -> Option<TokenAccount>;

    async fn delete(&self, pubkey: &Pubkey);

    async fn create_snapshot(&self, program: Program) -> Result<Vec<Vec<u8>>, AccountLoadingError>;
}
