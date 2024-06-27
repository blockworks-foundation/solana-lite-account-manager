use crate::{
    account_filter::{AccountFilter, AccountFilterType},
    account_store_interface::AccountStorageInterface,
};
use async_trait::async_trait;
use solana_sdk::pubkey::Pubkey;
use std::sync::Arc;

// Source to create snapshot and subscribe to new accounts
#[async_trait]
pub trait AccountsSourceInterface: Send + Sync {
    async fn subscribe_account(&self, account: Pubkey) -> anyhow::Result<()>;

    async fn subscribe_program_accounts(
        &self,
        program_id: Pubkey,
        filters: Option<Vec<AccountFilterType>>,
    ) -> anyhow::Result<()>;

    async fn save_snapshot(
        &self,
        storage: Arc<dyn AccountStorageInterface>,
        account_filter: AccountFilter,
    ) -> anyhow::Result<()>;
}
