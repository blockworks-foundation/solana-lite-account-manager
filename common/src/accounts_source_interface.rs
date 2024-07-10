use crate::{
    account_filter::{AccountFilterType, AccountFilters},
    account_store_interface::AccountStorageInterface,
};
use async_trait::async_trait;
use solana_sdk::pubkey::Pubkey;
use std::{collections::HashSet, sync::Arc};

// Source to create snapshot and subscribe to new accounts
#[async_trait]
pub trait AccountsSourceInterface: Send + Sync {
    async fn subscribe_accounts(&self, account: HashSet<Pubkey>) -> anyhow::Result<()>;

    async fn subscribe_program_accounts(
        &self,
        program_id: Pubkey,
        filters: Option<Vec<AccountFilterType>>,
    ) -> anyhow::Result<()>;

    async fn save_snapshot(
        &self,
        storage: Arc<dyn AccountStorageInterface>,
        account_filters: AccountFilters,
    ) -> anyhow::Result<()>;
}
