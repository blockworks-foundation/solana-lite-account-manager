use crate::{
    account_filter::{AccountFilterType, AccountFilters},
    account_store_interface::AccountStorageInterface,
};
use solana_sdk::pubkey::Pubkey;
use std::{collections::HashSet, sync::Arc};

// Source to c
pub trait AccountsSourceInterface: Send + Sync {
    fn subscribe_accounts(&self, account: HashSet<Pubkey>) -> anyhow::Result<()>;

    fn subscribe_program_accounts(
        &self,
        program_id: Pubkey,
        filters: Option<Vec<AccountFilterType>>,
    ) -> anyhow::Result<()>;

    fn save_snapshot(
        &self,
        storage: Arc<dyn AccountStorageInterface>,
        account_filters: AccountFilters,
    ) -> anyhow::Result<()>;
}
