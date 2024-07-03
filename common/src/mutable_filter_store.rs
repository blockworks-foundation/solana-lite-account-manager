use crate::{
    account_data::AccountData,
    account_filter::{AccountFilter, AccountFilters},
    account_filters_interface::AccountFiltersStoreInterface,
    simple_filter_store::SimpleFilterStore,
};
use async_trait::async_trait;
use solana_sdk::pubkey::Pubkey;
use std::sync::Arc;
use tokio::sync::RwLock;

#[derive(Default)]
pub struct MutableFilterStore {
    filter_store: Arc<RwLock<SimpleFilterStore>>,
}

impl MutableFilterStore {
    pub async fn add_account_filters(&self, account_filters: &AccountFilters) {
        let mut lk = self.filter_store.write().await;
        lk.add_account_filters(account_filters)
    }

    pub async fn contains_account(&self, account_pk: Pubkey) -> bool {
        let lk = self.filter_store.read().await;
        lk.contains_account(account_pk)
    }

    pub async fn satisfies_filter(&self, account: &AccountData) -> bool {
        let lk = self.filter_store.read().await;
        lk.satisfies_filter(account)
    }
}

#[async_trait]
impl AccountFiltersStoreInterface for MutableFilterStore {
    async fn satisfies(&self, account_data: &AccountData) -> bool {
        self.satisfies_filter(account_data).await
    }

    async fn contains_filter(&self, filter: &AccountFilter) -> bool {
        let lk = self.filter_store.read().await;
        lk.contains_filter(filter).await
    }
}
