use crate::{
    account_data::AccountData,
    account_filter::{AccountFilter, AccountFilters},
    account_filters_interface::AccountFiltersStoreInterface,
    simple_filter_store::SimpleFilterStore,
};
use solana_sdk::pubkey::Pubkey;
use std::sync::{Arc, RwLock};

#[derive(Default)]
pub struct MutableFilterStore {
    filter_store: Arc<RwLock<SimpleFilterStore>>,
}

impl MutableFilterStore {
    pub fn add_account_filters(&self, account_filters: &AccountFilters) {
        let mut lk = self.filter_store.write().unwrap();
        lk.add_account_filters(account_filters)
    }

    pub fn contains_account(&self, account_pk: Pubkey) -> bool {
        let lk = self.filter_store.read().unwrap();
        lk.contains_account(account_pk)
    }

    pub fn satisfies_filter(&self, account: &AccountData) -> bool {
        let lk = self.filter_store.read().unwrap();
        lk.satisfies_filter(account)
    }
}

impl AccountFiltersStoreInterface for MutableFilterStore {
    fn satisfies(&self, account_data: &AccountData) -> bool {
        self.satisfies_filter(account_data)
    }

    fn contains_filter(&self, filter: &AccountFilter) -> bool {
        let lk = self.filter_store.read().unwrap();
        lk.contains_filter(filter)
    }
}
