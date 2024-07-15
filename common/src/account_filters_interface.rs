use crate::{account_data::AccountData, account_filter::AccountFilter};

pub trait AccountFiltersStoreInterface: Send + Sync {
    fn satisfies(&self, account_data: &AccountData) -> bool;

    fn contains_filter(&self, filter: &AccountFilter) -> bool;
}
