use async_trait::async_trait;

use crate::{account_data::AccountData, account_filter::AccountFilter};

#[async_trait]
pub trait AccountFiltersStoreInterface: Send + Sync {
    async fn satisfies(&self, account_data: &AccountData) -> bool;

    async fn contains_filter(&self, filter: &AccountFilter) -> bool;
}
