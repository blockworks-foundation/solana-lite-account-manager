use async_trait::async_trait;

use crate::account_data::AccountData;

#[async_trait]
pub trait AccountFiltersStoreInterface: Send + Sync {
    async fn satisfies(&self, account_data: &AccountData) -> bool;
}
