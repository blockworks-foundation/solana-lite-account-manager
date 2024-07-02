use crate::account_data::AccountData;
use crate::account_filter::AccountFilterType;
use crate::commitment::Commitment;
use crate::slot_info::SlotInfo;
use async_trait::async_trait;
use solana_sdk::pubkey::Pubkey;

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum AccountLoadingError {
    AccountNotFound,
    ConfigDoesnotContainRequiredFilters,
    OperationTimeOut,
    ErrorCreatingSnapshot,
    ErrorSubscribingAccount,
    TokenAccountsSizeNotFound,
    TokenAccountsCannotUseThisFilter,
    WrongIndex,
    ShouldContainAnAccountFilter,
}

#[async_trait]
pub trait AccountStorageInterface: Send + Sync {
    // Update account and return true if the account was sucessfylly updated
    async fn update_account(&self, account_data: AccountData, commitment: Commitment) -> bool;

    async fn initilize_or_update_account(&self, account_data: AccountData);

    async fn get_account(
        &self,
        account_pk: Pubkey,
        commitment: Commitment,
    ) -> Result<Option<AccountData>, AccountLoadingError>;

    async fn get_program_accounts(
        &self,
        program_pubkey: Pubkey,
        account_filter: Option<Vec<AccountFilterType>>,
        commitment: Commitment,
    ) -> Result<Vec<AccountData>, AccountLoadingError>;

    async fn process_slot_data(&self, slot: SlotInfo, commitment: Commitment) -> Vec<AccountData>;
}
