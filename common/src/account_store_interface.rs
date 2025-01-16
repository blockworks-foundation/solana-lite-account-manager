use std::collections::HashMap;
use std::ops::{Deref, DerefMut};

use crate::account_data::AccountData;
use crate::account_filter::AccountFilterType;
use crate::commitment::Commitment;
use crate::slot_info::SlotInfo;
use solana_sdk::pubkey::Pubkey;

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum AccountLoadingError {
    AccountNotFound,
    ConfigDoesNotContainRequiredFilters,
    OperationTimeOut,
    ErrorCreatingSnapshot,
    ErrorSubscribingAccount,
    TokenAccountsSizeNotFound,
    TokenAccountsCannotUseThisFilter,
    WrongIndex,
    ShouldContainAnAccountFilter,
    DeserializationIssues,
    CompressionIssues,
}

impl std::fmt::Display for AccountLoadingError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "AccountLoadingError : {:?}", *self)
    }
}

impl std::error::Error for AccountLoadingError {}

pub trait AccountStorageInterface: Send + Sync {
    // Update account and return true if the account was successfully updated
    fn update_account(&self, account_data: AccountData, commitment: Commitment) -> bool;

    fn initialize_or_update_account(&self, account_data: AccountData);

    fn get_account(
        &self,
        account_pk: Pubkey,
        commitment: Commitment,
    ) -> Result<Option<AccountData>, AccountLoadingError>;

    fn get_program_accounts(
        &self,
        program_pubkey: Pubkey,
        account_filters: Option<Vec<AccountFilterType>>,
        commitment: Commitment,
    ) -> Result<Vec<AccountData>, AccountLoadingError>;

    fn process_slot_data(&self, slot_info: SlotInfo, commitment: Commitment) -> Vec<AccountData>;

    // snapshot should always be created at finalized slot
    fn create_snapshot(&self, program_id: Pubkey) -> Result<Vec<u8>, AccountLoadingError>;

    fn load_from_snapshot(
        &self,
        program_id: Pubkey,
        snapshot: Vec<u8>,
    ) -> Result<(), AccountLoadingError>;
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TokenProgramType {
    TokenProgram,
    Token2022Program,
}

impl AsRef<Pubkey> for TokenProgramType {
    fn as_ref(&self) -> &Pubkey {
        match self {
            TokenProgramType::TokenProgram => &spl_token::ID,
            TokenProgramType::Token2022Program => &spl_token_2022::ID,
        }
    }
}

impl TryFrom<&Pubkey> for TokenProgramType {
    type Error = ();

    fn try_from(value: &Pubkey) -> Result<Self, Self::Error> {
        if *value == spl_token::ID {
            Ok(TokenProgramType::TokenProgram)
        } else if *value == spl_token_2022::ID {
            Ok(TokenProgramType::Token2022Program)
        } else {
            Err(())
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum TokenProgramAccountData {
    TokenAccount(TokenProgramTokenAccountData),
    OtherAccount(AccountData),
}

#[derive(Debug, Clone, PartialEq)]
pub struct TokenProgramTokenAccountData {
    pub account_data: AccountData,
    pub mint_pubkey: Pubkey,
}

impl TokenProgramAccountData {
    pub fn into_inner(self) -> AccountData {
        match self {
            TokenProgramAccountData::TokenAccount(token_account) => token_account.account_data,
            TokenProgramAccountData::OtherAccount(account_data) => account_data,
        }
    }
}

impl Deref for TokenProgramAccountData {
    type Target = AccountData;

    fn deref(&self) -> &Self::Target {
        match self {
            TokenProgramAccountData::TokenAccount(token_account) => &token_account.account_data,
            TokenProgramAccountData::OtherAccount(account_data) => account_data,
        }
    }
}

impl DerefMut for TokenProgramAccountData {
    fn deref_mut(&mut self) -> &mut Self::Target {
        match self {
            TokenProgramAccountData::TokenAccount(token_account) => &mut token_account.account_data,
            TokenProgramAccountData::OtherAccount(account_data) => account_data,
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum Mint {
    TokenMint(spl_token::state::Mint),
    Token2022Mint(spl_token_2022::state::Mint),
}

pub trait ProgramAccountStorageInterface: Send + Sync {
    fn get_program_accounts_with_mints(
        &self,
        token_program_type: TokenProgramType,
        account_filters: Option<Vec<AccountFilterType>>,
        commitment: Commitment,
    ) -> Result<(Vec<TokenProgramAccountData>, HashMap<Pubkey, Mint>), AccountLoadingError>;

    fn get_account_with_mint(
        &self,
        account_pk: Pubkey,
        commitment: Commitment,
    ) -> Result<Option<(TokenProgramAccountData, Option<Mint>)>, AccountLoadingError>;
}
