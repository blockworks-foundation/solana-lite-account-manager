use std::{
    collections::{BTreeMap, VecDeque},
    sync::{atomic::AtomicU64, Arc},
};

use async_trait::async_trait;
use dashmap::DashMap;
use lite_account_manager_common::{
    account_data::AccountData,
    account_filter::AccountFilterType,
    account_store_interface::{AccountLoadingError, AccountStorageInterface},
    commitment::Commitment,
};
use solana_sdk::{clock::Slot, pubkey::Pubkey};
use tokio::sync::RwLock;

use crate::{
    account_types::{MintAccount, MultiSig, TokenAccount},
    token_program_utils::TokenProgramAccountType,
};

#[derive(Debug, Clone)]
struct MintData {
    pub index: u64,
    pub mint_account: Option<MintAccount>,
}

struct ProcessedTokenAccount {
    pub token_account: TokenAccount,
    pub write_version: u64,
}

pub struct InMemoryTokenStorage {
    mints: Arc<DashMap<Pubkey, MintData>>,
    multisigs: Arc<DashMap<Pubkey, MultiSig>>,
    accounts_index_by_mint: Arc<DashMap<u64, Vec<u64>>>,
    account_index_by_pubkey: Arc<DashMap<Pubkey, u64>>,
    account_by_owner_pubkey: Arc<DashMap<Pubkey, Vec<u64>>>,
    token_accounts: Arc<RwLock<VecDeque<TokenAccount>>>,
    processed_slot_accounts: Arc<RwLock<BTreeMap<u64, ProcessedTokenAccount>>>,
    confirmed_slot: Arc<AtomicU64>,
    mint_counter: Arc<AtomicU64>,
}

impl InMemoryTokenStorage {
    pub fn new() -> Self {
        Self {
            mints: Arc::new(DashMap::new()),
            accounts_index_by_mint: Arc::new(DashMap::new()),
            multisigs: Arc::new(DashMap::new()),
            account_index_by_pubkey: Arc::new(DashMap::new()),
            account_by_owner_pubkey: Arc::new(DashMap::new()),
            token_accounts: Arc::new(RwLock::new(VecDeque::new())),
            processed_slot_accounts: Arc::new(RwLock::new(BTreeMap::new())),
            confirmed_slot: Arc::new(AtomicU64::new(0)),
            mint_counter: Arc::new(AtomicU64::new(0)),
        }
    }

    // treat account for finalized commitment
    pub async fn add_account_finalized(&self, account: TokenProgramAccountType) {
        match account {
            TokenProgramAccountType::TokenAccount(mut token_account, mint) => {
                let mint_index = match self.mints.entry(mint) {
                    dashmap::mapref::entry::Entry::Occupied(occ) => occ.get().index,
                    dashmap::mapref::entry::Entry::Vacant(v) => {
                        let index = self
                            .mint_counter
                            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                        v.insert(MintData {
                            index,
                            mint_account: None,
                        });
                        index
                    }
                };
                token_account.mint = mint_index;
                let token_account_owner = token_account.owner;

                // find if the token account is already present
                match self.account_index_by_pubkey.entry(token_account.pubkey) {
                    dashmap::mapref::entry::Entry::Occupied(occ) => {
                        // already present
                        let token_index = *occ.get() as usize;
                        let mut write_lk = self.token_accounts.write().await;
                        // update existing token account
                        *write_lk.get_mut(token_index).unwrap() = token_account;
                    }
                    dashmap::mapref::entry::Entry::Vacant(v) => {
                        // add new token account
                        let mut write_lk = self.token_accounts.write().await;
                        let token_index = write_lk.len();
                        write_lk.push_back(token_account);
                        v.insert(token_index as u64);
                        drop(write_lk);

                        // add account index in accounts_index_by_mint map
                        match self.accounts_index_by_mint.entry(mint_index) {
                            dashmap::mapref::entry::Entry::Occupied(mut occ) => {
                                occ.get_mut().push(token_index as u64);
                            }
                            dashmap::mapref::entry::Entry::Vacant(v) => {
                                v.insert(vec![token_index as u64]);
                            }
                        }

                        // add account index in account_by_owner_pubkey map
                        match self.account_by_owner_pubkey.entry(token_account_owner) {
                            dashmap::mapref::entry::Entry::Occupied(mut occ) => {
                                occ.get_mut().push(token_index as u64);
                            }
                            dashmap::mapref::entry::Entry::Vacant(v) => {
                                v.insert(vec![token_index as u64]);
                            }
                        }
                    }
                };
            }
            TokenProgramAccountType::Mint(mint_data) => match self.mints.entry(mint_data.pubkey) {
                dashmap::mapref::entry::Entry::Occupied(mut occ) => {
                    occ.get_mut().mint_account = Some(mint_data);
                }
                dashmap::mapref::entry::Entry::Vacant(v) => {
                    let index = self
                        .mint_counter
                        .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                    v.insert(MintData {
                        index,
                        mint_account: Some(mint_data),
                    });
                }
            },
            TokenProgramAccountType::MultiSig(multisig_data, pubkey) => {
                self.multisigs.insert(pubkey, multisig_data);
            }
        }
    }
}

#[async_trait]
impl AccountStorageInterface for InMemoryTokenStorage {
    async fn update_account(&self, account_data: AccountData, commitment: Commitment) -> bool {
        todo!()
    }

    async fn initilize_or_update_account(&self, account_data: AccountData) {
        todo!()
    }

    async fn get_account(
        &self,
        account_pk: Pubkey,
        commitment: Commitment,
    ) -> Result<Option<AccountData>, AccountLoadingError> {
        todo!()
    }

    async fn get_program_accounts(
        &self,
        program_pubkey: Pubkey,
        account_filter: Option<Vec<AccountFilterType>>,
        commitment: Commitment,
    ) -> Result<Vec<AccountData>, AccountLoadingError> {
        todo!()
    }

    async fn process_slot_data(&self, slot: Slot, commitment: Commitment) -> Vec<AccountData> {
        todo!()
    }
}
