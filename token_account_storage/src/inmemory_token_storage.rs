use std::{
    collections::{BTreeMap, HashMap, VecDeque},
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
};

use async_trait::async_trait;
use dashmap::DashMap;
use lite_account_manager_common::{
    account_data::AccountData,
    account_filter::AccountFilterType,
    account_store_interface::{AccountLoadingError, AccountStorageInterface},
    commitment::Commitment,
    slot_info::SlotInfo,
};
use solana_sdk::{clock::Slot, pubkey::Pubkey};
use tokio::sync::RwLock;

use crate::{
    account_types::{MintData, MultiSig, TokenAccount},
    token_program_utils::{
        get_token_program_account_type, token_program_account_to_solana_account,
        TokenProgramAccountType,
    },
};

struct ProcessedAccount {
    pub processed_account: TokenProgramAccountType,
    pub write_version: u64,
}

struct ProcessedAccountBySlot {
    pub processed_accounts: HashMap<Pubkey, ProcessedAccount>,
    pub slot_parent: Option<u64>,
}

#[derive(Default, Clone)]
struct ProcessedAccountStore {
    processed_slot_accounts: Arc<RwLock<BTreeMap<u64, ProcessedAccountBySlot>>>,
}

impl ProcessedAccountStore {
    pub async fn insert_account(
        &self,
        account_pk: Pubkey,
        processed_account: TokenProgramAccountType,
        write_version: u64,
        slot: Slot,
    ) -> bool {
        let mut lk = self.processed_slot_accounts.write().await;
        match lk.get_mut(&slot) {
            Some(processed_account_by_slot) => {
                match processed_account_by_slot
                    .processed_accounts
                    .get_mut(&account_pk)
                {
                    Some(acc) => {
                        if acc.write_version < write_version {
                            acc.write_version = write_version;
                            acc.processed_account = processed_account;
                            true
                        } else {
                            false
                        }
                    }
                    None => {
                        processed_account_by_slot.processed_accounts.insert(
                            account_pk,
                            ProcessedAccount {
                                processed_account,
                                write_version,
                            },
                        );
                        true
                    }
                }
            }
            None => {
                let mut processed_accounts = HashMap::new();
                processed_accounts.insert(
                    account_pk,
                    ProcessedAccount {
                        processed_account,
                        write_version,
                    },
                );
                lk.insert(
                    slot,
                    ProcessedAccountBySlot {
                        processed_accounts,
                        slot_parent: None,
                    },
                );
                true
            }
        }
    }

    pub async fn update_slot_info(&self, slot_info: SlotInfo) {
        let mut lk = self.processed_slot_accounts.write().await;
        match lk.get_mut(&slot_info.slot) {
            Some(processed_account_by_slot) => {
                processed_account_by_slot.slot_parent = Some(slot_info.parent);
            }
            None => {
                lk.insert(
                    slot_info.slot,
                    ProcessedAccountBySlot {
                        processed_accounts: HashMap::new(),
                        slot_parent: None,
                    },
                );
            }
        }
    }

    pub async fn get_processed_for_slot(
        &self,
        slot: u64,
        mints_by_index: &Arc<DashMap<u64, MintData>>,
    ) -> Vec<AccountData> {
        let lk = self.processed_slot_accounts.read().await;
        match lk.get(&slot) {
            Some(acc) => acc
                .processed_accounts
                .iter()
                .map(|(_, account)| {
                    token_program_account_to_solana_account(
                        &account.processed_account,
                        slot,
                        account.write_version,
                        mints_by_index,
                    )
                })
                .collect(),
            None => {
                log::error!("confirmed slot not found in cache : {}", slot);
                vec![]
            }
        }
    }
}

pub struct InMemoryTokenStorage {
    mints_by_index: Arc<DashMap<u64, MintData>>,
    mints_index_by_pubkey: Arc<DashMap<Pubkey, u64>>,
    multisigs: Arc<DashMap<Pubkey, MultiSig>>,
    accounts_index_by_mint: Arc<DashMap<u64, Vec<u64>>>,
    account_index_by_pubkey: Arc<DashMap<Pubkey, u64>>,
    account_by_owner_pubkey: Arc<DashMap<Pubkey, Vec<u64>>>,
    token_accounts: Arc<RwLock<VecDeque<TokenAccount>>>,
    processed_storage: ProcessedAccountStore,
    confirmed_slot: Arc<AtomicU64>,
    mint_counter: Arc<AtomicU64>,
}

impl InMemoryTokenStorage {
    pub fn new() -> Self {
        Self {
            mints_by_index: Arc::new(DashMap::new()),
            mints_index_by_pubkey: Arc::new(DashMap::new()),
            accounts_index_by_mint: Arc::new(DashMap::new()),
            multisigs: Arc::new(DashMap::new()),
            account_index_by_pubkey: Arc::new(DashMap::new()),
            account_by_owner_pubkey: Arc::new(DashMap::new()),
            token_accounts: Arc::new(RwLock::new(VecDeque::new())),
            processed_storage: ProcessedAccountStore::default(),
            confirmed_slot: Arc::new(AtomicU64::new(0)),
            mint_counter: Arc::new(AtomicU64::new(0)),
        }
    }

    // treat account for finalized commitment
    pub async fn add_account_finalized(&self, account: TokenProgramAccountType) -> bool {
        match account {
            TokenProgramAccountType::TokenAccount(token_account) => {
                let token_account_owner = token_account.owner;
                let mint_index = token_account.mint;
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
            TokenProgramAccountType::Mint(mint_data) => {
                match self.mints_index_by_pubkey.entry(mint_data.pubkey) {
                    dashmap::mapref::entry::Entry::Occupied(occ) => {
                        self.mints_by_index.get_mut(occ.get()).unwrap().mint_account =
                            Some(mint_data);
                    }
                    dashmap::mapref::entry::Entry::Vacant(v) => {
                        let index = self
                            .mint_counter
                            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                        self.mints_by_index.insert(
                            index,
                            MintData {
                                pubkey: mint_data.pubkey,
                                mint_account: Some(mint_data),
                            },
                        );
                        v.insert(index);
                    }
                }
            }
            TokenProgramAccountType::MultiSig(multisig_data, pubkey) => {
                self.multisigs.insert(pubkey, multisig_data);
            }
        }
        true
    }

    pub fn is_token_program_account(&self, account_data: &AccountData) -> bool {
        if self
            .account_index_by_pubkey
            .contains_key(&account_data.pubkey)
            || self
                .mints_index_by_pubkey
                .contains_key(&account_data.pubkey)
            || self.multisigs.contains_key(&account_data.pubkey)
            || account_data.account.owner == spl_token::id()
            || account_data.account.owner == spl_token_2022::id()
        {
            true
        } else {
            false
        }
    }
}

#[async_trait]
impl AccountStorageInterface for InMemoryTokenStorage {
    async fn update_account(&self, account_data: AccountData, commitment: Commitment) -> bool {
        if !self.is_token_program_account(&account_data) {
            return false;
        }
        let token_program_account = match get_token_program_account_type(
            &account_data,
            &self.mints_index_by_pubkey,
            &self.mint_counter,
        ) {
            Ok(res) => res,
            Err(e) => {
                log::error!("Token account was not able to identified {}", e);
                return false;
            }
        };
        if commitment == Commitment::Processed || commitment == Commitment::Confirmed {
            self.processed_storage
                .insert_account(
                    account_data.pubkey,
                    token_program_account,
                    account_data.write_version,
                    account_data.updated_slot,
                )
                .await
        } else {
            self.add_account_finalized(token_program_account).await
        }
    }

    async fn initilize_or_update_account(&self, account_data: AccountData) {
        if !self.is_token_program_account(&account_data) {
            return;
        }

        // add account for finalized commitment
        let token_program_account = match get_token_program_account_type(
            &account_data,
            &self.mints_index_by_pubkey,
            &self.mint_counter,
        ) {
            Ok(res) => res,
            Err(e) => {
                log::error!("Token account was not able to identified {}", e);
                return;
            }
        };
        self.add_account_finalized(token_program_account).await;
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

    async fn process_slot_data(
        &self,
        slot_info: SlotInfo,
        commitment: Commitment,
    ) -> Vec<AccountData> {
        match commitment {
            Commitment::Processed => {
                self.processed_storage.update_slot_info(slot_info).await;
                vec![]
            }
            Commitment::Confirmed => {
                self.confirmed_slot.store(slot_info.slot, Ordering::Relaxed);
                self.processed_storage
                    .get_processed_for_slot(slot_info.slot, &self.mints_by_index)
                    .await
            }
            Commitment::Finalized => {
                // move data from processed storage to main storage
                todo!()
            }
        }
    }
}
