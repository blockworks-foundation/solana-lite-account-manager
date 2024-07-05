use std::{
    collections::{BTreeMap, HashMap, VecDeque},
    sync::{
        atomic::{AtomicU32, AtomicU64, Ordering},
        Arc,
    },
};

use async_trait::async_trait;
use dashmap::DashMap;
use itertools::Itertools;
use lite_account_manager_common::{
    account_data::AccountData,
    account_filter::AccountFilterType,
    account_store_interface::{AccountLoadingError, AccountStorageInterface},
    commitment::Commitment,
    slot_info::SlotInfo,
};
use serde::{Deserialize, Serialize};
use solana_sdk::{clock::Slot, pubkey::Pubkey};
use tokio::sync::RwLock;

use crate::{
    account_types::{MintAccount, MintIndex, MultiSig, Program, TokenAccount, TokenAccountIndex},
    token_program_utils::{
        get_spl_token_owner_mint_filter, get_token_program_account_type,
        token_account_to_solana_account, token_mint_to_solana_account,
        token_multisig_to_solana_account, token_program_account_to_solana_account,
        TokenProgramAccountType,
    },
};

#[derive(Clone)]
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
                            log::debug!(
                                "updating an existing account {account_pk:?} from {} to {}",
                                acc.write_version,
                                write_version
                            );
                            acc.write_version = write_version;
                            acc.processed_account = processed_account;
                            true
                        } else {
                            false
                        }
                    }
                    None => {
                        log::debug!("Adding a new account {account_pk:?} to slot {slot}");
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
                log::debug!("Adding a new slot {slot} with new account {account_pk:?}");
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
        log::debug!(
            "update slot info slot : {}, parent: {}, root: {}",
            slot_info.slot,
            slot_info.parent,
            slot_info.root
        );
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
        slot_info: SlotInfo,
        mints_by_index: &Arc<DashMap<MintIndex, MintAccount>>,
    ) -> Vec<AccountData> {
        let lk = self.processed_slot_accounts.read().await;
        match lk.get(&slot_info.slot) {
            Some(acc) => acc
                .processed_accounts
                .iter()
                .filter_map(|(_, account)| {
                    token_program_account_to_solana_account(
                        &account.processed_account,
                        slot_info.slot,
                        account.write_version,
                        mints_by_index,
                    )
                })
                .collect(),
            None => {
                log::error!("confirmed slot not found in cache : {}", slot_info.slot);
                drop(lk);
                self.update_slot_info(slot_info).await;
                vec![]
            }
        }
    }

    // returns list of accounts that are finalized
    pub async fn finalize(&self, slot: u64) -> Vec<(ProcessedAccount, u64)> {
        log::debug!("finalizing slot : {slot}");
        let mut map_of_accounts: HashMap<Pubkey, (ProcessedAccount, u64)> = HashMap::new();
        let mut lk = self.processed_slot_accounts.write().await;
        let mut process_slot = Some(slot);
        while let Some(current_slot) = process_slot {
            match lk.entry(current_slot) {
                std::collections::btree_map::Entry::Occupied(mut occ) => {
                    let value = occ.get_mut();
                    for (pk, acc) in value.processed_accounts.drain() {
                        // as we are going from most recent slot to least recent slot
                        // if the key already exists then we do not insert in the map
                        match map_of_accounts.entry(pk) {
                            std::collections::hash_map::Entry::Vacant(vac) => {
                                vac.insert((acc, current_slot));
                            }
                            std::collections::hash_map::Entry::Occupied(_) => {
                                // already present
                                continue;
                            }
                        }
                    }
                    process_slot = value.slot_parent;
                    occ.remove();
                }
                std::collections::btree_map::Entry::Vacant(_) => {
                    break;
                }
            }
        }

        // remove processed nodes less than finalized slot
        while let Some(first_entry) = lk.first_entry() {
            if *first_entry.key() <= slot {
                first_entry.remove();
            } else {
                break;
            }
        }
        let mut return_vec = Vec::with_capacity(map_of_accounts.len());
        for (_, acc) in map_of_accounts.drain() {
            return_vec.push(acc);
        }
        return_vec
    }

    pub async fn get_confirmed_accounts(
        &self,
        account_pks: Vec<Pubkey>,
        return_list: &mut [Option<(ProcessedAccount, u64)>],
        confirmed_slot: Slot,
    ) {
        let mut process_slot = Some(confirmed_slot);
        let lk = self.processed_slot_accounts.read().await;
        while let Some(current_slot) = process_slot {
            match lk.get(&current_slot) {
                Some(processed_accounts) => {
                    for (index, account_pk) in account_pks.iter().enumerate() {
                        if return_list.get(index).unwrap().is_some() {
                            continue;
                        }
                        if let Some(acc) = processed_accounts.processed_accounts.get(account_pk) {
                            *return_list.get_mut(index).unwrap() =
                                Some((acc.clone(), current_slot));
                        }
                    }
                    if return_list.iter().all(|x| x.is_some()) {
                        break;
                    }
                    process_slot = processed_accounts.slot_parent;
                }
                None => {
                    break;
                }
            }
        }
    }

    pub async fn get_accounts(
        &self,
        account_pks: Vec<Pubkey>,
        commitment: Commitment,
        confirmed_slot: Slot,
    ) -> Vec<Option<(ProcessedAccount, u64)>> {
        let mut return_list = vec![None; account_pks.len()];
        match commitment {
            Commitment::Processed => {
                let lk = self.processed_slot_accounts.read().await;
                // iterate backwards on all the processed slots until we reach confirmed slot
                for (slot, processed_account_slots) in lk.iter().rev() {
                    log::debug!("getting processed account at slot : {}", slot);
                    if *slot > confirmed_slot {
                        for (index, account_pk) in account_pks.iter().enumerate() {
                            if return_list.get(index).unwrap().is_some() {
                                continue;
                            }
                            if let Some(processed_account) =
                                processed_account_slots.processed_accounts.get(account_pk)
                            {
                                *return_list.get_mut(index).unwrap() =
                                    Some((processed_account.clone(), *slot));
                            }
                        }

                        if return_list.iter().all(|x| x.is_some()) {
                            break;
                        }
                    } else {
                        drop(lk);
                        self.get_confirmed_accounts(account_pks, &mut return_list, confirmed_slot)
                            .await;
                        return return_list;
                    }
                }
            }
            Commitment::Confirmed => {
                self.get_confirmed_accounts(account_pks, &mut return_list, confirmed_slot)
                    .await
            }
            Commitment::Finalized => unreachable!(),
        };
        return_list
    }
}

pub struct InMemoryTokenStorage {
    mints_by_index: Arc<DashMap<MintIndex, MintAccount>>,
    mints_index_by_pubkey: Arc<DashMap<Pubkey, MintIndex>>,
    multisigs: Arc<DashMap<Pubkey, MultiSig>>,
    accounts_index_by_mint: Arc<DashMap<MintIndex, VecDeque<TokenAccountIndex>>>,
    account_index_by_pubkey: Arc<DashMap<Pubkey, TokenAccountIndex>>,
    account_by_owner_pubkey: Arc<DashMap<Pubkey, Vec<TokenAccountIndex>>>,
    token_accounts: Arc<RwLock<VecDeque<TokenAccount>>>,
    processed_storage: ProcessedAccountStore,
    confirmed_slot: Arc<AtomicU64>,
    finalized_slot: Arc<AtomicU64>,
    mint_counter: Arc<AtomicU32>,
}

impl InMemoryTokenStorage {
    #[allow(clippy::new_without_default)]
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
            finalized_slot: Arc::new(AtomicU64::new(0)),
            mint_counter: Arc::new(AtomicU32::new(0)),
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
                        let token_index = write_lk.len() as TokenAccountIndex;
                        write_lk.push_back(token_account);
                        v.insert(token_index as TokenAccountIndex);
                        drop(write_lk);

                        // add account index in accounts_index_by_mint map
                        match self.accounts_index_by_mint.entry(mint_index) {
                            dashmap::mapref::entry::Entry::Occupied(mut occ) => {
                                occ.get_mut().push_back(token_index);
                            }
                            dashmap::mapref::entry::Entry::Vacant(v) => {
                                let mut q = VecDeque::new();
                                q.push_back(token_index);
                                v.insert(q);
                            }
                        }

                        // add account index in account_by_owner_pubkey map
                        match self.account_by_owner_pubkey.entry(token_account_owner) {
                            dashmap::mapref::entry::Entry::Occupied(mut occ) => {
                                occ.get_mut().push(token_index);
                            }
                            dashmap::mapref::entry::Entry::Vacant(v) => {
                                v.insert(vec![token_index]);
                            }
                        }
                    }
                };
            }
            TokenProgramAccountType::Mint(mint_data) => {
                match self.mints_index_by_pubkey.entry(mint_data.pubkey) {
                    dashmap::mapref::entry::Entry::Occupied(occ) => {
                        self.mints_by_index.insert(*occ.get(), mint_data);
                    }
                    dashmap::mapref::entry::Entry::Vacant(v) => {
                        let index = self
                            .mint_counter
                            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                        self.mints_by_index.insert(index, mint_data);
                        v.insert(index);
                    }
                }
            }
            TokenProgramAccountType::MultiSig(multisig_data, pubkey) => {
                self.multisigs.insert(pubkey, multisig_data);
            }
            TokenProgramAccountType::Deleted(account_pk) => {
                if let Some((_, index)) = self.mints_index_by_pubkey.remove(&account_pk) {
                    self.mints_by_index.remove(&index);
                    self.accounts_index_by_mint.remove(&index);
                } else if self.mints_index_by_pubkey.remove(&account_pk).is_some() {
                    // do nothing multisig account is removed
                } else if let Some((_, index)) = self.account_index_by_pubkey.remove(&account_pk) {
                    let mut lk = self.token_accounts.write().await;
                    if let Some(token_account) = lk.get_mut(index as usize) {
                        token_account.lamports = 0;
                        token_account.mint = 0;
                        token_account.owner = Pubkey::default();
                    }
                }
            }
        }
        true
    }

    pub fn is_token_program_account(&self, account_data: &AccountData) -> bool {
        self.account_index_by_pubkey
            .contains_key(&account_data.pubkey)
            || self
                .mints_index_by_pubkey
                .contains_key(&account_data.pubkey)
            || self.multisigs.contains_key(&account_data.pubkey)
            || account_data.account.owner == spl_token::id()
            || account_data.account.owner == spl_token_2022::id()
    }

    pub async fn get_account_finalized(&self, account_pk: Pubkey) -> Option<AccountData> {
        if let Some(multisig) = self.multisigs.get(&account_pk) {
            return Some(token_multisig_to_solana_account(
                &multisig,
                account_pk,
                self.finalized_slot.load(Ordering::Relaxed),
                0,
            ));
        }

        if let Some(mint_index) = self.mints_index_by_pubkey.get(&account_pk) {
            let mint_data = self.mints_by_index.get(&mint_index).unwrap();
            return Some(token_mint_to_solana_account(
                &mint_data,
                self.finalized_slot.load(Ordering::Relaxed),
                0,
            ));
        }

        if let Some(ite) = self.account_index_by_pubkey.get(&account_pk) {
            let token_account_index = *ite as usize;
            let lk = self.token_accounts.read().await;
            let token_account = lk.get(token_account_index).unwrap();
            if token_account.lamports == 0 {
                return None;
            }
            return token_account_to_solana_account(
                token_account,
                self.finalized_slot.load(Ordering::Relaxed),
                0,
                &self.mints_by_index,
            );
        }
        None
    }

    pub async fn get_program_accounts_finalized(
        &self,
        program_pubkey: Pubkey,
        account_filters: Option<Vec<AccountFilterType>>,
    ) -> Result<Vec<AccountData>, AccountLoadingError> {
        let finalized_slot = self.finalized_slot.load(Ordering::Relaxed);

        if let Some(account_filters) = account_filters {
            let (owner, mint) = get_spl_token_owner_mint_filter(&program_pubkey, &account_filters);
            if let Some(owner) = owner {
                match self.account_by_owner_pubkey.get(&owner) {
                    Some(token_acc_indexes) => {
                        let lk = self.token_accounts.read().await;
                        let indexes = token_acc_indexes.value();
                        let mint =
                            mint.map(|pk| *self.mints_index_by_pubkey.get(&pk).unwrap().value());
                        let token_accounts = indexes
                            .iter()
                            .filter_map(|index| lk.get(*index as usize))
                            .filter(|acc| {
                                // filter by mint if necessary
                                if let Some(mint) = mint {
                                    acc.mint == mint
                                } else {
                                    true
                                }
                            })
                            .filter_map(|token_account| {
                                token_account_to_solana_account(
                                    token_account,
                                    finalized_slot,
                                    0,
                                    &self.mints_by_index,
                                )
                            })
                            .collect_vec();
                        Ok(token_accounts)
                    }
                    None => Ok(vec![]),
                }
            } else if let Some(mint) = mint {
                // token account filter
                match self.mints_index_by_pubkey.get(&mint) {
                    Some(mint_index) => match self.accounts_index_by_mint.get(mint_index.value()) {
                        Some(token_acc_indexes) => {
                            let indexes = token_acc_indexes.value();
                            let lk = self.token_accounts.read().await;
                            let token_accounts = indexes
                                .iter()
                                .filter_map(|index| lk.get(*index as usize))
                                .filter_map(|token_account| {
                                    token_account_to_solana_account(
                                        token_account,
                                        finalized_slot,
                                        0,
                                        &self.mints_by_index,
                                    )
                                })
                                .collect_vec();
                            Ok(token_accounts)
                        }
                        None => Ok(vec![]),
                    },
                    None => Ok(vec![]),
                }
            } else if account_filters.contains(&AccountFilterType::Datasize(82)) {
                // filtering by mint
                let mints = self
                    .mints_by_index
                    .iter()
                    .map(|mint_account| {
                        token_mint_to_solana_account(mint_account.value(), finalized_slot, 0)
                    })
                    .filter(|account| {
                        account_filters
                            .iter()
                            .all(|filter| filter.allows(&account.account.data.data()))
                    })
                    .collect_vec();
                Ok(mints)
            } else {
                Err(AccountLoadingError::ShouldContainAnAccountFilter)
            }
        } else {
            Err(AccountLoadingError::ShouldContainAnAccountFilter)
        }
    }
}

#[derive(Serialize, Deserialize)]
struct TokenProgramSnapshot {
    pub mints: Vec<(MintIndex, MintAccount)>,
    pub multisigs: Vec<MultiSig>,
    pub token_accounts: Vec<TokenAccount>,
}

#[async_trait]
impl AccountStorageInterface for InMemoryTokenStorage {
    async fn update_account(&self, account_data: AccountData, commitment: Commitment) -> bool {
        if !self.is_token_program_account(&account_data) {
            return false;
        }

        log::debug!(
            "update account : {} for commitment : {}",
            account_data.pubkey.to_string(),
            commitment
        );
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

        log::debug!("initializing account : {}", account_data.pubkey.to_string());
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
        log::debug!("get account : {}", account_pk.to_string());
        match commitment {
            Commitment::Confirmed | Commitment::Processed => {
                match self
                    .processed_storage
                    .get_accounts(
                        vec![account_pk],
                        commitment,
                        self.confirmed_slot.load(Ordering::Relaxed),
                    )
                    .await
                    .get(0)
                    .unwrap()
                {
                    Some((processed_account, slot)) => Ok(token_program_account_to_solana_account(
                        &processed_account.processed_account,
                        *slot,
                        0,
                        &self.mints_by_index,
                    )),
                    None => Ok(self.get_account_finalized(account_pk).await),
                }
            }
            Commitment::Finalized => Ok(self.get_account_finalized(account_pk).await),
        }
    }

    async fn get_program_accounts(
        &self,
        program_pubkey: Pubkey,
        account_filters: Option<Vec<AccountFilterType>>,
        commitment: Commitment,
    ) -> Result<Vec<AccountData>, AccountLoadingError> {
        if program_pubkey != spl_token::id() && program_pubkey != spl_token_2022::id() {
            return Err(AccountLoadingError::WrongIndex);
        }
        // get all accounts for finalized commitment and update them if necessary
        let mut account_data = self
            .get_program_accounts_finalized(program_pubkey, account_filters)
            .await?;
        let confirmed_slot = self.confirmed_slot.load(Ordering::Relaxed);
        if commitment == Commitment::Processed || commitment == Commitment::Confirmed {
            // get list of all pks to search
            let pks = account_data.iter().map(|x| x.pubkey).collect_vec();
            let processed_accounts = self
                .processed_storage
                .get_accounts(pks, commitment, confirmed_slot)
                .await;
            let mut to_remove = vec![];
            for (index, processed_account) in processed_accounts.iter().enumerate() {
                if let Some((processed_account, slot)) = processed_account {
                    let updated_account = token_program_account_to_solana_account(
                        &processed_account.processed_account,
                        *slot,
                        processed_account.write_version,
                        &self.mints_by_index,
                    );
                    match updated_account {
                        Some(updated_account) => {
                            // update with processed or confirmed account state
                            *account_data.get_mut(index).unwrap() = updated_account;
                        }
                        None => {
                            // account must have been deleted
                            to_remove.push(index);
                        }
                    }
                }
            }

            for index in to_remove.iter().rev() {
                account_data.remove(*index);
            }
        }
        Ok(account_data)
    }

    async fn process_slot_data(
        &self,
        slot_info: SlotInfo,
        commitment: Commitment,
    ) -> Vec<AccountData> {
        match commitment {
            Commitment::Processed => {
                log::debug!("process slot data  : {slot_info:?}");
                self.processed_storage.update_slot_info(slot_info).await;
                vec![]
            }
            Commitment::Confirmed => {
                log::debug!("confirm slot data  : {slot_info:?}");
                if self.confirmed_slot.load(Ordering::Relaxed) < slot_info.slot {
                    self.confirmed_slot.store(slot_info.slot, Ordering::Relaxed);
                    self.processed_storage.update_slot_info(slot_info).await;
                }
                self.processed_storage
                    .get_processed_for_slot(slot_info, &self.mints_by_index)
                    .await
            }
            Commitment::Finalized => {
                log::debug!("finalize slot data  : {slot_info:?}");
                if self.finalized_slot.load(Ordering::Relaxed) < slot_info.slot {
                    self.processed_storage.update_slot_info(slot_info).await;
                    self.finalized_slot.store(slot_info.slot, Ordering::Relaxed);
                }

                // move data from processed storage to main storage
                let finalized_accounts = self.processed_storage.finalize(slot_info.slot).await;

                let accounts_notifications = finalized_accounts
                    .iter()
                    .filter_map(|(acc, slot)| {
                        token_program_account_to_solana_account(
                            &acc.processed_account,
                            *slot,
                            acc.write_version,
                            &self.mints_by_index,
                        )
                    })
                    .collect_vec();
                for (finalized_account, _) in finalized_accounts {
                    self.add_account_finalized(finalized_account.processed_account)
                        .await;
                }
                accounts_notifications
            }
        }
    }

    async fn create_snapshot(&self, program_id: Pubkey) -> Result<Vec<u8>, AccountLoadingError> {
        let program = if program_id == spl_token::id() {
            Program::TokenProgram
        } else if program_id == spl_token_2022::id() {
            Program::Token2022Program
        } else {
            return Err(AccountLoadingError::WrongIndex);
        };

        let mints = self
            .mints_by_index
            .iter()
            .filter_map(|mint_data| {
                if mint_data.program == program {
                    Some((*mint_data.key(), mint_data.clone()))
                } else {
                    None
                }
            })
            .collect_vec();

        let multisigs = self.multisigs.iter().map(|ite| ite.clone()).collect_vec();
        let token_accounts = self
            .token_accounts
            .read()
            .await
            .iter()
            .filter(|x| x.program == program && x.lamports > 0)
            .cloned()
            .collect_vec();

        let snapshot = TokenProgramSnapshot {
            mints,
            multisigs,
            token_accounts,
        };
        let binary = bincode::serialize(&snapshot).unwrap();
        drop(snapshot);
        Ok(lz4::block::compress(
            &binary,
            Some(lz4::block::CompressionMode::HIGHCOMPRESSION(8)),
            false,
        )
        .map_err(|_| AccountLoadingError::CompressionIssues)?)
    }

    async fn load_from_snapshot(
        &self,
        _program_id: Pubkey,
        snapshot: Vec<u8>,
    ) -> Result<(), AccountLoadingError> {
        let decompress = lz4::block::decompress(&snapshot, None)
            .map_err(|_| AccountLoadingError::CompressionIssues)?;
        drop(snapshot);
        let snapshot = bincode::deserialize::<TokenProgramSnapshot>(&decompress)
            .map_err(|_| AccountLoadingError::DeserializationIssues)?;
        let TokenProgramSnapshot {
            mints,
            multisigs,
            token_accounts,
        } = snapshot;

        for multisig in multisigs {
            self.multisigs.insert(multisig.pubkey, multisig);
        }

        let mut mint_mapping = HashMap::<MintIndex, MintIndex>::new();
        // remap all the mints
        for (index, mint_account) in mints {
            match self.mints_index_by_pubkey.get_mut(&mint_account.pubkey) {
                Some(value) => {
                    mint_mapping.insert(index, *value);
                    self.mints_by_index.insert(index, mint_account);
                }
                None => {
                    let mint_index = self.mint_counter.fetch_add(1, Ordering::Relaxed);
                    mint_mapping.insert(index, mint_index);
                    self.mints_by_index.insert(mint_index, mint_account);
                }
            }
        }

        let mut lk = self.token_accounts.write().await;
        for mut token_account in token_accounts {
            // use the new mint index
            let new_mint_index = *mint_mapping.get(&token_account.mint).unwrap();
            token_account.mint = new_mint_index;
            match self.account_index_by_pubkey.get(&token_account.pubkey) {
                Some(exists) => {
                    // pubkey is already present, replace existing
                    let index = *exists as usize;
                    *lk.get_mut(index).unwrap() = token_account;
                }
                None => {
                    let index = lk.len() as MintIndex;
                    let pk = token_account.pubkey;
                    let owner = token_account.owner;
                    lk.push_back(token_account);
                    self.account_index_by_pubkey.insert(pk, index);
                    match self.account_by_owner_pubkey.get_mut(&owner) {
                        Some(mut accs) => accs.push(index),
                        None => {
                            self.account_by_owner_pubkey.insert(owner, vec![index]);
                        }
                    }

                    match self.accounts_index_by_mint.get_mut(&new_mint_index) {
                        Some(mut accs) => {
                            accs.push_back(index);
                        }
                        None => {
                            let mut q = VecDeque::new();
                            q.push_back(index);
                            self.accounts_index_by_mint.insert(new_mint_index, q);
                        }
                    }
                }
            }
        }

        Ok(())
    }
}
