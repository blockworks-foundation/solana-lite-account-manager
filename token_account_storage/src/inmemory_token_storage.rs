use std::{
    collections::{BTreeMap, HashMap, HashSet, VecDeque},
    sync::{
        atomic::{AtomicU32, AtomicU64, Ordering},
        Arc, RwLock,
    },
};

use dashmap::DashMap;
use itertools::Itertools;
use lite_account_manager_common::{
    account_data::AccountData,
    account_filter::AccountFilterType,
    account_store_interface::{
        AccountLoadingError, AccountStorageInterface, Mint, ProgramAccountStorageInterface,
        TokenProgramAccountData, TokenProgramTokenAccountData, TokenProgramType,
    },
    commitment::Commitment,
    pubkey_container_utils::PartialPubkey,
    slot_info::SlotInfo,
};
use prometheus::{opts, register_int_gauge, IntGauge};
use serde::{Deserialize, Serialize};
use solana_sdk::{clock::Slot, pubkey::Pubkey};

use crate::{
    account_types::{MintAccount, MintIndex, MultiSig, Program, TokenAccount, TokenAccountIndex},
    token_account_storage_interface::TokenAccountStorageInterface,
    token_program_utils::{
        get_token_program_account_filter, get_token_program_account_type,
        token_account_to_solana_account, token_mint_to_solana_account,
        token_mint_to_spl_token_mint, token_multisig_to_solana_account,
        token_program_account_to_solana_account, AccountFilter, TokenProgramAccountFilter,
        TokenProgramAccountType,
    },
};

lazy_static::lazy_static! {
    static ref TOKEN_PROGRAM_TOTAL_PROCESSED_ACCOUNTS: IntGauge =
        register_int_gauge!(opts!("lite_accounts_token_processed_accounts_in_memory", "Account processed accounts InMemory")).unwrap();

    static ref TOKEN_MINTS_IN_MEMORY: IntGauge =
        register_int_gauge!(opts!("lite_accounts_token_mints_in_memory", "Slot of latest account update")).unwrap();
}

const PARTIAL_PUBKEY_SIZE: usize = 4;
type InmemoryPubkey = PartialPubkey<PARTIAL_PUBKEY_SIZE>;
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
    pub fn insert_account(
        &self,
        account_pk: Pubkey,
        processed_account: TokenProgramAccountType,
        write_version: u64,
        slot: Slot,
    ) -> bool {
        let mut lk = self.processed_slot_accounts.write().unwrap();
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
                        TOKEN_PROGRAM_TOTAL_PROCESSED_ACCOUNTS.inc();
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
                TOKEN_PROGRAM_TOTAL_PROCESSED_ACCOUNTS.inc();
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

    pub fn update_slot_info(&self, slot_info: SlotInfo) {
        log::debug!(
            "update slot info slot : {}, parent: {}, root: {}",
            slot_info.slot,
            slot_info.parent,
            slot_info.root
        );
        let mut lk = self.processed_slot_accounts.write().unwrap();
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

    pub fn get_processed_for_slot(
        &self,
        slot_info: SlotInfo,
        mints_by_index: &Arc<DashMap<MintIndex, MintAccount>>,
    ) -> Vec<AccountData> {
        let lk = self.processed_slot_accounts.read().unwrap();
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
                self.update_slot_info(slot_info);
                vec![]
            }
        }
    }

    // returns list of accounts that are finalized
    pub fn finalize(&self, slot: u64) -> Vec<(ProcessedAccount, u64)> {
        log::debug!("finalizing slot : {slot}");
        let mut map_of_accounts: HashMap<Pubkey, (ProcessedAccount, u64)> = HashMap::new();
        let mut lk = self.processed_slot_accounts.write().unwrap();
        let mut process_slot = Some(slot);
        while let Some(current_slot) = process_slot {
            match lk.entry(current_slot) {
                std::collections::btree_map::Entry::Occupied(mut occ) => {
                    let value = occ.get_mut();
                    for (pk, acc) in value.processed_accounts.drain() {
                        TOKEN_PROGRAM_TOTAL_PROCESSED_ACCOUNTS.dec();
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

    pub fn get_confirmed_accounts(
        &self,
        account_pks: Vec<Pubkey>,
        return_list: &mut [Option<(ProcessedAccount, u64)>],
        confirmed_slot: Slot,
    ) {
        let mut process_slot = Some(confirmed_slot);
        let lk = self.processed_slot_accounts.read().unwrap();
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

    pub fn get_accounts(
        &self,
        account_pks: Vec<Pubkey>,
        commitment: Commitment,
        confirmed_slot: Slot,
    ) -> Vec<Option<(ProcessedAccount, u64)>> {
        let mut return_list = vec![None; account_pks.len()];
        match commitment {
            Commitment::Processed => {
                let lk = self.processed_slot_accounts.read().unwrap();
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
                        self.get_confirmed_accounts(account_pks, &mut return_list, confirmed_slot);
                        return return_list;
                    }
                }
            }
            Commitment::Confirmed => {
                self.get_confirmed_accounts(account_pks, &mut return_list, confirmed_slot)
            }
            Commitment::Finalized => {
                unreachable!("this function must not be called for a finalized commitment")
            }
        };
        return_list
    }
}

pub struct TokenProgramAccountsStorage {
    mints_by_index: Arc<DashMap<MintIndex, MintAccount>>,
    mints_index_by_pubkey: Arc<DashMap<Pubkey, MintIndex>>,
    multisigs: Arc<DashMap<Pubkey, MultiSig>>,
    accounts_index_by_mint: Arc<DashMap<MintIndex, VecDeque<TokenAccountIndex>>>,
    account_by_owner_pubkey: Arc<DashMap<InmemoryPubkey, Vec<TokenAccountIndex>>>,
    token_accounts_storage: Arc<dyn TokenAccountStorageInterface>,
    processed_storage: ProcessedAccountStore,
    confirmed_slot: Arc<AtomicU64>,
    finalized_slot: Arc<AtomicU64>,
    mint_counter: Arc<AtomicU32>,
}

impl TokenProgramAccountsStorage {
    #[allow(clippy::new_without_default)]
    pub fn new(token_accounts_storage: Arc<dyn TokenAccountStorageInterface>) -> Self {
        Self {
            mints_by_index: Arc::new(DashMap::new()),
            mints_index_by_pubkey: Arc::new(DashMap::new()),
            accounts_index_by_mint: Arc::new(DashMap::new()),
            multisigs: Arc::new(DashMap::new()),
            account_by_owner_pubkey: Arc::new(DashMap::new()),
            token_accounts_storage,
            processed_storage: ProcessedAccountStore::default(),
            confirmed_slot: Arc::new(AtomicU64::new(0)),
            finalized_slot: Arc::new(AtomicU64::new(0)),
            mint_counter: Arc::new(AtomicU32::new(0)),
        }
    }

    // treat account for finalized commitment
    pub fn add_account_finalized(&self, account: TokenProgramAccountType) -> bool {
        match account {
            TokenProgramAccountType::TokenAccount(token_account) => {
                let token_account_owner = token_account.owner;
                let mint_index = token_account.mint;
                let (token_index, is_added) =
                    self.token_accounts_storage.save_or_update(token_account);
                if is_added {
                    // add account to the indexes
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
                    match self
                        .account_by_owner_pubkey
                        .entry(token_account_owner.into())
                    {
                        dashmap::mapref::entry::Entry::Occupied(mut occ) => {
                            occ.get_mut().push(token_index);
                        }
                        dashmap::mapref::entry::Entry::Vacant(v) => {
                            v.insert(vec![token_index]);
                        }
                    }
                }
            }
            TokenProgramAccountType::Mint(mint_data) => {
                match self.mints_index_by_pubkey.entry(mint_data.pubkey) {
                    dashmap::mapref::entry::Entry::Occupied(occ) => {
                        self.mints_by_index.insert(*occ.get(), mint_data);
                    }
                    dashmap::mapref::entry::Entry::Vacant(v) => {
                        TOKEN_MINTS_IN_MEMORY.inc();
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
                    TOKEN_MINTS_IN_MEMORY.dec();
                    self.mints_by_index.remove(&index);
                    self.accounts_index_by_mint.remove(&index);
                } else if self.multisigs.remove(&account_pk).is_some() {
                    // do nothing multisig account is removed
                } else {
                    self.token_accounts_storage.delete(&account_pk);
                }
            }
        }
        true
    }

    pub fn is_token_program_account(&self, account_data: &AccountData) -> bool {
        self.mints_index_by_pubkey
            .contains_key(&account_data.pubkey)
            || self.multisigs.contains_key(&account_data.pubkey)
            || account_data.account.owner == spl_token::id()
            || account_data.account.owner == spl_token_2022::id()
            || self
                .token_accounts_storage
                .contains(&account_data.pubkey)
                .is_some()
    }

    pub fn get_account_finalized(
        &self,
        account_pk: Pubkey,
    ) -> Option<(TokenProgramAccountData, Option<MintAccount>)> {
        self.multisigs
            .get(&account_pk)
            .map(|multisig| {
                (
                    TokenProgramAccountData::OtherAccount(token_multisig_to_solana_account(
                        &multisig,
                        account_pk,
                        self.finalized_slot.load(Ordering::Relaxed),
                        0,
                    )),
                    None,
                )
            })
            .or_else(|| {
                self.mints_index_by_pubkey
                    .get(&account_pk)
                    .map(|mint_index| {
                        let mint_data = self.mints_by_index.get(&mint_index).expect(
                            "Mint for index must exist when a mint index by pubkey entry exists",
                        );
                        (
                            TokenProgramAccountData::OtherAccount(token_mint_to_solana_account(
                                &mint_data,
                                self.finalized_slot.load(Ordering::Relaxed),
                                0,
                            )),
                            None,
                        )
                    })
            })
            .or_else(|| {
                self.token_accounts_storage
                    .get_by_pubkey(&account_pk)
                    .and_then(|token_account| {
                        token_account_to_solana_account(
                            &token_account,
                            self.finalized_slot.load(Ordering::Relaxed),
                            0,
                            &self.mints_by_index,
                        )
                        .map(|account_data| {
                            let mint = self
                                .mints_by_index
                                .get(&token_account.mint)
                                .expect("Mint must exist when a token account for a mint exists");
                            (
                                TokenProgramAccountData::TokenAccount(
                                    TokenProgramTokenAccountData {
                                        account_data,
                                        mint_pubkey: mint.pubkey,
                                    },
                                ),
                                Some(mint.clone()),
                            )
                        })
                    })
            })
    }

    fn get_account_by_commitment(
        &self,
        account_pk: Pubkey,
        commitment: Commitment,
    ) -> Result<Option<(TokenProgramAccountData, Option<MintAccount>)>, AccountLoadingError> {
        match commitment {
            Commitment::Confirmed | Commitment::Processed => {
                match self
                    .processed_storage
                    .get_accounts(
                        vec![account_pk],
                        commitment,
                        self.confirmed_slot.load(Ordering::Relaxed),
                    )
                    .first()
                    .and_then(|first| first.as_ref())
                {
                    Some((processed_account, slot)) => Ok(token_program_account_to_solana_account(
                        &processed_account.processed_account,
                        *slot,
                        0,
                        &self.mints_by_index,
                    )
                    .map(|account_data| {
                        if let TokenProgramAccountType::TokenAccount(token_account) =
                            &processed_account.processed_account
                        {
                            let mint = self
                                .mints_by_index
                                .get(&token_account.mint)
                                .expect("Mint must exist when a token account for a mint exists");
                            (
                                TokenProgramAccountData::TokenAccount(
                                    TokenProgramTokenAccountData {
                                        account_data,
                                        mint_pubkey: mint.pubkey,
                                    },
                                ),
                                Some(mint.clone()),
                            )
                        } else {
                            (TokenProgramAccountData::OtherAccount(account_data), None)
                        }
                    })),
                    None => Ok(self.get_account_finalized(account_pk)),
                }
            }
            Commitment::Finalized => Ok(self.get_account_finalized(account_pk)),
        }
    }

    pub fn get_program_accounts_finalized(
        &self,
        program_type: TokenProgramType,
        account_filters: &Option<Vec<AccountFilterType>>,
    ) -> Result<(Vec<TokenProgramAccountData>, HashMap<Pubkey, MintAccount>), AccountLoadingError>
    {
        let account_filters: &Vec<AccountFilterType> = (account_filters
            .as_ref()
            .ok_or(AccountLoadingError::ShouldContainAnAccountFilter))?;

        let finalized_slot = self.finalized_slot.load(Ordering::Relaxed);
        let token_program_account_filter =
            get_token_program_account_filter(program_type.clone(), account_filters);

        match token_program_account_filter {
            TokenProgramAccountFilter::FilterError => {
                Err(AccountLoadingError::TokenAccountsCannotUseThisFilter)
            }
            TokenProgramAccountFilter::AccountFilter(AccountFilter {
                owner: owner_filter,
                mint: mint_filter,
            }) => {
                if let Some(owner) = owner_filter {
                    // filter token accounts by owner and optionally by mint
                    let indexes: Option<HashSet<u32>> = self
                        .account_by_owner_pubkey
                        .get(&owner.into())
                        .map(|indexes| indexes.iter().cloned().collect());

                    let mint = mint_filter
                        .and_then(|pk| self.mints_index_by_pubkey.get(&pk))
                        .map(|mint_index| *mint_index.value());

                    match indexes {
                        Some(indexes) => {
                            let mut mints: HashMap<Pubkey, MintAccount> = HashMap::new();
                            let mut accounts: Vec<TokenProgramAccountData> = vec![];
                            for token_account in self
                                .token_accounts_storage
                                .get_by_index(indexes)?
                                .iter()
                                .filter(|acc| {
                                    // filter by mint if necessary
                                    if let Some(mint) = mint {
                                        acc.mint == mint && acc.owner == owner
                                    } else {
                                        acc.owner == owner
                                    }
                                })
                            {
                                if let Some(account_data) = token_account_to_solana_account(
                                    token_account,
                                    finalized_slot,
                                    0,
                                    &self.mints_by_index,
                                ) {
                                    let mint_account_ref =
                                        self.mints_by_index.get(&token_account.mint).expect("Mint must exist when a token account for a mint exists");
                                    let mint_account = mint_account_ref.value();
                                    accounts.push(TokenProgramAccountData::TokenAccount(
                                        TokenProgramTokenAccountData {
                                            account_data,
                                            mint_pubkey: mint_account.pubkey,
                                        },
                                    ));
                                    if !mints.contains_key(&mint_account.pubkey) {
                                        mints.insert(mint_account.pubkey, mint_account.clone());
                                    }
                                }
                            }

                            Ok((accounts, mints))
                        }
                        None => Ok((vec![], HashMap::with_capacity(0))),
                    }
                } else if let Some(mint) = mint_filter {
                    // filter token accounts by mint
                    match self
                        .mints_index_by_pubkey
                        .get(&mint)
                        .and_then(|mint_index| self.accounts_index_by_mint.get(mint_index.value()))
                    {
                        Some(token_acc_indexes) => {
                            let indexes: HashSet<u32> =
                                token_acc_indexes.value().iter().cloned().collect();

                            let mint_account = self
                                .mints_index_by_pubkey
                                .get(&mint)
                                .and_then(|mint_index| self.mints_by_index.get(&mint_index))
                                .map(|mint_account| mint_account.value().clone())
                                .expect("Mint must exist when an account index by mint entry exists for a mint");

                            let token_accounts = self
                                .token_accounts_storage
                                .get_by_index(indexes)?
                                .iter()
                                .filter_map(|token_account| {
                                    token_account_to_solana_account(
                                        token_account,
                                        finalized_slot,
                                        0,
                                        &self.mints_by_index,
                                    )
                                })
                                .map(|account_data| {
                                    TokenProgramAccountData::TokenAccount(
                                        TokenProgramTokenAccountData {
                                            account_data,
                                            mint_pubkey: mint_account.pubkey,
                                        },
                                    )
                                })
                                .collect_vec();

                            let mut mints: HashMap<Pubkey, MintAccount> = HashMap::with_capacity(1);
                            mints.insert(mint_account.pubkey, mint_account);

                            Ok((token_accounts, mints))
                        }
                        None => Ok((vec![], HashMap::with_capacity(0))),
                    }
                } else {
                    Err(AccountLoadingError::ShouldContainAnAccountFilter)
                }
            }
            TokenProgramAccountFilter::MintFilter => {
                let mints = self
                    .mints_by_index
                    .iter()
                    .filter(|mint_account_entry| {
                        mint_account_entry.value().program == program_type.clone().into()
                    })
                    .map(|mint_account_entry| {
                        token_mint_to_solana_account(mint_account_entry.value(), finalized_slot, 0)
                    })
                    .filter(|account| {
                        account_filters
                            .iter()
                            .all(|filter| filter.allows(&account.account.data.data()))
                    })
                    .map(TokenProgramAccountData::OtherAccount)
                    .collect_vec();
                Ok((mints, HashMap::with_capacity(0)))
            }
            TokenProgramAccountFilter::MultisigFilter => {
                let multisigs = self
                    .multisigs
                    .iter()
                    .filter(|multisig_account_entry| {
                        multisig_account_entry.value().program == program_type.clone().into()
                    })
                    .map(|multisig_account_entry| {
                        token_multisig_to_solana_account(
                            multisig_account_entry.value(),
                            multisig_account_entry.pubkey,
                            finalized_slot,
                            0,
                        )
                    })
                    .filter(|account| {
                        account_filters
                            .iter()
                            .all(|filter| filter.allows(&account.account.data.data()))
                    })
                    .map(TokenProgramAccountData::OtherAccount)
                    .collect_vec();
                Ok((multisigs, HashMap::with_capacity(0)))
            }
            TokenProgramAccountFilter::NoFilter => {
                Err(AccountLoadingError::ShouldContainAnAccountFilter)
            }
        }
    }

    fn get_program_accounts_by_commitment(
        &self,
        program_pubkey: Pubkey,
        account_filters: &Option<Vec<AccountFilterType>>,
        commitment: Commitment,
    ) -> Result<(Vec<TokenProgramAccountData>, HashMap<Pubkey, MintAccount>), AccountLoadingError>
    {
        let program_type = TokenProgramType::try_from(&program_pubkey)
            .map_err(|_| AccountLoadingError::WrongIndex)?;
        // get all accounts for finalized commitment and update them if necessary
        let (mut account_data, mints_for_token_accounts) =
            self.get_program_accounts_finalized(program_type, account_filters)?;
        let confirmed_slot = self.confirmed_slot.load(Ordering::Relaxed);
        if commitment == Commitment::Processed || commitment == Commitment::Confirmed {
            // get list of all pks to search
            let pks = account_data.iter().map(|x| x.pubkey).collect_vec();
            let processed_accounts =
                self.processed_storage
                    .get_accounts(pks, commitment, confirmed_slot);
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
                            **account_data.get_mut(index).unwrap() = updated_account;
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
        Ok((account_data, mints_for_token_accounts))
    }
}

#[derive(Serialize, Deserialize)]
struct TokenProgramSnapshot {
    pub mints: Vec<(MintIndex, MintAccount)>,
    pub multisigs: Vec<MultiSig>,
    pub token_accounts: Vec<Vec<u8>>,
}

impl AccountStorageInterface for TokenProgramAccountsStorage {
    fn update_account(&self, account_data: AccountData, commitment: Commitment) -> bool {
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
                log::error!(
                    "Token account {} was not able to be identified {}",
                    account_data.pubkey.to_string(),
                    e
                );
                return false;
            }
        };
        if commitment == Commitment::Processed || commitment == Commitment::Confirmed {
            self.processed_storage.insert_account(
                account_data.pubkey,
                token_program_account,
                account_data.write_version,
                account_data.updated_slot,
            )
        } else {
            self.add_account_finalized(token_program_account)
        }
    }

    fn initialize_or_update_account(&self, account_data: AccountData) {
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
                log::error!(
                    "Token program account {} was not able to be identified {}",
                    account_data.pubkey.to_string(),
                    e
                );
                return;
            }
        };
        self.add_account_finalized(token_program_account);
    }

    fn get_account(
        &self,
        account_pk: Pubkey,
        commitment: Commitment,
    ) -> Result<Option<AccountData>, AccountLoadingError> {
        log::debug!("get account : {}", account_pk.to_string());
        self.get_account_by_commitment(account_pk, commitment)
            .map(|res| res.map(|(account_data, _)| account_data.into_inner()))
    }

    fn get_program_accounts(
        &self,
        program_pubkey: Pubkey,
        account_filters: Option<Vec<AccountFilterType>>,
        commitment: Commitment,
    ) -> Result<Vec<AccountData>, AccountLoadingError> {
        self.get_program_accounts_by_commitment(program_pubkey, &account_filters, commitment)
            .map(|(account_data, _)| {
                account_data
                    .into_iter()
                    .map(|acc| acc.into_inner())
                    .collect_vec()
            })
    }

    fn process_slot_data(&self, slot_info: SlotInfo, commitment: Commitment) -> Vec<AccountData> {
        match commitment {
            Commitment::Processed => {
                log::debug!("process slot data  : {slot_info:?}");
                self.processed_storage.update_slot_info(slot_info);
                vec![]
            }
            Commitment::Confirmed => {
                log::debug!("confirm slot data  : {slot_info:?}");
                if self.confirmed_slot.load(Ordering::Relaxed) < slot_info.slot {
                    self.confirmed_slot.store(slot_info.slot, Ordering::Relaxed);
                    self.processed_storage.update_slot_info(slot_info);
                }
                self.processed_storage
                    .get_processed_for_slot(slot_info, &self.mints_by_index)
            }
            Commitment::Finalized => {
                log::debug!("finalize slot data  : {slot_info:?}");
                if self.finalized_slot.load(Ordering::Relaxed) < slot_info.slot {
                    self.processed_storage.update_slot_info(slot_info);
                    self.finalized_slot.store(slot_info.slot, Ordering::Relaxed);
                }

                // move data from processed storage to main storage
                let finalized_accounts = self.processed_storage.finalize(slot_info.slot);

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
                    self.add_account_finalized(finalized_account.processed_account);
                }
                accounts_notifications
            }
        }
    }

    fn create_snapshot(&self, program_id: Pubkey) -> Result<Vec<u8>, AccountLoadingError> {
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
        let token_accounts = self.token_accounts_storage.create_snapshot(program)?;

        let snapshot = TokenProgramSnapshot {
            mints,
            multisigs,
            token_accounts,
        };
        let binary = bincode::serialize(&snapshot).unwrap();
        drop(snapshot);
        lz4::block::compress(
            &binary,
            Some(lz4::block::CompressionMode::HIGHCOMPRESSION(8)),
            false,
        )
        .map_err(|_| AccountLoadingError::CompressionIssues)
    }

    fn load_from_snapshot(
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

        for token_account_bytes in token_accounts {
            // use the new mint index
            let mut token_account = TokenAccount::from_bytes(&token_account_bytes);
            let new_mint_index = *mint_mapping.get(&token_account.mint).unwrap();
            token_account.mint = new_mint_index;
            let owner = token_account.owner;
            let (index, is_added) = self.token_accounts_storage.save_or_update(token_account);
            if is_added {
                match self.account_by_owner_pubkey.get_mut(&owner.into()) {
                    Some(mut accs) => accs.push(index),
                    None => {
                        self.account_by_owner_pubkey
                            .insert(owner.into(), vec![index]);
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

        Ok(())
    }
}

impl ProgramAccountStorageInterface for TokenProgramAccountsStorage {
    fn get_program_accounts_with_mints(
        &self,
        token_program_type: TokenProgramType,
        account_filters: Option<Vec<AccountFilterType>>,
        commitment: Commitment,
    ) -> Result<(Vec<TokenProgramAccountData>, HashMap<Pubkey, Mint>), AccountLoadingError> {
        // get all accounts for finalized commitment and update them if necessary
        let (account_data, mints_for_token_accounts) = self.get_program_accounts_by_commitment(
            *token_program_type.as_ref(),
            &account_filters,
            commitment,
        )?;

        let mints_for_token_accounts: HashMap<Pubkey, Mint> = mints_for_token_accounts
            .into_iter()
            .map(|(pubkey, mint_account)| {
                (
                    pubkey,
                    token_mint_to_spl_token_mint(
                        &mint_account,
                        self.finalized_slot.load(Ordering::Relaxed),
                        0,
                    ),
                )
            })
            .collect();

        Ok((account_data, mints_for_token_accounts))
    }

    fn get_account_with_mint(
        &self,
        account_pk: Pubkey,
        commitment: Commitment,
    ) -> Result<Option<(TokenProgramAccountData, Option<Mint>)>, AccountLoadingError> {
        self.get_account_by_commitment(account_pk, commitment)
            .map(|res| {
                res.map(|(account_data, mint)| {
                    let mint = mint.map(|mint_account| {
                        token_mint_to_spl_token_mint(
                            &mint_account,
                            self.finalized_slot.load(Ordering::Relaxed),
                            0,
                        )
                    });
                    (account_data, mint)
                })
            })
    }
}
