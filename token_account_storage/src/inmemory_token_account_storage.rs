use std::{
    collections::{HashSet, VecDeque},
    sync::{Arc, RwLock},
};

use dashmap::DashMap;
use itertools::Itertools;
use lite_account_manager_common::{
    account_store_interface::AccountLoadingError, pubkey_container_utils::PartialPubkey,
};
use prometheus::{opts, register_int_gauge, IntGauge};
use solana_sdk::pubkey::Pubkey;

use crate::{
    account_types::{Program, TokenAccount, TokenAccountIndex},
    token_account_storage_interface::TokenAccountStorageInterface,
};

lazy_static::lazy_static! {
    static ref TOKEN_ACCOUNT_STORED_IN_MEMORY: IntGauge =
       register_int_gauge!(opts!("lite_account_token_accounts_in_memory", "Account InMemory")).unwrap();

    static ref TOKEN_ACCOUNT_DELETED_IN_MEMORY: IntGauge =
       register_int_gauge!(opts!("lite_account_token_accounts_deleted_in_memory", "Account InMemory")).unwrap();
}

const PARTIAL_PUBKEY_SIZE: usize = 8;
type InmemoryPubkey = PartialPubkey<PARTIAL_PUBKEY_SIZE>;

#[derive(Default)]
pub struct InmemoryTokenAccountStorage {
    // use only first 8 bytes of pubkey to create an index
    pubkey_to_index: Arc<DashMap<InmemoryPubkey, Vec<TokenAccountIndex>>>,
    token_accounts: Arc<RwLock<VecDeque<Vec<u8>>>>,
}

impl TokenAccountStorageInterface for InmemoryTokenAccountStorage {
    fn contains(&self, pubkey: &Pubkey) -> Option<TokenAccountIndex> {
        match self.pubkey_to_index.entry(pubkey.into()) {
            dashmap::mapref::entry::Entry::Occupied(occ) => {
                let value = occ.get();
                if value.len() > 1 {
                    let lk = self.token_accounts.read().unwrap();
                    for index in value {
                        if TokenAccount::get_pubkey_from_binary(lk.get(*index as usize).unwrap())
                            == *pubkey
                        {
                            return Some(*index);
                        }
                    }
                    None
                } else {
                    Some(value[0])
                }
            }
            dashmap::mapref::entry::Entry::Vacant(_) => None,
        }
    }

    fn save_or_update(&self, token_account: TokenAccount) -> (TokenAccountIndex, bool) {
        match self.pubkey_to_index.entry(token_account.pubkey.into()) {
            dashmap::mapref::entry::Entry::Occupied(mut occ) => {
                // already present
                let mut write_lk = self.token_accounts.write().unwrap();
                {
                    let token_indexes = occ.get();
                    // update existing token account
                    for token_index in token_indexes {
                        let binary = write_lk.get_mut(*token_index as usize).unwrap();
                        if TokenAccount::get_pubkey_from_binary(binary) == token_account.pubkey {
                            *write_lk.get_mut(*token_index as usize).unwrap() =
                                token_account.to_bytes();
                            return (*token_index as TokenAccountIndex, false);
                        }
                    }
                }
                let token_indexes = occ.get_mut();
                let token_index = write_lk.len() as TokenAccountIndex;
                write_lk.push_back(token_account.to_bytes());
                token_indexes.push(token_index);
                TOKEN_ACCOUNT_STORED_IN_MEMORY.inc();
                (token_index as TokenAccountIndex, false)
            }
            dashmap::mapref::entry::Entry::Vacant(v) => {
                // add new token account
                let mut write_lk = self.token_accounts.write().unwrap();
                let token_index = write_lk.len() as TokenAccountIndex;
                write_lk.push_back(token_account.to_bytes());
                v.insert(vec![token_index as TokenAccountIndex]);
                drop(write_lk);
                TOKEN_ACCOUNT_STORED_IN_MEMORY.inc();
                (token_index, true)
            }
        }
    }

    fn get_by_index(
        &self,
        indexes: HashSet<TokenAccountIndex>,
    ) -> Result<Vec<TokenAccount>, AccountLoadingError> {
        let lk = self.token_accounts.read().unwrap();
        let accounts = indexes
            .iter()
            .filter_map(|index| lk.get(*index as usize))
            .map(|x| TokenAccount::from_bytes(x))
            .collect_vec();
        Ok(accounts)
    }

    fn get_by_pubkey(&self, pubkey: &Pubkey) -> Option<TokenAccount> {
        match self.pubkey_to_index.entry(pubkey.into()) {
            dashmap::mapref::entry::Entry::Occupied(occ) => {
                let indexes = occ.get();
                let lk = self.token_accounts.read().unwrap();
                for index in indexes {
                    let binary = lk.get(*index as usize).unwrap();
                    if TokenAccount::get_pubkey_from_binary(binary) == *pubkey {
                        let token_account: TokenAccount = TokenAccount::from_bytes(binary);
                        return Some(token_account);
                    }
                }
                None
            }
            dashmap::mapref::entry::Entry::Vacant(_) => None,
        }
    }

    fn delete(&self, pubkey: &Pubkey) {
        let acc_to_remove = self.pubkey_to_index.remove(&pubkey.into());
        if let Some((_, indexes)) = acc_to_remove {
            let mut write_lk = self.token_accounts.write().unwrap();
            let deleted_account = TokenAccount {
                program: Program::TokenProgram,
                is_deleted: true,
                pubkey: Pubkey::default(),
                owner: Pubkey::default(),
                mint: 0,
                amount: 0,
                state: crate::account_types::TokenProgramAccountState::Uninitialized,
                delegate: None,
                is_native: None,
                close_authority: None,
                additional_data: None,
            }
            .to_bytes();
            for index in indexes {
                let binary = write_lk.get_mut(index as usize).unwrap();
                if TokenAccount::get_pubkey_from_binary(binary) == *pubkey {
                    TOKEN_ACCOUNT_DELETED_IN_MEMORY.inc();
                    *binary = deleted_account;
                    return;
                }
            }
        }
    }

    fn create_snapshot(&self, program: Program) -> Result<Vec<Vec<u8>>, AccountLoadingError> {
        let program_bit = match program {
            Program::TokenProgram => 0,
            Program::Token2022Program => 0x1,
        };
        let lk = self.token_accounts.read().unwrap();
        Ok(lk
            .iter()
            .filter(|x| (*x.first().unwrap() & 0x3 == program_bit) && *x.get(1).unwrap() == 0)
            .cloned()
            .collect())
    }
}
