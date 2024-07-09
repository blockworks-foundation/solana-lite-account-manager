use std::{
    collections::{HashSet, VecDeque},
    sync::Arc,
};

use async_trait::async_trait;
use dashmap::DashMap;
use itertools::Itertools;
use lite_account_manager_common::{
    account_store_interface::AccountLoadingError, pubkey_container_utils::PartialPubkey,
};
use solana_sdk::pubkey::Pubkey;
use tokio::sync::RwLock;

use crate::{
    account_types::{Program, TokenAccount, TokenAccountIndex},
    token_account_storage_interface::TokenAccountStorageInterface,
};

const PARTIAL_PUBKEY_SIZE: usize = 8;
type InmemoryPubkey = PartialPubkey<PARTIAL_PUBKEY_SIZE>;

#[derive(Default)]
pub struct InmemoryTokenAccountStorage {
    // use only first 8 bytes of pubkey to create an index
    pubkey_to_index: Arc<DashMap<InmemoryPubkey, Vec<TokenAccountIndex>>>,
    token_accounts: Arc<RwLock<VecDeque<Vec<u8>>>>,
}

#[async_trait]
impl TokenAccountStorageInterface for InmemoryTokenAccountStorage {
    async fn contains(&self, pubkey: &Pubkey) -> Option<TokenAccountIndex> {
        match self.pubkey_to_index.get(&pubkey.into()) {
            Some(x) => {
                let value = x.value();
                if value.len() > 1 {
                    let lk = self.token_accounts.read().await;
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
            None => None,
        }
    }

    async fn save_or_update(&self, token_account: TokenAccount) -> (TokenAccountIndex, bool) {
        match self.pubkey_to_index.entry(token_account.pubkey.into()) {
            dashmap::mapref::entry::Entry::Occupied(mut occ) => {
                // already present
                let mut write_lk = self.token_accounts.write().await;
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
                (token_index as TokenAccountIndex, false)
            }
            dashmap::mapref::entry::Entry::Vacant(v) => {
                // add new token account
                let mut write_lk = self.token_accounts.write().await;
                let token_index = write_lk.len() as TokenAccountIndex;
                write_lk.push_back(token_account.to_bytes());
                v.insert(vec![token_index as TokenAccountIndex]);
                drop(write_lk);
                (token_index, true)
            }
        }
    }

    async fn get_by_index(
        &self,
        indexes: HashSet<TokenAccountIndex>,
    ) -> Result<Vec<TokenAccount>, AccountLoadingError> {
        let lk = self.token_accounts.read().await;
        let accounts = indexes
            .iter()
            .filter_map(|index| lk.get(*index as usize))
            .map(|x| TokenAccount::from_bytes(x))
            .collect_vec();
        Ok(accounts)
    }

    async fn get_by_pubkey(&self, pubkey: &Pubkey) -> Option<TokenAccount> {
        if let Some(acc) = self.pubkey_to_index.get(&pubkey.into()) {
            let indexes = acc.value();
            let lk = self.token_accounts.read().await;
            for index in indexes {
                let binary = lk.get(*index as usize).unwrap();
                if TokenAccount::get_pubkey_from_binary(binary) == *pubkey {
                    let token_account: TokenAccount = TokenAccount::from_bytes(binary);
                    return Some(token_account);
                }
            }
            None
        } else {
            None
        }
    }

    async fn delete(&self, pubkey: &Pubkey) {
        let acc_to_remove = self.pubkey_to_index.remove(&pubkey.into());
        if let Some((_, indexes)) = acc_to_remove {
            let mut write_lk = self.token_accounts.write().await;
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
                    *binary = deleted_account;
                    return;
                }
            }
        }
    }

    async fn create_snapshot(&self, program: Program) -> Result<Vec<Vec<u8>>, AccountLoadingError> {
        let program_bit = match program {
            Program::TokenProgram => 0,
            Program::Token2022Program => 0x1,
        };
        let lk = self.token_accounts.read().await;
        Ok(lk
            .iter()
            .filter(|x| (*x.first().unwrap() & 0x3 == program_bit) && *x.get(1).unwrap() == 0)
            .cloned()
            .collect())
    }
}
