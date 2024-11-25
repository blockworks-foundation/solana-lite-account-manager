use std::{collections::BTreeMap, sync::Arc};

use lite_account_manager_common::{
    account_data::{self, Account, AccountData},
    account_filter::AccountFilterType,
    commitment::Commitment,
};
use solana_sdk::clock::Slot;

#[derive(Default)]
pub struct AccountDataByCommitment {
    // should have maximum 32 entries, all processed slots which are not yet finalized
    pub processed_accounts: BTreeMap<Slot, AccountData>,
    pub confirmed_account: Option<AccountData>,
    pub finalized_account: Option<AccountData>,
}

impl AccountDataByCommitment {
    // Should be used when accounts is created by geyser notification
    pub fn new(data: AccountData, commitment: Commitment) -> Self {
        let mut processed_accounts = BTreeMap::new();
        processed_accounts.insert(data.updated_slot, data.clone());

        AccountDataByCommitment {
            processed_accounts,
            confirmed_account: if commitment == Commitment::Confirmed
                || commitment == Commitment::Finalized
            {
                Some(data.clone())
            } else {
                None
            },
            finalized_account: if commitment == Commitment::Finalized {
                Some(data)
            } else {
                None
            },
        }
    }

    pub fn get_account_data(&self, commitment: Commitment) -> Option<AccountData> {
        match commitment {
            Commitment::Processed => self
                .processed_accounts
                .last_key_value()
                .map(|(_, v)| v.clone())
                .or(self.confirmed_account.clone()),
            Commitment::Confirmed => self.confirmed_account.clone(),
            Commitment::Finalized => self.finalized_account.clone(),
        }
    }

    pub fn get_account_data_filtered(
        &self,
        commitment: Commitment,
        filters: &Vec<AccountFilterType>,
    ) -> Option<AccountData> {
        let account_data = match commitment {
            Commitment::Processed => self
                .processed_accounts
                .last_key_value()
                .map(|(_, v)| v)
                .or(self.confirmed_account.as_ref()),
            Commitment::Confirmed => self.confirmed_account.as_ref(),
            Commitment::Finalized => self.finalized_account.as_ref(),
        };

        let Some(account_data) = account_data else {
            return None;
        };

        // check size filter first before decompressing
        for filter in filters {
            if let AccountFilterType::Datasize(size) = filter {
                if account_data.account.data.len() as u64 != *size {
                    return None;
                }
            }
        }

        // match other filters
        if !filters.is_empty() {
            if let account_data::Data::Uncompressed(data) = &account_data.account.data {
                // optimized for uncompressed data
                if filters.iter().all(|x| x.allows(data)) {
                    Some(account_data.clone())
                } else {
                    None
                }
            } else {
                // decompress data due to lz4/zstd compression
                let data = account_data.account.data.data();
                if filters.iter().all(|x| x.allows(&data)) {
                    Some(AccountData {
                        pubkey: account_data.pubkey,
                        account: Arc::new(Account {
                            lamports: account_data.account.lamports,
                            data: account_data::Data::Uncompressed(data),
                            owner: account_data.account.owner,
                            executable: account_data.account.executable,
                            rent_epoch: account_data.account.rent_epoch,
                        }),
                        updated_slot: account_data.updated_slot,
                        write_version: account_data.write_version,
                    })
                } else {
                    None
                }
            }
        } else {
            Some(account_data.clone())
        }
    }

    // should be called with finalized accounts data
    // when accounts storage is being warmed up
    pub fn initialize(data: AccountData) -> Self {
        let mut processed_accounts = BTreeMap::new();
        processed_accounts.insert(data.updated_slot, data.clone());
        AccountDataByCommitment {
            processed_accounts,
            confirmed_account: Some(data.clone()),
            finalized_account: Some(data),
        }
    }

    pub fn update(&mut self, data: AccountData, commitment: Commitment) -> bool {
        // if commitmentment is processed check and update processed
        // if commitmentment is confirmed check and update processed and confirmed
        // if commitmentment is finalized check and update all
        let update_confirmed = self
            .confirmed_account
            .as_ref()
            .map(|x| {
                x.updated_slot < data.updated_slot
                    || (x.updated_slot == data.updated_slot && x.write_version < data.write_version)
            })
            .unwrap_or(true);
        let update_finalized = self
            .finalized_account
            .as_ref()
            .map(|x| {
                x.updated_slot < data.updated_slot
                    || (x.updated_slot == data.updated_slot && x.write_version < data.write_version)
            })
            .unwrap_or(true);

        let mut updated = false;
        // processed not present for the slot
        // grpc can send multiple inter transaction changed account states for same slot
        // we have to update till we get the last
        match self.processed_accounts.get_mut(&data.updated_slot) {
            Some(processed_account) => {
                // check if the data is newer
                if processed_account.write_version < data.write_version {
                    *processed_account = data.clone();
                    updated = true;
                }
            }
            None => {
                self.processed_accounts
                    .insert(data.updated_slot, data.clone());
                updated = true;
            }
        }

        match commitment {
            Commitment::Confirmed => {
                if update_confirmed {
                    self.confirmed_account = Some(data);
                    updated = true;
                }
            }
            Commitment::Finalized => {
                if update_confirmed {
                    self.confirmed_account = Some(data.clone());
                }
                if update_finalized {
                    self.finalized_account = Some(data);
                    updated = true;
                }
            }
            Commitment::Processed => {
                // processed already treated
            }
        }
        updated
    }
    // this method will promote processed account to confirmed account to finalized account
    // returns promoted account
    pub fn promote_slot_commitment(
        &mut self,
        slot: Slot,
        commitment: Commitment,
    ) -> Option<(AccountData, Option<AccountData>)> {
        if let Some(account_data) = self.processed_accounts.get(&slot) {
            match commitment {
                Commitment::Processed => {
                    // do nothing
                    None
                }
                Commitment::Confirmed => match self.confirmed_account.take() {
                    Some(prev_account_data) => {
                        if prev_account_data.updated_slot < account_data.updated_slot {
                            self.confirmed_account = Some(account_data.clone());
                            Some((account_data.clone(), Some(prev_account_data)))
                        } else {
                            self.confirmed_account = Some(prev_account_data);
                            None
                        }
                    }
                    None => {
                        self.confirmed_account = Some(account_data.clone());
                        Some((account_data.clone(), None))
                    }
                },
                Commitment::Finalized => {
                    // update confirmed if needed
                    match &self.confirmed_account {
                        Some(confirmed_account) => {
                            if confirmed_account.updated_slot < account_data.updated_slot {
                                self.confirmed_account = Some(account_data.clone());
                            }
                        }
                        None => {
                            self.confirmed_account = Some(account_data.clone());
                        }
                    }

                    let result = match self.finalized_account.take() {
                        Some(prev_account_data) => {
                            if prev_account_data.updated_slot < account_data.updated_slot {
                                self.finalized_account = Some(account_data.clone());
                                Some((account_data.clone(), Some(prev_account_data)))
                            } else {
                                self.finalized_account = Some(prev_account_data);
                                None
                            }
                        }
                        None => {
                            self.finalized_account = Some(account_data.clone());
                            Some((account_data.clone(), None))
                        }
                    };

                    // slot finalized remove old processed slot data
                    while self.processed_accounts.len() > 1
                        && self
                            .processed_accounts
                            .first_key_value()
                            .map(|(s, _)| *s)
                            .unwrap_or(u64::MAX)
                            < slot
                    {
                        self.processed_accounts.pop_first();
                    }
                    result
                }
            }
        } else if commitment == Commitment::Finalized {
            // slot finalized remove old processed slot data
            while self.processed_accounts.len() > 1
                && self
                    .processed_accounts
                    .first_key_value()
                    .map(|(s, _)| *s)
                    .unwrap_or(u64::MAX)
                    < slot
            {
                self.processed_accounts.pop_first();
            }

            None
        } else {
            //log::warn!("Expected to have processed account update for slot {} data and pk {}", slot, pubkey);
            None
        }
    }

    pub fn delete(&mut self) {
        self.processed_accounts.clear();
        self.confirmed_account = None;
        self.finalized_account = None;
    }
}
