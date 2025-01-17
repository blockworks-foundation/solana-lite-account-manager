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

        let account_data = account_data?;

        // check size filter first before decompressing
        for filter in filters {
            if let AccountFilterType::DataSize(size) = filter {
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

// this method will promote processed account to confirmed account to finalized account
// returns promoted account
#[cfg(test)]
mod tests {
    use super::*;
    use lite_account_manager_common::{
        account_data::{Account, AccountData},
        commitment::Commitment,
    };
    use solana_sdk::pubkey::Pubkey;

    fn create_account_data(slot: Slot, write_version: u64) -> AccountData {
        AccountData {
            pubkey: Pubkey::new_unique(),
            account: Arc::new(Account {
                lamports: 100,
                data: account_data::Data::Uncompressed(vec![0; 32]),
                owner: Pubkey::new_unique(),
                executable: false,
                rent_epoch: 0,
            }),
            updated_slot: slot,
            write_version,
        }
    }

    #[test]
    fn test_promote_slot_commitment() {
        // normal promotion
        let mut account_data_by_commitment = AccountDataByCommitment::default();
        let slot = 1;
        let data = create_account_data(slot, 1);

        account_data_by_commitment.update(data.clone(), Commitment::Processed);
        assert_eq!(
            account_data_by_commitment.promote_slot_commitment(slot, Commitment::Confirmed),
            Some((data.clone(), None))
        );
        assert_eq!(
            account_data_by_commitment.confirmed_account,
            Some(data.clone())
        );
        assert_eq!(
            account_data_by_commitment.promote_slot_commitment(slot, Commitment::Finalized),
            Some((data.clone(), None))
        );
        assert_eq!(
            account_data_by_commitment.confirmed_account,
            Some(data.clone())
        );
        assert_eq!(
            account_data_by_commitment.finalized_account,
            Some(data.clone())
        );

        let slot = 2;
        let data2 = create_account_data(slot, 2);
        account_data_by_commitment.update(data2.clone(), Commitment::Processed);
        assert_eq!(
            account_data_by_commitment.promote_slot_commitment(slot, Commitment::Finalized),
            Some((data2.clone(), Some(data.clone())))
        );
        assert_eq!(
            account_data_by_commitment.confirmed_account,
            Some(data2.clone())
        );
        assert_eq!(
            account_data_by_commitment.finalized_account,
            Some(data2.clone())
        );
    }

    #[test]
    fn test_update_and_get_account_data() {
        let mut account_data_by_commitment = AccountDataByCommitment::default();
        let slot = 1;
        let data = create_account_data(slot, 1);

        account_data_by_commitment.update(data.clone(), Commitment::Processed);
        assert_eq!(
            account_data_by_commitment.get_account_data(Commitment::Processed),
            Some(data.clone())
        );

        account_data_by_commitment.update(data.clone(), Commitment::Confirmed);
        assert_eq!(
            account_data_by_commitment.get_account_data(Commitment::Confirmed),
            Some(data.clone())
        );

        account_data_by_commitment.update(data.clone(), Commitment::Finalized);
        assert_eq!(
            account_data_by_commitment.get_account_data(Commitment::Finalized),
            Some(data.clone())
        );
    }

    #[test]
    fn test_update_finalized_account() {
        let mut account_data_by_commitment = AccountDataByCommitment::default();
        let slot = 1;
        let data = create_account_data(slot, 10);

        account_data_by_commitment.update(data.clone(), Commitment::Finalized);
        assert_eq!(
            account_data_by_commitment.get_account_data(Commitment::Processed),
            Some(data.clone())
        );

        assert_eq!(
            account_data_by_commitment.get_account_data(Commitment::Confirmed),
            Some(data.clone())
        );

        assert_eq!(
            account_data_by_commitment.get_account_data(Commitment::Finalized),
            Some(data.clone())
        );
    }

    #[test]
    fn test_update_confirmed_account() {
        let mut account_data_by_commitment = AccountDataByCommitment::default();
        let slot = 1;
        let data = create_account_data(slot, 10);

        account_data_by_commitment.update(data.clone(), Commitment::Confirmed);
        assert_eq!(
            account_data_by_commitment.get_account_data(Commitment::Processed),
            Some(data.clone())
        );

        assert_eq!(
            account_data_by_commitment.get_account_data(Commitment::Confirmed),
            Some(data.clone())
        );

        assert_eq!(
            account_data_by_commitment.get_account_data(Commitment::Finalized),
            None
        );
    }

    #[test]
    fn test_update_processed_account() {
        let mut account_data_by_commitment = AccountDataByCommitment::default();
        let slot = 5;
        let data = create_account_data(slot, 10);

        account_data_by_commitment.update(data.clone(), Commitment::Processed);
        assert_eq!(
            account_data_by_commitment.get_account_data(Commitment::Processed),
            Some(data.clone())
        );

        assert_eq!(
            account_data_by_commitment.get_account_data(Commitment::Confirmed),
            None
        );

        assert_eq!(
            account_data_by_commitment.get_account_data(Commitment::Finalized),
            None
        );

        let data_2 = create_account_data(slot, 9);

        account_data_by_commitment.update(data_2.clone(), Commitment::Processed);
        assert_eq!(
            account_data_by_commitment.get_account_data(Commitment::Processed),
            Some(data.clone())
        );

        assert_eq!(
            account_data_by_commitment.get_account_data(Commitment::Confirmed),
            None
        );

        assert_eq!(
            account_data_by_commitment.get_account_data(Commitment::Finalized),
            None
        );

        let data_3 = create_account_data(slot, 11);

        account_data_by_commitment.update(data_3.clone(), Commitment::Processed);
        assert_eq!(
            account_data_by_commitment.get_account_data(Commitment::Processed),
            Some(data_3.clone())
        );

        assert_eq!(
            account_data_by_commitment.get_account_data(Commitment::Confirmed),
            None
        );

        assert_eq!(
            account_data_by_commitment.get_account_data(Commitment::Finalized),
            None
        );

        let data_4 = create_account_data(4, 8);
        account_data_by_commitment.update(data_4.clone(), Commitment::Processed);
        assert_eq!(
            account_data_by_commitment.get_account_data(Commitment::Processed),
            Some(data_3.clone())
        );

        assert_eq!(
            account_data_by_commitment.get_account_data(Commitment::Confirmed),
            None
        );

        assert_eq!(
            account_data_by_commitment.get_account_data(Commitment::Finalized),
            None
        );

        assert_eq!(
            account_data_by_commitment.promote_slot_commitment(3, Commitment::Confirmed),
            None
        );
        assert_eq!(
            account_data_by_commitment.get_account_data(Commitment::Processed),
            Some(data_3.clone())
        );

        assert_eq!(
            account_data_by_commitment.get_account_data(Commitment::Confirmed),
            None
        );

        assert_eq!(
            account_data_by_commitment.get_account_data(Commitment::Finalized),
            None
        );

        assert_eq!(
            account_data_by_commitment.promote_slot_commitment(4, Commitment::Confirmed),
            Some((data_4.clone(), None))
        );
        assert_eq!(
            account_data_by_commitment.get_account_data(Commitment::Processed),
            Some(data_3.clone())
        );

        assert_eq!(
            account_data_by_commitment.get_account_data(Commitment::Confirmed),
            Some(data_4.clone())
        );

        assert_eq!(
            account_data_by_commitment.get_account_data(Commitment::Finalized),
            None
        );

        assert_eq!(
            account_data_by_commitment.promote_slot_commitment(5, Commitment::Confirmed),
            Some((data_3.clone(), Some(data_4.clone())))
        );
        assert_eq!(
            account_data_by_commitment.get_account_data(Commitment::Processed),
            Some(data_3.clone())
        );

        assert_eq!(
            account_data_by_commitment.get_account_data(Commitment::Confirmed),
            Some(data_3.clone())
        );

        assert_eq!(
            account_data_by_commitment.get_account_data(Commitment::Finalized),
            None
        );

        assert_eq!(
            account_data_by_commitment.promote_slot_commitment(5, Commitment::Finalized),
            Some((data_3.clone(), None))
        );

        assert_eq!(
            account_data_by_commitment.get_account_data(Commitment::Processed),
            Some(data_3.clone())
        );

        assert_eq!(
            account_data_by_commitment.get_account_data(Commitment::Confirmed),
            Some(data_3.clone())
        );

        assert_eq!(
            account_data_by_commitment.get_account_data(Commitment::Finalized),
            Some(data_3.clone())
        );
    }

    #[test]
    fn test_update_and_get_account_data_old_slot_and_write_version() {
        let mut account_data_by_commitment = AccountDataByCommitment::default();
        let slot = 1;
        let data = create_account_data(slot, 10);

        account_data_by_commitment.update(data.clone(), Commitment::Processed);
        assert_eq!(
            account_data_by_commitment.get_account_data(Commitment::Processed),
            Some(data.clone())
        );

        account_data_by_commitment.update(data.clone(), Commitment::Confirmed);
        assert_eq!(
            account_data_by_commitment.get_account_data(Commitment::Confirmed),
            Some(data.clone())
        );

        account_data_by_commitment.update(data.clone(), Commitment::Finalized);
        assert_eq!(
            account_data_by_commitment.get_account_data(Commitment::Finalized),
            Some(data.clone())
        );
    }

    #[test]
    fn test_get_account_data_filtered() {
        let mut account_data_by_commitment = AccountDataByCommitment::default();
        let slot = 1;
        let data = create_account_data(slot, 1);

        account_data_by_commitment.update(data.clone(), Commitment::Processed);
        let filters = vec![AccountFilterType::DataSize(32)];
        assert_eq!(
            account_data_by_commitment.get_account_data_filtered(Commitment::Processed, &filters),
            Some(data.clone())
        );

        let filters = vec![AccountFilterType::DataSize(64)];
        assert_eq!(
            account_data_by_commitment.get_account_data_filtered(Commitment::Processed, &filters),
            None
        );
    }

    #[test]
    fn test_initialize() {
        let data = create_account_data(1, 1);
        let account_data_by_commitment = AccountDataByCommitment::initialize(data.clone());

        assert_eq!(
            account_data_by_commitment.get_account_data(Commitment::Processed),
            Some(data.clone())
        );
        assert_eq!(
            account_data_by_commitment.get_account_data(Commitment::Confirmed),
            Some(data.clone())
        );
        assert_eq!(
            account_data_by_commitment.get_account_data(Commitment::Finalized),
            Some(data)
        );
    }

    #[test]
    fn test_delete() {
        let mut account_data_by_commitment = AccountDataByCommitment::default();
        let slot = 1;
        let data = create_account_data(slot, 1);

        account_data_by_commitment.update(data.clone(), Commitment::Processed);
        account_data_by_commitment.delete();

        assert_eq!(
            account_data_by_commitment.get_account_data(Commitment::Processed),
            None
        );
        assert_eq!(
            account_data_by_commitment.get_account_data(Commitment::Confirmed),
            None
        );
        assert_eq!(
            account_data_by_commitment.get_account_data(Commitment::Finalized),
            None
        );
    }
}
