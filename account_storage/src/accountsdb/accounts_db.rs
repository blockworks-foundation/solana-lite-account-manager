use std::sync::{Arc, Mutex};

use itertools::Itertools;
use solana_accounts_db::accounts::Accounts;
use solana_accounts_db::accounts_db::{
    AccountShrinkThreshold, AccountsDb as SolanaAccountsDb, AccountsDbConfig, CreateAncientStorage,
};
use solana_accounts_db::accounts_file::StorageAccess;
use solana_accounts_db::accounts_index::{
    AccountSecondaryIndexes, AccountsIndexConfig, IndexLimitMb,
};
use solana_accounts_db::ancestors::Ancestors;
use solana_accounts_db::partitioned_rewards::TestPartitionedEpochRewards;
use solana_sdk::account::{AccountSharedData, ReadableAccount};
use solana_sdk::clock::Slot;
use solana_sdk::genesis_config::ClusterType;
use solana_sdk::pubkey::Pubkey;
use solana_sdk::transaction_context::TransactionAccount;

use lite_account_manager_common::account_data::{Account, AccountData, Data};
use lite_account_manager_common::account_filter::AccountFilterType;
use lite_account_manager_common::account_store_interface::{
    AccountLoadingError, AccountStorageInterface,
};
use lite_account_manager_common::commitment::Commitment;
use lite_account_manager_common::slot_info::SlotInfo;

pub const BINS: usize = 8192;
pub const FLUSH_THREADS: usize = 1;

pub const ACCOUNTS_INDEX_CONFIG: AccountsIndexConfig = AccountsIndexConfig {
    bins: Some(BINS),
    flush_threads: Some(FLUSH_THREADS),
    drives: None,
    index_limit_mb: IndexLimitMb::Unspecified,
    ages_to_stay_in_cache: None,
    scan_results_limit_bytes: None,
    started_from_validator: false,
};

pub const ACCOUNTS_DB_CONFIG: AccountsDbConfig = AccountsDbConfig {
    index: Some(ACCOUNTS_INDEX_CONFIG),
    base_working_path: None,
    accounts_hash_cache_path: None,
    shrink_paths: None,
    read_cache_limit_bytes: None,
    write_cache_limit_bytes: None,
    ancient_append_vec_offset: None,
    skip_initial_hash_calc: false,
    exhaustively_verify_refcounts: false,
    create_ancient_storage: CreateAncientStorage::Pack,
    test_partitioned_epoch_rewards: TestPartitionedEpochRewards::None,
    test_skip_rewrites_but_include_in_bank_hash: false,
    storage_access: StorageAccess::Mmap,
};

pub struct AccountsDb {
    accounts: Accounts,
    slot_state: SlotState,
}

pub struct SlotState {
    inner: Mutex<InnerSlotState>,
}

struct InnerSlotState {
    processed: u64,
    confirmed: u64,
    finalised: u64,
}

impl SlotState {
    fn new(processed: u64, confirmed: u64, finalised: u64) -> Self {
        Self {
            inner: Mutex::new(InnerSlotState {
                processed,
                confirmed,
                finalised,
            }),
        }
    }

    fn get(&self) -> (u64, u64, u64) {
        let inner = self.inner.lock().unwrap();
        (inner.processed, inner.confirmed, inner.finalised)
    }

    fn set(&self, processed: u64, confirmed: u64, finalised: u64) {
        let mut inner = self.inner.lock().unwrap();
        inner.processed = processed;
        inner.confirmed = confirmed;
        inner.finalised = finalised;
    }
}

impl AccountsDb {
    #[allow(clippy::new_without_default)]
    pub fn new() -> Self {
        let db = SolanaAccountsDb::new_with_config(
            vec![],
            &ClusterType::MainnetBeta,
            AccountSecondaryIndexes::default(),
            AccountShrinkThreshold::default(),
            Some(ACCOUNTS_DB_CONFIG),
            None,
            Arc::default(),
        );

        let accounts = Accounts::new(Arc::new(db));
        Self {
            accounts,
            slot_state: SlotState::new(0, 0, 0),
        }
    }

    pub fn new_for_testing() -> Self {
        let db = SolanaAccountsDb::new_single_for_tests();
        let accounts = Accounts::new(Arc::new(db));
        Self {
            accounts,
            slot_state: SlotState::new(0, 0, 0),
        }
    }
}

impl AccountStorageInterface for AccountsDb {
    fn update_account(&self, account_data: AccountData, _commitment: Commitment) -> bool {
        self.initilize_or_update_account(account_data);
        true
    }

    fn initilize_or_update_account(&self, account_data: AccountData) {
        let shared_data = AccountSharedData::from(account_data.account.to_solana_account());

        let account_to_store = [(&account_data.pubkey, &shared_data)];
        self.accounts
            .store_accounts_cached((account_data.updated_slot, account_to_store.as_slice()));
    }

    fn get_account(
        &self,
        account_pk: Pubkey,
        commitment: Commitment,
    ) -> Result<Option<AccountData>, AccountLoadingError> {
        let ancestors = self.get_ancestors_from_commitment(commitment);
        Ok(self
            .accounts
            .load_with_fixed_root(&ancestors, &account_pk)
            .map(|(shared_data, slot)| {
                Self::convert_to_account_data(account_pk, slot, shared_data)
            }))
    }

    fn get_program_accounts(
        &self,
        program_pubkey: Pubkey,
        account_filter: Option<Vec<AccountFilterType>>,
        commitment: Commitment,
    ) -> Result<Vec<AccountData>, AccountLoadingError> {
        let slot = self.get_slot_from_commitment(commitment);

        let filter = |data: &AccountSharedData| match &account_filter {
            Some(filters) => filters.iter().all(|filter| match filter {
                AccountFilterType::Datasize(size) => data.data().len() == *size as usize,
                AccountFilterType::Memcmp(cmp) => cmp.bytes_match(data.data()),
            }),
            None => true,
        };

        let transaction_accounts: Vec<TransactionAccount> = self
            .accounts
            .load_by_program_slot(slot, Some(&program_pubkey))
            .into_iter()
            .filter(|ta| filter(&ta.1))
            .collect();

        let result = transaction_accounts
            .into_iter()
            .map(|ta| Self::convert_to_account_data(ta.0, slot, ta.1))
            .collect_vec();

        Ok(result)
    }

    fn process_slot_data(&self, info: SlotInfo, commitment: Commitment) -> Vec<AccountData> {
        let slot = info.slot;
        if commitment == Commitment::Finalized {
            self.accounts.add_root(slot);
        }

        let (mut processed, mut confirmed, mut finalized) = self.slot_state.get();
        match commitment {
            Commitment::Processed => {
                if slot > processed {
                    processed = slot;
                }
            }
            Commitment::Confirmed => {
                if slot > processed {
                    processed = slot;
                }
                if slot > confirmed {
                    confirmed = slot;
                }
            }
            Commitment::Finalized => {
                if slot > processed {
                    processed = slot
                }
                if slot > confirmed {
                    confirmed = slot
                }
                if slot > finalized {
                    finalized = slot
                }
            }
        }

        assert!(processed >= confirmed);
        assert!(confirmed >= finalized);

        self.slot_state.set(processed, confirmed, finalized);

        self.accounts
            .load_all(&Ancestors::from(vec![slot]), slot, false)
            .unwrap()
            .into_iter()
            .filter(|(_, _, updated_slot)| *updated_slot == slot)
            .map(|(key, data, slot)| Self::convert_to_account_data(key, slot, data))
            .collect_vec()
    }

    fn create_snapshot(&self, _program_id: Pubkey) -> Result<Vec<u8>, AccountLoadingError> {
        unimplemented!()
    }

    fn load_from_snapshot(
        &self,
        _program_id: Pubkey,
        _snapshot: Vec<u8>,
    ) -> Result<(), AccountLoadingError> {
        unimplemented!()
    }
}

impl AccountsDb {
    fn get_slot_from_commitment(&self, commitment: Commitment) -> Slot {
        let (processed, confirmed, finalized) = self.slot_state.get();
        match commitment {
            Commitment::Processed => processed,
            Commitment::Confirmed => confirmed,
            Commitment::Finalized => finalized,
        }
    }

    fn get_ancestors_from_commitment(&self, commitment: Commitment) -> Ancestors {
        let slot = self.get_slot_from_commitment(commitment);
        Ancestors::from(vec![slot])
    }

    fn convert_to_account_data(
        pk: Pubkey,
        slot: Slot,
        shared_data: AccountSharedData,
    ) -> AccountData {
        AccountData {
            pubkey: pk,
            account: Arc::new(Account {
                lamports: shared_data.lamports(),
                data: Data::Uncompressed(Vec::from(shared_data.data())),
                owner: *shared_data.owner(),
                executable: shared_data.executable(),
                rent_epoch: shared_data.rent_epoch(),
            }),
            updated_slot: slot,
            write_version: 0,
        }
    }
}

#[cfg(test)]
mod tests {
    use std::str::FromStr;

    use solana_sdk::pubkey::Pubkey;

    use lite_account_manager_common::account_store_interface::AccountStorageInterface;
    use lite_account_manager_common::commitment::Commitment::Processed;

    use crate::accountsdb::accounts_db::{create_account_data, slot_info};
    use crate::accountsdb::AccountsDb;

    #[test]
    fn store_new_account() {
        let ti = AccountsDb::new_for_testing();

        let pk = Pubkey::from_str("HZGMUF6kdCUK6nuc3TdNR6X5HNdGtg5HmVQ8cV2pRiHE").unwrap();
        let ak = Pubkey::from_str("6rRiMihF7UdJz25t5QvS7PgP9yzfubN7TBRv26ZBVAhE").unwrap();

        let ad = create_account_data(2, ak, pk, 1);
        ti.initilize_or_update_account(ad);

        ti.process_slot_data(slot_info(2), Processed);

        let result = ti.get_account(ak, Processed);
        assert!(result.is_ok());
        let data = result.unwrap().unwrap();
        assert_eq!(data.updated_slot, 2);
        assert_eq!(data.account.lamports, 1);
    }

    mod get_account {
        use std::str::FromStr;

        use solana_sdk::pubkey::Pubkey;

        use lite_account_manager_common::account_store_interface::AccountStorageInterface;
        use lite_account_manager_common::commitment::Commitment::{
            Confirmed, Finalized, Processed,
        };

        use crate::accountsdb::accounts_db::{create_account_data, slot_info};
        use crate::accountsdb::AccountsDb;

        #[test]
        fn different_commitments() {
            let ti = AccountsDb::new_for_testing();

            let pk = Pubkey::from_str("HZGMUF6kdCUK6nuc3TdNR6X5HNdGtg5HmVQ8cV2pRiHE").unwrap();
            let ak = Pubkey::from_str("6rRiMihF7UdJz25t5QvS7PgP9yzfubN7TBRv26ZBVAhE").unwrap();

            ti.process_slot_data(slot_info(5), Processed);
            ti.process_slot_data(slot_info(4), Confirmed);
            ti.process_slot_data(slot_info(3), Finalized);

            ti.initilize_or_update_account(create_account_data(5, ak, pk, 10));
            ti.initilize_or_update_account(create_account_data(4, ak, pk, 20));
            ti.initilize_or_update_account(create_account_data(3, ak, pk, 30));

            let processed = ti.get_account(ak, Processed).unwrap().unwrap();
            assert_eq!(processed.updated_slot, 5);
            assert_eq!(processed.account.lamports, 10);

            let confirmed = ti.get_account(ak, Confirmed).unwrap().unwrap();
            assert_eq!(confirmed.updated_slot, 4);
            assert_eq!(confirmed.account.lamports, 20);

            let finalized = ti.get_account(ak, Finalized).unwrap().unwrap();
            assert_eq!(finalized.updated_slot, 3);
            assert_eq!(finalized.account.lamports, 30);
        }

        #[test]
        fn becoming_available_after_slot_update() {
            let ti = AccountsDb::new_for_testing();

            let pk = Pubkey::from_str("HZGMUF6kdCUK6nuc3TdNR6X5HNdGtg5HmVQ8cV2pRiHE").unwrap();
            let ak = Pubkey::from_str("6rRiMihF7UdJz25t5QvS7PgP9yzfubN7TBRv26ZBVAhE").unwrap();

            ti.initilize_or_update_account(create_account_data(5, ak, pk, 10));

            // Slot = Processed
            ti.process_slot_data(slot_info(5), Processed);

            let processed = ti.get_account(ak, Processed).unwrap().unwrap();
            assert_eq!(processed.updated_slot, 5);
            assert_eq!(processed.account.lamports, 10);

            let confirmed = ti.get_account(ak, Confirmed).unwrap();
            assert_eq!(confirmed, None);

            let finalized = ti.get_account(ak, Finalized).unwrap();
            assert_eq!(finalized, None);

            // Slot = Confirmed
            ti.process_slot_data(slot_info(5), Confirmed);

            let processed = ti.get_account(ak, Processed).unwrap().unwrap();
            assert_eq!(processed.updated_slot, 5);
            assert_eq!(processed.account.lamports, 10);

            let confirmed = ti.get_account(ak, Confirmed).unwrap().unwrap();
            assert_eq!(confirmed.updated_slot, 5);
            assert_eq!(confirmed.account.lamports, 10);

            let finalized = ti.get_account(ak, Finalized).unwrap();
            assert_eq!(finalized, None);

            // Slot = Finalized
            ti.process_slot_data(slot_info(5), Finalized);

            let processed = ti.get_account(ak, Processed).unwrap().unwrap();
            assert_eq!(processed.updated_slot, 5);
            assert_eq!(processed.account.lamports, 10);

            let confirmed = ti.get_account(ak, Confirmed).unwrap().unwrap();
            assert_eq!(confirmed.updated_slot, 5);
            assert_eq!(confirmed.account.lamports, 10);

            let finalized = ti.get_account(ak, Finalized).unwrap().unwrap();
            assert_eq!(finalized.updated_slot, 5);
            assert_eq!(finalized.account.lamports, 10);
        }
    }

    mod get_program_accounts {
        use std::str::FromStr;

        use solana_sdk::pubkey::Pubkey;

        use lite_account_manager_common::account_filter::{
            AccountFilterType, MemcmpFilter, MemcmpFilterData,
        };
        use lite_account_manager_common::account_store_interface::AccountStorageInterface;
        use lite_account_manager_common::commitment::Commitment::{
            Confirmed, Finalized, Processed,
        };

        use crate::accountsdb::accounts_db::{
            create_account_data, create_account_data_with_data, slot_info,
        };
        use crate::accountsdb::AccountsDb;

        #[test]
        fn different_commitments() {
            let ti = AccountsDb::new_for_testing();

            let pk = Pubkey::from_str("HZGMUF6kdCUK6nuc3TdNR6X5HNdGtg5HmVQ8cV2pRiHE").unwrap();
            let ak = Pubkey::from_str("6rRiMihF7UdJz25t5QvS7PgP9yzfubN7TBRv26ZBVAhE").unwrap();

            ti.process_slot_data(slot_info(5), Processed);
            ti.process_slot_data(slot_info(4), Confirmed);
            ti.process_slot_data(slot_info(3), Finalized);

            ti.initilize_or_update_account(create_account_data(5, ak, pk, 10));
            ti.initilize_or_update_account(create_account_data(4, ak, pk, 20));
            ti.initilize_or_update_account(create_account_data(3, ak, pk, 30));

            let processed = ti
                .get_program_accounts(pk, None, Processed)
                .unwrap()
                .pop()
                .unwrap();
            assert_eq!(processed.updated_slot, 5);
            assert_eq!(processed.account.lamports, 10);

            let confirmed = ti
                .get_program_accounts(pk, None, Confirmed)
                .unwrap()
                .pop()
                .unwrap();
            assert_eq!(confirmed.updated_slot, 4);
            assert_eq!(confirmed.account.lamports, 20);

            let finalized = ti
                .get_program_accounts(pk, None, Finalized)
                .unwrap()
                .pop()
                .unwrap();
            assert_eq!(finalized.updated_slot, 3);
            assert_eq!(finalized.account.lamports, 30);
        }

        #[test]
        fn becoming_available_after_slot_update() {
            let ti = AccountsDb::new_for_testing();

            let pk = Pubkey::from_str("HZGMUF6kdCUK6nuc3TdNR6X5HNdGtg5HmVQ8cV2pRiHE").unwrap();
            let ak = Pubkey::from_str("6rRiMihF7UdJz25t5QvS7PgP9yzfubN7TBRv26ZBVAhE").unwrap();

            ti.initilize_or_update_account(create_account_data(5, ak, pk, 10));

            // Slot = Processed
            ti.process_slot_data(slot_info(5), Processed);

            let processed = ti
                .get_program_accounts(pk, None, Processed)
                .unwrap()
                .pop()
                .unwrap();
            assert_eq!(processed.updated_slot, 5);
            assert_eq!(processed.account.lamports, 10);

            let confirmed = ti.get_program_accounts(pk, None, Confirmed);
            assert_eq!(confirmed, Ok(Vec::new()));

            let finalized = ti.get_program_accounts(pk, None, Finalized);
            assert_eq!(finalized, Ok(Vec::new()));

            // Slot = Confirmed
            ti.process_slot_data(slot_info(5), Confirmed);

            let processed = ti
                .get_program_accounts(pk, None, Processed)
                .unwrap()
                .pop()
                .unwrap();
            assert_eq!(processed.updated_slot, 5);
            assert_eq!(processed.account.lamports, 10);

            let confirmed = ti
                .get_program_accounts(pk, None, Confirmed)
                .unwrap()
                .pop()
                .unwrap();
            assert_eq!(confirmed.updated_slot, 5);
            assert_eq!(confirmed.account.lamports, 10);

            let finalized = ti.get_program_accounts(pk, None, Finalized);
            assert_eq!(finalized, Ok(Vec::new()));

            // Slot = Finalized
            ti.process_slot_data(slot_info(5), Finalized);

            let processed = ti
                .get_program_accounts(pk, None, Processed)
                .unwrap()
                .pop()
                .unwrap();
            assert_eq!(processed.updated_slot, 5);
            assert_eq!(processed.account.lamports, 10);

            let confirmed = ti
                .get_program_accounts(pk, None, Confirmed)
                .unwrap()
                .pop()
                .unwrap();
            assert_eq!(confirmed.updated_slot, 5);
            assert_eq!(confirmed.account.lamports, 10);

            let finalized = ti
                .get_program_accounts(pk, None, Finalized)
                .unwrap()
                .pop()
                .unwrap();
            assert_eq!(finalized.updated_slot, 5);
            assert_eq!(finalized.account.lamports, 10);
        }

        #[test]
        fn filter_by_data_size() {
            let ti = AccountsDb::new_for_testing();

            let pk = Pubkey::from_str("HZGMUF6kdCUK6nuc3TdNR6X5HNdGtg5HmVQ8cV2pRiHE").unwrap();
            let ak1 = Pubkey::from_str("6rRiMihF7UdJz25t5QvS7PgP9yzfubN7TBRv26ZBVAhE").unwrap();
            let ak2 = Pubkey::from_str("5VsPdDtqyFw6BmxrTZXKfnTLZy3TgzVA2MA1vZKAfddw").unwrap();

            ti.process_slot_data(slot_info(5), Processed);
            ti.initilize_or_update_account(create_account_data_with_data(
                5,
                ak1,
                pk,
                Vec::from("abc"),
            ));
            ti.initilize_or_update_account(create_account_data_with_data(
                5,
                ak2,
                pk,
                Vec::from("abcdef"),
            ));

            let mut result = ti
                .get_program_accounts(pk, Some(vec![AccountFilterType::Datasize(3)]), Processed)
                .unwrap();
            assert_eq!(result.len(), 1);
            let result = result.pop().unwrap();
            assert_eq!(result.pubkey, ak1);
            assert_eq!(result.updated_slot, 5);
            assert_eq!(result.account.data.data(), Vec::from("abc"));
        }

        #[test]
        fn filter_by_mem_cmp() {
            let ti = AccountsDb::new_for_testing();

            let pk = Pubkey::from_str("HZGMUF6kdCUK6nuc3TdNR6X5HNdGtg5HmVQ8cV2pRiHE").unwrap();
            let ak1 = Pubkey::from_str("6rRiMihF7UdJz25t5QvS7PgP9yzfubN7TBRv26ZBVAhE").unwrap();
            let ak2 = Pubkey::from_str("5VsPdDtqyFw6BmxrTZXKfnTLZy3TgzVA2MA1vZKAfddw").unwrap();

            ti.process_slot_data(slot_info(5), Processed);
            ti.initilize_or_update_account(create_account_data_with_data(
                5,
                ak1,
                pk,
                Vec::from("abc"),
            ));
            ti.initilize_or_update_account(create_account_data_with_data(
                5,
                ak2,
                pk,
                Vec::from("abcdef"),
            ));

            let mut result = ti
                .get_program_accounts(
                    pk,
                    Some(vec![AccountFilterType::Memcmp(MemcmpFilter::new(
                        1,
                        MemcmpFilterData::Bytes(Vec::from("bcdef")),
                    ))]),
                    Processed,
                )
                .unwrap();
            assert_eq!(result.len(), 1);
            let result = result.pop().unwrap();
            assert_eq!(result.pubkey, ak2);
            assert_eq!(result.updated_slot, 5);
            assert_eq!(result.account.data.data(), Vec::from("abcdef"));
        }

        #[test]
        fn multiple_filter() {
            let ti = AccountsDb::new_for_testing();

            let pk = Pubkey::from_str("HZGMUF6kdCUK6nuc3TdNR6X5HNdGtg5HmVQ8cV2pRiHE").unwrap();
            let ak1 = Pubkey::from_str("6rRiMihF7UdJz25t5QvS7PgP9yzfubN7TBRv26ZBVAhE").unwrap();
            let ak2 = Pubkey::from_str("5VsPdDtqyFw6BmxrTZXKfnTLZy3TgzVA2MA1vZKAfddw").unwrap();

            ti.process_slot_data(slot_info(5), Processed);
            ti.initilize_or_update_account(create_account_data_with_data(
                5,
                ak1,
                pk,
                Vec::from("abc"),
            ));
            ti.initilize_or_update_account(create_account_data_with_data(
                5,
                ak2,
                pk,
                Vec::from("abcdef"),
            ));

            let mut result = ti
                .get_program_accounts(
                    pk,
                    Some(vec![
                        AccountFilterType::Datasize(6),
                        AccountFilterType::Memcmp(MemcmpFilter::new(
                            1,
                            MemcmpFilterData::Bytes(Vec::from("bcdef")),
                        )),
                    ]),
                    Processed,
                )
                .unwrap();

            assert_eq!(result.len(), 1);
            let result = result.pop().unwrap();
            assert_eq!(result.pubkey, ak2);
            assert_eq!(result.updated_slot, 5);
            assert_eq!(result.account.data.data(), Vec::from("abcdef"));
        }

        #[test]
        fn contradicting_filter() {
            let ti = AccountsDb::new_for_testing();

            let pk = Pubkey::from_str("HZGMUF6kdCUK6nuc3TdNR6X5HNdGtg5HmVQ8cV2pRiHE").unwrap();
            let ak1 = Pubkey::from_str("6rRiMihF7UdJz25t5QvS7PgP9yzfubN7TBRv26ZBVAhE").unwrap();
            let ak2 = Pubkey::from_str("5VsPdDtqyFw6BmxrTZXKfnTLZy3TgzVA2MA1vZKAfddw").unwrap();

            ti.process_slot_data(slot_info(5), Processed);
            ti.initilize_or_update_account(create_account_data_with_data(
                5,
                ak1,
                pk,
                Vec::from("abc"),
            ));
            ti.initilize_or_update_account(create_account_data_with_data(
                5,
                ak2,
                pk,
                Vec::from("abcdef"),
            ));

            let result = ti.get_program_accounts(
                pk,
                Some(vec![
                    AccountFilterType::Memcmp(MemcmpFilter::new(
                        0,
                        MemcmpFilterData::Bytes(Vec::from("a")),
                    )),
                    AccountFilterType::Memcmp(MemcmpFilter::new(
                        0,
                        MemcmpFilterData::Bytes(Vec::from("b")),
                    )),
                ]),
                Processed,
            );
            assert_eq!(result, Ok(Vec::new()));
        }
    }

    mod process_slot_data {
        use std::str::FromStr;

        use solana_sdk::pubkey::Pubkey;

        use lite_account_manager_common::account_store_interface::AccountStorageInterface;
        use lite_account_manager_common::commitment::Commitment::{
            Confirmed, Finalized, Processed,
        };

        use crate::accountsdb::accounts_db::{create_account_data, slot_info};
        use crate::accountsdb::AccountsDb;

        #[test]
        fn first_time_invocation() {
            let ti = AccountsDb::new_for_testing();

            ti.process_slot_data(slot_info(3), Processed);
            ti.process_slot_data(slot_info(2), Confirmed);
            ti.process_slot_data(slot_info(1), Finalized);

            let (processed, confirmed, finalized) = ti.slot_state.get();
            assert_eq!(processed, 3);
            assert_eq!(confirmed, 2);
            assert_eq!(finalized, 1);
        }

        #[test]
        fn only_updates_processed_slot() {
            let ti = AccountsDb::new_for_testing();

            ti.process_slot_data(slot_info(1), Processed);
            ti.process_slot_data(slot_info(1), Confirmed);
            ti.process_slot_data(slot_info(1), Finalized);

            ti.process_slot_data(slot_info(2), Processed);

            let (processed, confirmed, finalized) = ti.slot_state.get();
            assert_eq!(processed, 2);
            assert_eq!(confirmed, 1);
            assert_eq!(finalized, 1);
        }

        #[test]
        fn update_processed_slot_when_confirmed_slot_is_ahead() {
            let ti = AccountsDb::new_for_testing();

            ti.process_slot_data(slot_info(1), Processed);
            ti.process_slot_data(slot_info(1), Confirmed);
            ti.process_slot_data(slot_info(1), Finalized);

            ti.process_slot_data(slot_info(2), Confirmed);

            let (processed, confirmed, finalized) = ti.slot_state.get();
            assert_eq!(processed, 2);
            assert_eq!(confirmed, 2);
            assert_eq!(finalized, 1);
        }

        #[test]
        fn update_processed_and_confirmed_slot_when_finalized_slot_is_ahead() {
            let ti = AccountsDb::new_for_testing();

            ti.process_slot_data(slot_info(1), Processed);
            ti.process_slot_data(slot_info(1), Confirmed);
            ti.process_slot_data(slot_info(1), Finalized);

            ti.process_slot_data(slot_info(2), Finalized);

            let (processed, confirmed, finalized) = ti.slot_state.get();
            assert_eq!(processed, 2);
            assert_eq!(confirmed, 2);
            assert_eq!(finalized, 2);
        }

        #[test]
        fn returns_updated_account_if_commitment_changes_to_finalized() {
            let ti = AccountsDb::new_for_testing();

            let pk = Pubkey::from_str("HZGMUF6kdCUK6nuc3TdNR6X5HNdGtg5HmVQ8cV2pRiHE").unwrap();
            let ak = Pubkey::from_str("6rRiMihF7UdJz25t5QvS7PgP9yzfubN7TBRv26ZBVAhE").unwrap();

            let result = ti.process_slot_data(slot_info(3), Processed);
            assert_eq!(result.len(), 0);

            ti.initilize_or_update_account(create_account_data(3, ak, pk, 10));

            let result = ti.process_slot_data(slot_info(3), Confirmed);
            assert_eq!(result.len(), 0);

            let result = ti.process_slot_data(slot_info(3), Finalized);
            assert_eq!(result.len(), 1)
        }

        #[test]
        fn does_not_return_updated_account_if_different_slot_gets_finalized() {
            let ti = AccountsDb::new_for_testing();

            let pk = Pubkey::from_str("HZGMUF6kdCUK6nuc3TdNR6X5HNdGtg5HmVQ8cV2pRiHE").unwrap();
            let ak = Pubkey::from_str("6rRiMihF7UdJz25t5QvS7PgP9yzfubN7TBRv26ZBVAhE").unwrap();

            ti.initilize_or_update_account(create_account_data(3, ak, pk, 10));
            ti.process_slot_data(slot_info(3), Finalized);

            let result = ti.process_slot_data(slot_info(4), Finalized);
            assert_eq!(result.len(), 0)
        }
    }
}

pub fn slot_info(slot: Slot) -> SlotInfo {
    SlotInfo {
        slot,
        parent: Slot::from(1u64),
        root: Slot::from(0u64),
    }
}

pub fn create_account_data(
    updated_slot: Slot,
    pubkey: Pubkey,
    program: Pubkey,
    lamports: u64,
) -> AccountData {
    AccountData {
        pubkey,
        account: Arc::new(Account {
            lamports,
            data: Data::Uncompressed(Vec::from([])),
            owner: program,
            executable: false,
            rent_epoch: 0,
        }),
        updated_slot,
        write_version: 0,
    }
}

pub fn create_account_data_with_data(
    updated_slot: Slot,
    ak: Pubkey,
    program: Pubkey,
    data: Vec<u8>,
) -> AccountData {
    AccountData {
        pubkey: ak,
        account: Arc::new(Account {
            lamports: 1,
            data: Data::Uncompressed(data),
            owner: program,
            executable: false,
            rent_epoch: 0,
        }),
        updated_slot,
        write_version: 0,
    }
}
