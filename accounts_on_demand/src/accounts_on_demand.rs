use std::{
    collections::{BTreeMap, HashMap, HashSet},
    sync::Arc,
    time::Duration,
};

use async_trait::async_trait;
use futures::lock::Mutex;
use lite_account_manager_common::{
    account_data::AccountData,
    account_filter::{AccountFilter, AccountFilterType},
    account_filters_interface::AccountFiltersStoreInterface,
    account_store_interface::{AccountLoadingError, AccountStorageInterface},
    accounts_source_interface::AccountsSourceInterface,
    commitment::Commitment,
    mutable_filter_store::MutableFilterStore,
    slot_info::SlotInfo,
};
use prometheus::{opts, register_int_gauge, IntGauge};
use solana_sdk::pubkey::Pubkey;
use tokio::sync::Notify;

lazy_static::lazy_static! {
    static ref NUMBER_OF_ACCOUNTS_ON_DEMAND: IntGauge =
       register_int_gauge!(opts!("lite_account_manager_number_of_accounts_on_demand", "Number of accounts on demand")).unwrap();

    static ref NUMBER_OF_PROGRAM_FILTERS_ON_DEMAND: IntGauge =
        register_int_gauge!(opts!("lite_account_manager_number_of_program_filters_on_demand", "Number of program filters on demand")).unwrap();
}

type GpaAccountKey = (Pubkey, Vec<AccountFilterType>);

pub struct AccountsOnDemand {
    accounts_source: Arc<dyn AccountsSourceInterface>,
    mutable_filters: Arc<MutableFilterStore>,
    accounts_storage: Arc<dyn AccountStorageInterface>,
    accounts_in_loading: Arc<Mutex<HashMap<Pubkey, Arc<Notify>>>>,
    gpa_in_loading: Arc<Mutex<BTreeMap<GpaAccountKey, Arc<Notify>>>>,
}

impl AccountsOnDemand {
    pub fn new(
        accounts_source: Arc<dyn AccountsSourceInterface>,
        mutable_filters: Arc<MutableFilterStore>,
        accounts_storage: Arc<dyn AccountStorageInterface>,
    ) -> Self {
        Self {
            accounts_source,
            mutable_filters,
            accounts_storage: accounts_storage.clone(),
            accounts_in_loading: Arc::new(Mutex::new(HashMap::new())),
            gpa_in_loading: Arc::new(Mutex::new(BTreeMap::new())),
        }
    }
}

#[async_trait]
impl AccountStorageInterface for AccountsOnDemand {
    async fn update_account(&self, account_data: AccountData, commitment: Commitment) -> bool {
        self.accounts_storage
            .update_account(account_data, commitment)
            .await
    }

    async fn initilize_or_update_account(&self, account_data: AccountData) {
        self.accounts_storage
            .initilize_or_update_account(account_data)
            .await
    }

    async fn get_account(
        &self,
        account_pk: Pubkey,
        commitment: Commitment,
    ) -> Result<Option<AccountData>, AccountLoadingError> {
        match self
            .accounts_storage
            .get_account(account_pk, commitment)
            .await?
        {
            Some(account_data) => Ok(Some(account_data)),
            None => {
                // account does not exist in account store
                // first check if we have already subscribed to the required account
                // This is to avoid resetting geyser subscription because of accounts that do not exists.
                let mut lk = self.accounts_in_loading.lock().await;
                match lk.get(&account_pk).cloned() {
                    Some(loading_account) => {
                        drop(lk);
                        match tokio::time::timeout(
                            Duration::from_secs(10),
                            loading_account.notified(),
                        )
                        .await
                        {
                            Ok(_) => {
                                self.accounts_storage
                                    .get_account(account_pk, commitment)
                                    .await
                            }
                            Err(_timeout) => Err(AccountLoadingError::OperationTimeOut),
                        }
                    }
                    None => {
                        // account is not loading
                        if self.mutable_filters.contains_account(account_pk).await {
                            // account was already tried to be loaded but does not exists
                            Ok(None)
                        } else {
                            // update account loading map
                            // create a notify for accounts under loading
                            lk.insert(account_pk, Arc::new(Notify::new()));
                            log::debug!("subscribing to accounts update : {}", account_pk);
                            let mut hash_set = HashSet::new();
                            hash_set.insert(account_pk);
                            self.accounts_source
                                .subscribe_accounts(hash_set)
                                .await
                                .map_err(|_| AccountLoadingError::ErrorSubscribingAccount)?;
                            drop(lk);

                            // save snapshot
                            let account_filter = vec![AccountFilter {
                                accounts: vec![account_pk],
                                program_id: None,
                                filters: None,
                            }];
                            self.accounts_source
                                .save_snapshot(self.accounts_storage.clone(), account_filter)
                                .await
                                .map_err(|_| AccountLoadingError::ErrorCreatingSnapshot)?;
                            // update loading lock
                            {
                                let mut write_lock = self.accounts_in_loading.lock().await;
                                let notify = write_lock.remove(&account_pk);
                                drop(write_lock);
                                if let Some(notify) = notify {
                                    notify.notify_waiters();
                                }
                            }
                            self.accounts_storage
                                .get_account(account_pk, commitment)
                                .await
                        }
                    }
                }
            }
        }
    }

    async fn get_program_accounts(
        &self,
        program_id: Pubkey,
        filters: Option<Vec<AccountFilterType>>,
        commitment: Commitment,
    ) -> Result<Vec<AccountData>, AccountLoadingError> {
        let account_filter = AccountFilter {
            accounts: vec![],
            program_id: Some(program_id),
            filters: filters.clone(),
        };
        // accounts on demand will fetch gPA if they do not exist
        // it will first compare with existing filters and do the necessary if needed
        if self.mutable_filters.contains_filter(&account_filter).await {
            self.accounts_storage
                .get_program_accounts(program_id, filters.clone(), commitment)
                .await
        } else {
            // subsribing to new gpa accounts
            let mut lk = self.gpa_in_loading.lock().await;
            match lk
                .get(&(program_id, filters.clone().unwrap_or_default()))
                .cloned()
            {
                Some(loading_account) => {
                    drop(lk);
                    match tokio::time::timeout(Duration::from_secs(10), loading_account.notified())
                        .await
                    {
                        Ok(_) => {
                            self.accounts_storage
                                .get_program_accounts(program_id, filters.clone(), commitment)
                                .await
                        }
                        Err(_timeout) => {
                            // todo replace with error
                            log::error!("gPA on program : {}", program_id.to_string());
                            Err(AccountLoadingError::OperationTimeOut)
                        }
                    }
                }
                None => {
                    // update account loading map
                    // create a notify for accounts under loading
                    lk.insert(
                        (program_id, filters.clone().unwrap_or_default()),
                        Arc::new(Notify::new()),
                    );
                    self.accounts_source
                        .subscribe_program_accounts(program_id, filters.clone())
                        .await
                        .map_err(|_| AccountLoadingError::ErrorSubscribingAccount)?;
                    drop(lk);
                    self.mutable_filters
                        .add_account_filters(&vec![account_filter.clone()])
                        .await;

                    self.accounts_source
                        .save_snapshot(self.accounts_storage.clone(), vec![account_filter])
                        .await
                        .map_err(|_| AccountLoadingError::ErrorCreatingSnapshot)?;
                    // update loading lock
                    {
                        let mut write_lock = self.gpa_in_loading.lock().await;
                        let notify =
                            write_lock.remove(&(program_id, filters.clone().unwrap_or_default()));
                        drop(write_lock);
                        if let Some(notify) = notify {
                            notify.notify_waiters();
                        }
                    }
                    self.accounts_storage
                        .get_program_accounts(program_id, filters, commitment)
                        .await
                }
            }
        }
    }

    async fn process_slot_data(
        &self,
        slot_info: SlotInfo,
        commitment: Commitment,
    ) -> Vec<AccountData> {
        self.accounts_storage
            .process_slot_data(slot_info, commitment)
            .await
    }

    async fn create_snapshot(&self, program_id: Pubkey) -> Result<Vec<u8>, AccountLoadingError> {
        self.accounts_storage.create_snapshot(program_id).await
    }

    async fn load_from_snapshot(
        &self,
        program_id: Pubkey,
        snapshot: Vec<u8>,
    ) -> Result<(), AccountLoadingError> {
        self.load_from_snapshot(program_id, snapshot).await
    }
}
