use std::collections::BTreeMap;
use std::sync::RwLock;
use std::{
    collections::{BTreeSet, HashMap},
    sync::{Arc, Mutex, RwLockReadGuard},
};

use dashmap::DashMap;
use prometheus::{opts, register_int_gauge, IntGauge};
use solana_sdk::{pubkey::Pubkey, slot_history::Slot};

use lite_account_manager_common::{
    account_data::AccountData,
    account_filter::{AccountFilter, AccountFilterType},
    account_filters_interface::AccountFiltersStoreInterface,
    account_store_interface::{AccountLoadingError, AccountStorageInterface},
    commitment::Commitment,
    slot_info::SlotInfo,
};

use crate::account_data_by_commitment::AccountDataByCommitment;

lazy_static::lazy_static! {
    static ref ACCOUNT_STORED_IN_MEMORY: IntGauge =
       register_int_gauge!(opts!("lite_accounts_accounts_in_memory", "Account InMemory")).unwrap();

    static ref TOTAL_PROCESSED_ACCOUNTS: IntGauge =
        register_int_gauge!(opts!("lite_accounts_total_processed_accounts_in_memory", "Account processed accounts InMemory")).unwrap();

    static ref SLOT_FOR_LATEST_ACCOUNT_UPDATE: IntGauge =
        register_int_gauge!(opts!("lite_accounts_slot_for_latest_account_update", "Slot of latest account update")).unwrap();
}

struct SlotStatus {
    pub commitment: Commitment,
    pub accounts_updated: BTreeSet<AccountIndex>,
    pub parent: Option<Slot>,
}

type AccountIndex = usize;

pub struct InmemoryAccountStore {
    pubkey_to_account_index: Arc<DashMap<Pubkey, AccountIndex>>,
    accounts_by_owner: Arc<DashMap<Pubkey, Arc<RwLock<BTreeSet<AccountIndex>>>>>,
    slots_status: Arc<Mutex<BTreeMap<Slot, SlotStatus>>>,
    filtered_accounts: Arc<dyn AccountFiltersStoreInterface>,
    accounts_store: RwLock<Vec<RwLock<AccountDataByCommitment>>>,
}

impl InmemoryAccountStore {
    pub fn new(filtered_accounts: Arc<dyn AccountFiltersStoreInterface>) -> Self {
        Self {
            pubkey_to_account_index: Arc::new(DashMap::new()),
            accounts_by_owner: Arc::new(DashMap::new()),
            slots_status: Arc::new(Mutex::new(BTreeMap::new())),
            filtered_accounts,
            accounts_store: RwLock::new(vec![]),
        }
    }

    fn add_account_owner(&self, account_index: AccountIndex, owner: Pubkey) {
        match self.accounts_by_owner.entry(owner) {
            dashmap::mapref::entry::Entry::Occupied(mut occ) => {
                occ.get_mut().write().unwrap().insert(account_index);
            }
            dashmap::mapref::entry::Entry::Vacant(vc) => {
                let mut set = BTreeSet::new();
                set.insert(account_index);
                vc.insert(Arc::new(RwLock::new(set)));
            }
        }
    }

    // here if the commitment is processed and the account has changed owner from A->B we keep the key for both A and B
    // then we remove the key from A for finalized commitment
    // returns true if entry needs to be deleted
    fn update_owner_delete_if_necessary(
        &self,
        prev_account_data: &AccountData,
        new_account_data: &AccountData,
        account_index: AccountIndex,
        commitment: Commitment,
    ) -> bool {
        assert_eq!(prev_account_data.pubkey, new_account_data.pubkey);
        if prev_account_data.account.owner != new_account_data.account.owner {
            if commitment == Commitment::Finalized {
                match self
                    .accounts_by_owner
                    .entry(prev_account_data.account.owner)
                {
                    dashmap::mapref::entry::Entry::Occupied(mut occ) => {
                        occ.get_mut().write().unwrap().remove(&account_index);
                    }
                    dashmap::mapref::entry::Entry::Vacant(_) => {
                        // do nothing
                    }
                }

                // account is deleted or if new does not satisfies the filter criterias
                if Self::is_deleted(new_account_data) || !self.satisfies_filters(new_account_data) {
                    return true;
                }
            }
            if !Self::is_deleted(new_account_data) && self.satisfies_filters(new_account_data) {
                // update owner if account was not deleted but owner was change and the filter criterias are satisfied
                self.add_account_owner(account_index, new_account_data.account.owner);
            }
        }
        false
    }

    // commitment for which status was updated
    fn add_account_index_to_slot_status(
        &self,
        slot: Slot,
        account_index: AccountIndex,
        commitment: Commitment,
    ) -> Commitment {
        let mut lk = self.slots_status.lock().unwrap();
        let slot_status = match lk.get_mut(&slot) {
            Some(x) => x,
            None => {
                lk.insert(
                    slot,
                    SlotStatus {
                        commitment,
                        accounts_updated: BTreeSet::new(),
                        parent: None,
                    },
                );
                lk.get_mut(&slot).unwrap()
            }
        };
        match commitment {
            Commitment::Processed | Commitment::Confirmed => {
                // insert account into slot status
                slot_status.accounts_updated.insert(account_index);
                slot_status.commitment
            }
            Commitment::Finalized => commitment,
        }
    }

    fn maybe_update_slot_status(
        &self,
        account_data: &AccountData,
        commitment: Commitment,
    ) -> Commitment {
        let account_index = match self
            .pubkey_to_account_index
            .get(&account_data.pubkey)
            .map(|x| *x)
        {
            Some(x) => x,
            None => {
                return commitment;
            }
        };
        let slot = account_data.updated_slot;
        self.add_account_index_to_slot_status(slot, account_index, commitment)
    }

    pub fn satisfies_filters(&self, account: &AccountData) -> bool {
        self.filtered_accounts.satisfies(account)
    }

    pub fn is_deleted(account: &AccountData) -> bool {
        account.account.lamports == 0
    }

    pub fn account_store_contains_key(&self, pubkey: &Pubkey) -> bool {
        self.pubkey_to_account_index.contains_key(pubkey)
    }

    fn get_account_with_filtering_from_lock(
        lk_on_account: RwLockReadGuard<'_, AccountDataByCommitment>,
        return_vec: &mut Vec<AccountData>,
        commitment: Commitment,
        program_pubkey: Pubkey,
        account_filters: &Option<Vec<AccountFilterType>>,
    ) {
        match &account_filters {
            Some(account_filters) => {
                if let Some(account_data) =
                    lk_on_account.get_account_data_filtered(commitment, account_filters)
                {
                    if account_data.account.lamports > 0
                        && account_data.account.owner == program_pubkey
                    {
                        return_vec.push(account_data);
                    }
                };
            }
            None => {
                let account_data = lk_on_account.get_account_data(commitment);
                if let Some(account_data) = account_data {
                    // recheck owner
                    if program_pubkey == account_data.account.owner {
                        return_vec.push(account_data);
                    }
                }
            }
        }
    }
}

impl AccountStorageInterface for InmemoryAccountStore {
    fn update_account(&self, account_data: AccountData, commitment: Commitment) -> bool {
        SLOT_FOR_LATEST_ACCOUNT_UPDATE.set(account_data.updated_slot as i64);

        // account is neither deleted, nor tracked, not satifying any filters
        if !Self::is_deleted(&account_data)
            && !self.satisfies_filters(&account_data)
            && !self.account_store_contains_key(&account_data.pubkey)
        {
            return false;
        }
        let updated_slot = account_data.updated_slot;

        // check if the blockhash and slot is already confirmed
        let commitment = self.maybe_update_slot_status(&account_data, commitment);

        let updated = match self.pubkey_to_account_index.entry(account_data.pubkey) {
            dashmap::mapref::entry::Entry::Occupied(occ) => {
                let index = *occ.get();
                let read_lk = self.accounts_store.read().unwrap();
                let mut acc_by_commitment = read_lk[index].write().unwrap();
                let prev_account = acc_by_commitment.get_account_data(commitment);
                // if account has been updated
                if acc_by_commitment.update(account_data.clone(), commitment) {
                    if let Some(prev_account) = prev_account {
                        if self.update_owner_delete_if_necessary(
                            &prev_account,
                            &account_data,
                            index,
                            commitment,
                        ) {
                            acc_by_commitment.delete();
                            occ.remove_entry();
                        }
                    }
                    true
                } else {
                    false
                }
            }
            dashmap::mapref::entry::Entry::Vacant(vac) => {
                if self.satisfies_filters(&account_data) {
                    ACCOUNT_STORED_IN_MEMORY.inc();
                    let mut lk = self.accounts_store.write().unwrap();
                    lk.push(RwLock::new(AccountDataByCommitment::new(
                        account_data.clone(),
                        commitment,
                    )));
                    let index = lk.len() - 1;
                    drop(lk);
                    self.add_account_owner(index, account_data.account.owner);
                    self.add_account_index_to_slot_status(updated_slot, index, commitment);
                    vac.insert(index);
                    true
                } else {
                    false
                }
            }
        };
        updated
    }

    fn initialize_or_update_account(&self, account_data: AccountData) {
        println!("{}", account_data.pubkey);
        self.update_account(account_data, Commitment::Finalized);
    }

    fn get_account(
        &self,
        account_pk: Pubkey,
        commitment: Commitment,
    ) -> Result<Option<AccountData>, AccountLoadingError> {
        let account_index = self.pubkey_to_account_index.get(&account_pk).map(|x| *x);
        let Some(account_index) = account_index else {
            return Ok(None);
        };
        let account = self.accounts_store.read().unwrap()[account_index]
            .read()
            .unwrap()
            .get_account_data(commitment);
        if let Some(account_data) = &account {
            if account_data.account.lamports > 0 {
                Ok(account)
            } else {
                Ok(None)
            }
        } else {
            Ok(None)
        }
    }

    fn get_program_accounts(
        &self,
        program_pubkey: Pubkey,
        account_filters: Option<Vec<AccountFilterType>>,
        commitment: Commitment,
    ) -> Result<Vec<AccountData>, AccountLoadingError> {
        if !self.filtered_accounts.contains_filter(&AccountFilter {
            accounts: vec![],
            program_id: Some(program_pubkey),
            filters: account_filters.clone(),
        }) {
            return Err(AccountLoadingError::ConfigDoesNotContainRequiredFilters);
        }
        let lk = match self.accounts_by_owner.entry(program_pubkey) {
            dashmap::mapref::entry::Entry::Occupied(occ) => occ.get().clone(),
            dashmap::mapref::entry::Entry::Vacant(_) => {
                return Ok(vec![]);
            }
        };

        let mut return_vec = vec![];
        let store_read_lk = self.accounts_store.read().unwrap();
        let mut retry_list = Vec::with_capacity(128);
        // optimization to avoid locking for each account
        for program_account_index in lk.read().unwrap().iter() {
            let lk_on_account = match store_read_lk[*program_account_index].try_read() {
                Ok(lk) => lk,
                Err(_) => {
                    retry_list.push(*program_account_index);
                    continue;
                }
            };
            Self::get_account_with_filtering_from_lock(
                lk_on_account,
                &mut return_vec,
                commitment,
                program_pubkey,
                &account_filters,
            );
        }
        // retry for the accounts which were locked
        for retry_account_index in retry_list {
            let lk_on_account = store_read_lk[retry_account_index].read().unwrap();
            Self::get_account_with_filtering_from_lock(
                lk_on_account,
                &mut return_vec,
                commitment,
                program_pubkey,
                &account_filters,
            );
        }

        Ok(return_vec)
    }

    fn process_slot_data(&self, slot_info: SlotInfo, commitment: Commitment) -> Vec<AccountData> {
        let slot: u64 = slot_info.slot;
        let writable_accounts = {
            let mut lk = self.slots_status.lock().unwrap();
            let mut current_slot = Some((slot, Some(slot_info.parent)));
            let mut writable_accounts: HashMap<AccountIndex, Slot> = HashMap::new();
            while let Some((slot, parent)) = current_slot {
                current_slot = None;
                match lk.get_mut(&slot) {
                    Some(status) => {
                        if status.commitment == commitment {
                            // slot already processed for the commitment
                            break;
                        }
                        status.commitment = commitment;
                        if parent.is_some() {
                            status.parent = parent;
                        }
                        for account in &status.accounts_updated {
                            match writable_accounts.entry(*account) {
                                std::collections::hash_map::Entry::Occupied(_) => {
                                    // do nothing account has already been updated by higher slot
                                }
                                std::collections::hash_map::Entry::Vacant(vac) => {
                                    vac.insert(slot);
                                }
                            }
                        }
                        if commitment == Commitment::Confirmed
                            || commitment == Commitment::Finalized
                        {
                            if let Some(parent_slot) = status.parent {
                                // update commitment for parent slots
                                current_slot = Some((parent_slot, None));
                            }
                        }
                    }
                    None => {
                        if commitment == Commitment::Confirmed {
                            log::debug!(
                                "slot status not found for {} and commitment {}, confirmed lagging",
                                slot,
                                commitment
                            );
                        } else if commitment == Commitment::Finalized {
                            log::debug!("slot status not found for {} for commitment finalized, could be normal if the entry is already removed or during startup", slot,);
                        }
                        if commitment == Commitment::Confirmed
                            || commitment == Commitment::Processed
                        {
                            // add status for processed slot
                            let status = SlotStatus {
                                commitment,
                                accounts_updated: BTreeSet::new(),
                                parent: Some(slot_info.parent),
                            };
                            lk.insert(slot, status);
                        }
                        break;
                    }
                }
            }

            // remove old slot status if finalized
            if commitment == Commitment::Finalized {
                while let Some(entry) = lk.first_entry() {
                    if *entry.key() < slot {
                        log::debug!("removing slot {}", entry.key());
                        entry.remove();
                    } else {
                        break;
                    }
                }
            }

            writable_accounts
        };

        let mut updated_accounts = vec![];

        let lk = self.accounts_store.read().unwrap();
        for (writable_account_index, update_slot) in writable_accounts {
            let mut writable_lk = lk[writable_account_index].write().unwrap();
            if let Some((account_data, prev_account_data)) =
                writable_lk.promote_slot_commitment(update_slot, commitment)
            {
                if let Some(prev_account_data) = prev_account_data {
                    // check if owner has changed
                    if (prev_account_data.account.owner != account_data.account.owner
                        || account_data.account.lamports == 0)
                        && self.update_owner_delete_if_necessary(
                            &prev_account_data,
                            &account_data,
                            writable_account_index,
                            commitment,
                        )
                    {
                        self.pubkey_to_account_index.remove(&account_data.pubkey);
                        writable_lk.delete();
                    }
                }
                // account has been confirmed first time
                updated_accounts.push(account_data);
            }
        }
        updated_accounts
    }

    fn create_snapshot(&self, program_id: Pubkey) -> Result<Vec<u8>, AccountLoadingError> {
        let accounts = self.get_program_accounts(program_id, None, Commitment::Finalized)?;
        Ok(bincode::serialize(&accounts).unwrap())
    }

    fn load_from_snapshot(
        &self,
        _program_id: Pubkey,
        snapshot: Vec<u8>,
    ) -> Result<(), AccountLoadingError> {
        let accounts = bincode::deserialize::<Vec<AccountData>>(&snapshot)
            .map_err(|_| AccountLoadingError::DeserializationIssues)?;
        for account_data in accounts {
            self.initialize_or_update_account(account_data);
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::{collections::HashSet, sync::Arc};

    use base64::Engine;
    use itertools::Itertools;
    use rand::{rngs::ThreadRng, Rng};
    use solana_sdk::{account::Account as SolanaAccount, pubkey::Pubkey, slot_history::Slot};

    use lite_account_manager_common::{
        account_data::{Account, AccountData, CompressionMethod},
        account_filter::{
            AccountFilter, AccountFilterType, AccountFilters, MemcmpFilter, MemcmpFilterData,
        },
        account_filters_interface::AccountFiltersStoreInterface,
        account_store_interface::{AccountLoadingError, AccountStorageInterface},
        commitment::Commitment,
        simple_filter_store::SimpleFilterStore,
        slot_info::SlotInfo,
    };

    use crate::inmemory_account_store::InmemoryAccountStore;

    fn create_random_account(
        rng: &mut ThreadRng,
        updated_slot: Slot,
        pubkey: Pubkey,
        program: Pubkey,
    ) -> AccountData {
        let length: usize = rng.gen_range(100..1000);
        let sol_account = SolanaAccount {
            lamports: rng.gen(),
            data: (0..length).map(|_| rng.gen::<u8>()).collect_vec(),
            owner: program,
            executable: false,
            rent_epoch: 0,
        };
        AccountData {
            pubkey,
            account: Arc::new(Account::from_solana_account(
                sol_account,
                CompressionMethod::None,
            )),
            updated_slot,
            write_version: 0,
        }
    }

    fn create_random_account_with_data(
        rng: &mut ThreadRng,
        updated_slot: Slot,
        pubkey: Pubkey,
        program: Pubkey,
        data: Vec<u8>,
        compression_mode: CompressionMethod,
    ) -> AccountData {
        let sol_account = SolanaAccount {
            lamports: rng.gen(),
            data,
            owner: program,
            executable: false,
            rent_epoch: 0,
        };
        AccountData {
            pubkey,
            account: Arc::new(Account::from_solana_account(sol_account, compression_mode)),
            updated_slot,
            write_version: 0,
        }
    }

    fn create_random_account_with_write_version(
        rng: &mut ThreadRng,
        updated_slot: Slot,
        pubkey: Pubkey,
        program: Pubkey,
        write_version: u64,
    ) -> AccountData {
        let mut acc = create_random_account(rng, updated_slot, pubkey, program);
        acc.write_version = write_version;
        acc
    }

    pub fn new_filter_store(
        account_filters: AccountFilters,
    ) -> Arc<dyn AccountFiltersStoreInterface> {
        let mut simple_store = SimpleFilterStore::default();
        simple_store.add_account_filters(&account_filters);
        Arc::new(simple_store)
    }

    pub fn new_slot_info(slot: u64) -> SlotInfo {
        SlotInfo {
            slot,
            parent: slot.saturating_sub(1),
            root: 0,
        }
    }

    #[test]
    pub fn test_account_store() {
        let program = Pubkey::new_unique();
        let filter_store = new_filter_store(vec![AccountFilter {
            program_id: Some(program),
            accounts: vec![],
            filters: None,
        }]);
        let store = InmemoryAccountStore::new(filter_store);
        let mut rng = rand::thread_rng();
        let pk1 = Pubkey::new_unique();
        let pk2 = Pubkey::new_unique();

        let account_data_0 = create_random_account(&mut rng, 0, pk1, program);
        store.initialize_or_update_account(account_data_0.clone());

        let account_data_1 = create_random_account(&mut rng, 0, pk2, program);

        let mut pubkeys = HashSet::new();
        pubkeys.insert(pk1);
        pubkeys.insert(pk2);

        store.initialize_or_update_account(account_data_1.clone());

        assert_eq!(
            store.get_account(pk1, Commitment::Processed),
            Ok(Some(account_data_0.clone()))
        );
        assert_eq!(
            store.get_account(pk1, Commitment::Confirmed),
            Ok(Some(account_data_0.clone()))
        );
        assert_eq!(
            store.get_account(pk1, Commitment::Finalized),
            Ok(Some(account_data_0.clone()))
        );

        assert_eq!(
            store.get_account(pk2, Commitment::Processed),
            Ok(Some(account_data_1.clone()))
        );
        assert_eq!(
            store.get_account(pk2, Commitment::Confirmed),
            Ok(Some(account_data_1.clone()))
        );
        assert_eq!(
            store.get_account(pk2, Commitment::Finalized),
            Ok(Some(account_data_1.clone()))
        );

        let account_data_2 = create_random_account(&mut rng, 1, pk1, program);
        let account_data_3 = create_random_account(&mut rng, 2, pk1, program);
        let account_data_4 = create_random_account(&mut rng, 3, pk1, program);
        let account_data_5 = create_random_account(&mut rng, 4, pk1, program);

        store.update_account(account_data_2.clone(), Commitment::Processed);
        store.update_account(account_data_3.clone(), Commitment::Processed);
        store.update_account(account_data_4.clone(), Commitment::Processed);
        store.update_account(account_data_5.clone(), Commitment::Processed);

        assert_eq!(
            store.get_account(pk1, Commitment::Processed),
            Ok(Some(account_data_5.clone()))
        );
        assert_eq!(
            store.get_account(pk1, Commitment::Confirmed),
            Ok(Some(account_data_0.clone()))
        );
        assert_eq!(
            store.get_account(pk1, Commitment::Finalized),
            Ok(Some(account_data_0.clone()))
        );

        store.process_slot_data(new_slot_info(1), Commitment::Confirmed);

        assert_eq!(
            store.get_account(pk1, Commitment::Processed),
            Ok(Some(account_data_5.clone()))
        );
        assert_eq!(
            store.get_account(pk1, Commitment::Confirmed),
            Ok(Some(account_data_2.clone()))
        );
        assert_eq!(
            store.get_account(pk1, Commitment::Finalized),
            Ok(Some(account_data_0.clone()))
        );

        store.process_slot_data(new_slot_info(2), Commitment::Confirmed);

        assert_eq!(
            store.get_account(pk1, Commitment::Processed),
            Ok(Some(account_data_5.clone()))
        );
        assert_eq!(
            store.get_account(pk1, Commitment::Confirmed),
            Ok(Some(account_data_3.clone()))
        );
        assert_eq!(
            store.get_account(pk1, Commitment::Finalized),
            Ok(Some(account_data_0.clone()))
        );

        store.process_slot_data(new_slot_info(1), Commitment::Finalized);

        assert_eq!(
            store.get_account(pk1, Commitment::Processed),
            Ok(Some(account_data_5.clone()))
        );
        assert_eq!(
            store.get_account(pk1, Commitment::Confirmed),
            Ok(Some(account_data_3.clone()))
        );
        assert_eq!(
            store.get_account(pk1, Commitment::Finalized),
            Ok(Some(account_data_2.clone()))
        );
    }

    #[test]
    pub fn test_account_store_if_finalized_clears_old_processed_slots() {
        let program = Pubkey::new_unique();
        let filter_store = new_filter_store(vec![AccountFilter {
            program_id: Some(program),
            accounts: vec![],
            filters: None,
        }]);
        let store = InmemoryAccountStore::new(filter_store);

        let pk1 = Pubkey::new_unique();

        let mut pubkeys = HashSet::new();
        pubkeys.insert(pk1);

        let mut rng = rand::thread_rng();

        store.initialize_or_update_account(create_random_account(&mut rng, 0, pk1, program));

        store.update_account(
            create_random_account(&mut rng, 1, pk1, program),
            Commitment::Processed,
        );
        store.update_account(
            create_random_account(&mut rng, 1, pk1, program),
            Commitment::Processed,
        );
        store.update_account(
            create_random_account(&mut rng, 2, pk1, program),
            Commitment::Processed,
        );
        store.update_account(
            create_random_account(&mut rng, 3, pk1, program),
            Commitment::Processed,
        );
        store.update_account(
            create_random_account(&mut rng, 4, pk1, program),
            Commitment::Processed,
        );
        store.update_account(
            create_random_account(&mut rng, 5, pk1, program),
            Commitment::Processed,
        );
        store.update_account(
            create_random_account(&mut rng, 6, pk1, program),
            Commitment::Processed,
        );
        store.update_account(
            create_random_account(&mut rng, 7, pk1, program),
            Commitment::Processed,
        );

        let account_8 = create_random_account(&mut rng, 8, pk1, program);
        store.update_account(account_8.clone(), Commitment::Processed);
        store.update_account(
            create_random_account(&mut rng, 9, pk1, program),
            Commitment::Processed,
        );
        store.update_account(
            create_random_account(&mut rng, 10, pk1, program),
            Commitment::Processed,
        );

        let last_account = create_random_account(&mut rng, 11, pk1, program);
        store.update_account(last_account.clone(), Commitment::Processed);

        let acc_index = store.pubkey_to_account_index.get(&pk1).unwrap();
        assert_eq!(
            store.accounts_store.read().unwrap()[*acc_index]
                .read()
                .unwrap()
                .processed_accounts
                .len(),
            12
        );
        store.process_slot_data(new_slot_info(11), Commitment::Finalized);
        assert_eq!(
            store.accounts_store.read().unwrap()[*acc_index]
                .read()
                .unwrap()
                .processed_accounts
                .len(),
            1
        );

        assert_eq!(
            store.get_account(pk1, Commitment::Finalized),
            Ok(Some(last_account.clone())),
        );

        // check finalizing previous commitment does not affect
        store.process_slot_data(new_slot_info(8), Commitment::Finalized);

        assert_eq!(
            store.get_account(pk1, Commitment::Finalized),
            Ok(Some(last_account)),
        );
    }

    #[test]
    pub fn test_get_program_account() {
        let prog_1 = Pubkey::new_unique();
        let prog_2 = Pubkey::new_unique();
        let prog_3 = Pubkey::new_unique();
        let prog_4 = Pubkey::new_unique();
        let filter_store = new_filter_store(vec![
            AccountFilter {
                program_id: Some(prog_1),
                accounts: vec![],
                filters: None,
            },
            AccountFilter {
                program_id: Some(prog_2),
                accounts: vec![],
                filters: None,
            },
            AccountFilter {
                program_id: Some(prog_3),
                accounts: vec![],
                filters: None,
            },
            AccountFilter {
                program_id: Some(prog_4),
                accounts: vec![],
                filters: None,
            },
        ]);

        let store = InmemoryAccountStore::new(filter_store);

        let mut rng = rand::thread_rng();

        let pks = (0..5).map(|_| Pubkey::new_unique()).collect_vec();

        store.update_account(
            create_random_account(&mut rng, 1, pks[0], prog_1),
            Commitment::Confirmed,
        );
        store.update_account(
            create_random_account(&mut rng, 1, pks[1], prog_1),
            Commitment::Confirmed,
        );
        store.update_account(
            create_random_account(&mut rng, 1, pks[2], prog_1),
            Commitment::Confirmed,
        );
        store.update_account(
            create_random_account(&mut rng, 1, pks[3], prog_1),
            Commitment::Confirmed,
        );

        store.update_account(
            create_random_account(&mut rng, 1, pks[4], prog_2),
            Commitment::Confirmed,
        );

        let acc_prgram_1 = store.get_program_accounts(prog_1, None, Commitment::Processed);
        assert!(acc_prgram_1.is_ok());
        assert_eq!(acc_prgram_1.unwrap().len(), 4);
        let acc_prgram_1 = store.get_program_accounts(prog_1, None, Commitment::Confirmed);
        assert!(acc_prgram_1.is_ok());
        assert_eq!(acc_prgram_1.unwrap().len(), 4);
        let acc_prgram_1 = store.get_program_accounts(prog_1, None, Commitment::Finalized);
        assert!(acc_prgram_1.is_ok());
        assert!(acc_prgram_1.unwrap().is_empty());

        let acc_prgram_2 = store.get_program_accounts(prog_2, None, Commitment::Processed);
        assert!(acc_prgram_2.is_ok());
        assert_eq!(acc_prgram_2.unwrap().len(), 1);
        let acc_prgram_2 = store.get_program_accounts(prog_2, None, Commitment::Confirmed);
        assert!(acc_prgram_2.is_ok());
        assert_eq!(acc_prgram_2.unwrap().len(), 1);
        let acc_prgram_2 = store.get_program_accounts(prog_2, None, Commitment::Finalized);
        assert!(acc_prgram_2.is_ok());
        assert!(acc_prgram_2.unwrap().is_empty());

        let acc_prgram_3 =
            store.get_program_accounts(Pubkey::new_unique(), None, Commitment::Processed);
        assert!(acc_prgram_3.is_err());
        let acc_prgram_3 =
            store.get_program_accounts(Pubkey::new_unique(), None, Commitment::Confirmed);
        assert!(acc_prgram_3.is_err());
        let acc_prgram_3 =
            store.get_program_accounts(Pubkey::new_unique(), None, Commitment::Finalized);
        assert!(acc_prgram_3.is_err());

        store.process_slot_data(new_slot_info(1), Commitment::Finalized);

        let acc_prgram_1 = store.get_program_accounts(prog_1, None, Commitment::Finalized);
        assert!(acc_prgram_1.is_ok());
        assert_eq!(acc_prgram_1.unwrap().len(), 4);
        let acc_prgram_2 = store.get_program_accounts(prog_2, None, Commitment::Finalized);
        assert!(acc_prgram_2.is_ok());
        assert_eq!(acc_prgram_2.unwrap().len(), 1);

        let pk = Pubkey::new_unique();

        let account_finalized = create_random_account(&mut rng, 2, pk, prog_3);
        store.update_account(account_finalized.clone(), Commitment::Finalized);
        store.process_slot_data(new_slot_info(2), Commitment::Finalized);

        let account_confirmed = create_random_account(&mut rng, 3, pk, prog_3);
        store.update_account(account_confirmed.clone(), Commitment::Confirmed);

        let account_processed = create_random_account(&mut rng, 4, pk, prog_4);
        store.update_account(account_processed.clone(), Commitment::Processed);

        let f = store.get_program_accounts(prog_3, None, Commitment::Finalized);

        let c = store.get_program_accounts(prog_3, None, Commitment::Confirmed);

        let p_3 = store.get_program_accounts(prog_3, None, Commitment::Processed);

        let p_4 = store.get_program_accounts(prog_4, None, Commitment::Processed);

        assert_eq!(c, Ok(vec![account_confirmed.clone()]));
        assert_eq!(p_3, Ok(vec![]));
        assert_eq!(p_4, Ok(vec![account_processed.clone()]));

        assert_eq!(f, Ok(vec![account_finalized.clone()]));

        store.process_slot_data(new_slot_info(3), Commitment::Finalized);
        store.process_slot_data(new_slot_info(4), Commitment::Confirmed);

        let f = store.get_program_accounts(prog_3, None, Commitment::Finalized);

        let p_3 = store.get_program_accounts(prog_3, None, Commitment::Confirmed);

        let p_4 = store.get_program_accounts(prog_4, None, Commitment::Confirmed);

        assert_eq!(f, Ok(vec![account_confirmed.clone()]));
        assert_eq!(p_3, Ok(vec![]));
        assert_eq!(p_4, Ok(vec![account_processed.clone()]));

        store.process_slot_data(new_slot_info(4), Commitment::Finalized);
        let p_3 = store.get_program_accounts(prog_3, None, Commitment::Finalized);

        let p_4 = store.get_program_accounts(prog_4, None, Commitment::Finalized);

        assert_eq!(p_3, Ok(vec![]));
        assert_eq!(p_4, Ok(vec![account_processed.clone()]));
    }

    #[test]
    pub fn test_get_program_account_2() {
        let prog_1 = Pubkey::new_unique();
        let filter_store = new_filter_store(vec![AccountFilter {
            program_id: Some(prog_1),
            accounts: vec![],
            filters: None,
        }]);

        let store = InmemoryAccountStore::new(filter_store);

        let mut rng = rand::thread_rng();
        let data_1 = (0..100).map(|_| rng.gen::<u8>()).collect_vec();
        let data_2 = (0..200).map(|_| rng.gen::<u8>()).collect_vec();
        let data_3 = (0..300).map(|_| rng.gen::<u8>()).collect_vec();
        let data_4 = (0..400).map(|_| rng.gen::<u8>()).collect_vec();
        let data_5 = (0..400).map(|_| rng.gen::<u8>()).collect_vec();

        let pk1_1 = Pubkey::new_unique();
        let pk1_2 = Pubkey::new_unique();
        let pk2_1 = Pubkey::new_unique();
        let pk2_2 = Pubkey::new_unique();
        let pk3_1 = Pubkey::new_unique();
        let pk3_2 = Pubkey::new_unique();
        let pk4_1 = Pubkey::new_unique();
        let pk4_2 = Pubkey::new_unique();
        let pk5_1 = Pubkey::new_unique();

        store.initialize_or_update_account(create_random_account_with_data(
            &mut rng,
            1,
            pk1_1,
            prog_1,
            data_1.clone(),
            CompressionMethod::Lz4(8),
        ));
        store.initialize_or_update_account(create_random_account_with_data(
            &mut rng,
            1,
            pk1_2,
            prog_1,
            data_1.clone(),
            CompressionMethod::None,
        ));
        store.initialize_or_update_account(create_random_account_with_data(
            &mut rng,
            1,
            pk2_1,
            prog_1,
            data_2.clone(),
            CompressionMethod::Lz4(3),
        ));
        store.initialize_or_update_account(create_random_account_with_data(
            &mut rng,
            1,
            pk2_2,
            prog_1,
            data_2.clone(),
            CompressionMethod::None,
        ));
        store.initialize_or_update_account(create_random_account_with_data(
            &mut rng,
            1,
            pk3_1,
            prog_1,
            data_3.clone(),
            CompressionMethod::Lz4(10),
        ));
        store.initialize_or_update_account(create_random_account_with_data(
            &mut rng,
            1,
            pk3_2,
            prog_1,
            data_3.clone(),
            CompressionMethod::Lz4(5),
        ));
        store.initialize_or_update_account(create_random_account_with_data(
            &mut rng,
            1,
            pk4_1,
            prog_1,
            data_4.clone(),
            CompressionMethod::Lz4(4),
        ));
        store.initialize_or_update_account(create_random_account_with_data(
            &mut rng,
            1,
            pk4_2,
            prog_1,
            data_4.clone(),
            CompressionMethod::Lz4(2),
        ));
        store.initialize_or_update_account(create_random_account_with_data(
            &mut rng,
            1,
            pk5_1,
            prog_1,
            data_5.clone(),
            CompressionMethod::Lz4(1),
        ));

        let gpa_1 = store
            .get_program_accounts(prog_1, None, Commitment::Finalized)
            .unwrap()
            .iter()
            .map(|x| x.pubkey)
            .collect::<HashSet<Pubkey>>();
        assert_eq!(
            gpa_1,
            vec![pk1_1, pk1_2, pk2_1, pk2_2, pk3_1, pk3_2, pk4_1, pk4_2, pk5_1]
                .iter()
                .cloned()
                .collect::<HashSet<Pubkey>>()
        );

        let gpa_acc_1 = [pk1_1, pk1_2].iter().cloned().collect::<HashSet<Pubkey>>();
        let gpa_2 = store
            .get_program_accounts(
                prog_1,
                Some(vec![AccountFilterType::DataSize(100)]),
                Commitment::Finalized,
            )
            .unwrap()
            .iter()
            .map(|x| x.pubkey)
            .collect::<HashSet<Pubkey>>();
        assert_eq!(gpa_2, gpa_acc_1);

        let gpa_2 = store
            .get_program_accounts(
                prog_1,
                Some(vec![AccountFilterType::Memcmp(MemcmpFilter {
                    offset: 0,
                    data: MemcmpFilterData::Bytes(data_1[0..30].to_vec()),
                })]),
                Commitment::Finalized,
            )
            .unwrap()
            .iter()
            .map(|x| x.pubkey)
            .collect::<HashSet<Pubkey>>();
        assert_eq!(gpa_2, gpa_acc_1);

        let base64_str = base64::engine::general_purpose::STANDARD.encode(&data_1[10..30]);
        let gpa_2 = store
            .get_program_accounts(
                prog_1,
                Some(vec![AccountFilterType::Memcmp(MemcmpFilter {
                    offset: 10,
                    data: MemcmpFilterData::Base64(base64_str),
                })]),
                Commitment::Finalized,
            )
            .unwrap()
            .iter()
            .map(|x| x.pubkey)
            .collect::<HashSet<Pubkey>>();
        assert_eq!(gpa_2, gpa_acc_1);

        let base64_str = bs58::encode(&data_1[20..60]).into_string();
        let gpa_2 = store
            .get_program_accounts(
                prog_1,
                Some(vec![AccountFilterType::Memcmp(MemcmpFilter {
                    offset: 20,
                    data: MemcmpFilterData::Base58(base64_str),
                })]),
                Commitment::Finalized,
            )
            .unwrap()
            .iter()
            .map(|x| x.pubkey)
            .collect::<HashSet<Pubkey>>();
        assert_eq!(gpa_2, gpa_acc_1);

        let gpa_acc_3 = [pk3_1, pk3_2].iter().cloned().collect::<HashSet<Pubkey>>();
        let gpa = store
            .get_program_accounts(
                prog_1,
                Some(vec![AccountFilterType::DataSize(300)]),
                Commitment::Finalized,
            )
            .unwrap()
            .iter()
            .map(|x| x.pubkey)
            .collect::<HashSet<Pubkey>>();
        assert_eq!(gpa, gpa_acc_3);

        let base64_str = base64::engine::general_purpose::STANDARD.encode(&data_3[10..30]);
        let gpa = store
            .get_program_accounts(
                prog_1,
                Some(vec![AccountFilterType::Memcmp(MemcmpFilter {
                    offset: 10,
                    data: MemcmpFilterData::Base64(base64_str),
                })]),
                Commitment::Finalized,
            )
            .unwrap()
            .iter()
            .map(|x| x.pubkey)
            .collect::<HashSet<Pubkey>>();
        assert_eq!(gpa, gpa_acc_3);

        let base64_str = bs58::encode(&data_3[20..60]).into_string();
        let gpa = store
            .get_program_accounts(
                prog_1,
                Some(vec![AccountFilterType::Memcmp(MemcmpFilter {
                    offset: 20,
                    data: MemcmpFilterData::Base58(base64_str),
                })]),
                Commitment::Finalized,
            )
            .unwrap()
            .iter()
            .map(|x| x.pubkey)
            .collect::<HashSet<Pubkey>>();
        assert_eq!(gpa, gpa_acc_3);
    }

    #[test]
    pub fn writing_old_account_state() {
        let program = Pubkey::new_unique();
        let filter_store = new_filter_store(vec![AccountFilter {
            program_id: Some(program),
            accounts: vec![],
            filters: None,
        }]);

        let store = InmemoryAccountStore::new(filter_store);
        let mut rng = rand::thread_rng();
        let pk1 = Pubkey::new_unique();

        // setting random account as finalized at slot 0
        let account_data_0 = create_random_account(&mut rng, 0, pk1, program);
        store.initialize_or_update_account(account_data_0.clone());

        assert_eq!(
            store.get_account(pk1, Commitment::Processed),
            Ok(Some(account_data_0.clone()))
        );
        assert_eq!(
            store.get_account(pk1, Commitment::Confirmed),
            Ok(Some(account_data_0.clone()))
        );

        // updating state for processed at slot 3
        let account_data_slot_3 = create_random_account(&mut rng, 3, pk1, program);
        store.update_account(account_data_slot_3.clone(), Commitment::Processed);
        assert_eq!(
            store.get_account(pk1, Commitment::Processed),
            Ok(Some(account_data_slot_3.clone()))
        );
        assert_eq!(
            store.get_account(pk1, Commitment::Confirmed),
            Ok(Some(account_data_0.clone()))
        );

        // updating state for processed at slot 2
        let account_data_slot_2 = create_random_account(&mut rng, 2, pk1, program);
        store.update_account(account_data_slot_2.clone(), Commitment::Processed);
        assert_eq!(
            store.get_account(pk1, Commitment::Processed),
            Ok(Some(account_data_slot_3.clone()))
        );
        assert_eq!(
            store.get_account(pk1, Commitment::Confirmed),
            Ok(Some(account_data_0.clone()))
        );

        // confirming slot 2
        let updates = store.process_slot_data(new_slot_info(2), Commitment::Confirmed);
        assert_eq!(updates, vec![account_data_slot_2.clone()]);
        assert_eq!(
            store.get_account(pk1, Commitment::Processed),
            Ok(Some(account_data_slot_3.clone()))
        );
        assert_eq!(
            store.get_account(pk1, Commitment::Confirmed),
            Ok(Some(account_data_slot_2.clone()))
        );
        assert_eq!(
            store.get_account(pk1, Commitment::Finalized),
            Ok(Some(account_data_0.clone()))
        );

        // confirming random state at slot 1 / does not do anything as slot 2 has already been confrimed
        let account_data_slot_1 = create_random_account(&mut rng, 1, pk1, program);
        store.update_account(account_data_slot_1.clone(), Commitment::Confirmed);
        assert_eq!(
            store.get_account(pk1, Commitment::Processed),
            Ok(Some(account_data_slot_3.clone()))
        );
        assert_eq!(
            store.get_account(pk1, Commitment::Confirmed),
            Ok(Some(account_data_slot_2.clone()))
        );
        assert_eq!(
            store.get_account(pk1, Commitment::Finalized),
            Ok(Some(account_data_0.clone()))
        );

        // making slot 3 finalized
        let updates = store.process_slot_data(new_slot_info(3), Commitment::Finalized);
        assert_eq!(updates, vec![account_data_slot_3.clone()]);
        assert_eq!(
            store.get_account(pk1, Commitment::Processed),
            Ok(Some(account_data_slot_3.clone()))
        );
        assert_eq!(
            store.get_account(pk1, Commitment::Confirmed),
            Ok(Some(account_data_slot_3.clone()))
        );
        assert_eq!(
            store.get_account(pk1, Commitment::Finalized),
            Ok(Some(account_data_slot_3.clone()))
        );

        // making slot 2 finalized
        let updates = store.process_slot_data(new_slot_info(2), Commitment::Finalized);
        assert!(updates.is_empty());
        assert_eq!(
            store.get_account(pk1, Commitment::Processed),
            Ok(Some(account_data_slot_3.clone()))
        );
        assert_eq!(
            store.get_account(pk1, Commitment::Confirmed),
            Ok(Some(account_data_slot_3.clone()))
        );
        assert_eq!(
            store.get_account(pk1, Commitment::Finalized),
            Ok(Some(account_data_slot_3.clone()))
        );

        // useless old updates
        let account_data_slot_1_2 = create_random_account(&mut rng, 1, pk1, program);
        let account_data_slot_2_2 = create_random_account(&mut rng, 2, pk1, program);
        store.update_account(account_data_slot_1_2.clone(), Commitment::Processed);
        store.update_account(account_data_slot_2_2.clone(), Commitment::Confirmed);
        assert_eq!(
            store.get_account(pk1, Commitment::Processed),
            Ok(Some(account_data_slot_3.clone()))
        );
        assert_eq!(
            store.get_account(pk1, Commitment::Confirmed),
            Ok(Some(account_data_slot_3.clone()))
        );
        assert_eq!(
            store.get_account(pk1, Commitment::Finalized),
            Ok(Some(account_data_slot_3.clone()))
        );
    }

    #[test]
    pub fn account_states_with_different_write_version() {
        let program = Pubkey::new_unique();
        let filter_store = new_filter_store(vec![AccountFilter {
            program_id: Some(program),
            accounts: vec![],
            filters: None,
        }]);

        let store = InmemoryAccountStore::new(filter_store);
        let mut rng = rand::thread_rng();
        let pk1 = Pubkey::new_unique();

        // setting random account as finalized at slot 0
        let account_data_10 =
            create_random_account_with_write_version(&mut rng, 1, pk1, program, 10);
        store.update_account(account_data_10.clone(), Commitment::Processed);
        assert_eq!(
            store.get_account(pk1, Commitment::Processed),
            Ok(Some(account_data_10.clone()))
        );
        assert_eq!(store.get_account(pk1, Commitment::Confirmed), Ok(None));
        assert_eq!(store.get_account(pk1, Commitment::Finalized), Ok(None));

        // with higher write version process account is updated
        let account_data_11 =
            create_random_account_with_write_version(&mut rng, 1, pk1, program, 11);
        store.update_account(account_data_11.clone(), Commitment::Processed);
        assert_eq!(
            store.get_account(pk1, Commitment::Processed),
            Ok(Some(account_data_11.clone()))
        );
        assert_eq!(store.get_account(pk1, Commitment::Confirmed), Ok(None));
        assert_eq!(store.get_account(pk1, Commitment::Finalized), Ok(None));

        // with lower write version process account is not updated
        let account_data_9 = create_random_account_with_write_version(&mut rng, 1, pk1, program, 9);
        store.update_account(account_data_9.clone(), Commitment::Processed);
        assert_eq!(
            store.get_account(pk1, Commitment::Processed),
            Ok(Some(account_data_11.clone()))
        );
        assert_eq!(store.get_account(pk1, Commitment::Confirmed), Ok(None));
        assert_eq!(store.get_account(pk1, Commitment::Finalized), Ok(None));

        // with finalized commitment all the last account version is taken into account
        store.process_slot_data(new_slot_info(1), Commitment::Finalized);
        assert_eq!(
            store.get_account(pk1, Commitment::Processed),
            Ok(Some(account_data_11.clone()))
        );
        assert_eq!(
            store.get_account(pk1, Commitment::Confirmed),
            Ok(Some(account_data_11.clone()))
        );
        assert_eq!(
            store.get_account(pk1, Commitment::Finalized),
            Ok(Some(account_data_11.clone()))
        );

        // if the account for slot is updated after with higher account write version both processed and finalized slots are updated
        let account_data_12 =
            create_random_account_with_write_version(&mut rng, 1, pk1, program, 12);
        store.update_account(account_data_12.clone(), Commitment::Processed);
        assert_eq!(
            store.get_account(pk1, Commitment::Processed),
            Ok(Some(account_data_12.clone()))
        );
        assert_eq!(
            store.get_account(pk1, Commitment::Confirmed),
            Ok(Some(account_data_12.clone()))
        );
        assert_eq!(
            store.get_account(pk1, Commitment::Finalized),
            Ok(Some(account_data_12.clone()))
        );
    }

    #[test]
    pub fn test_owner_changes() {
        let program_1 = Pubkey::new_unique();
        let program_2 = Pubkey::new_unique();
        let program_3 = Pubkey::new_unique();
        let filter_store = new_filter_store(vec![
            AccountFilter {
                program_id: Some(program_1),
                accounts: vec![],
                filters: None,
            },
            AccountFilter {
                program_id: Some(program_2),
                accounts: vec![],
                filters: None,
            },
        ]);

        let store = InmemoryAccountStore::new(filter_store);
        let mut rng = rand::thread_rng();
        let pk1 = Pubkey::new_unique();
        let pk2 = Pubkey::new_unique();
        let pk3 = Pubkey::new_unique();
        let account_1 = create_random_account(&mut rng, 0, pk1, program_1);
        let account_2 = create_random_account(&mut rng, 1, pk2, program_2);
        let account_3 = create_random_account(&mut rng, 1, pk3, program_3);
        store.initialize_or_update_account(account_1.clone());
        store.update_account(account_2.clone(), Commitment::Processed);
        store.update_account(account_3.clone(), Commitment::Processed);
        assert_eq!(
            store.get_account(pk1, Commitment::Finalized).unwrap(),
            Some(account_1.clone())
        );
        assert_eq!(
            store.get_account(pk1, Commitment::Confirmed).unwrap(),
            Some(account_1.clone())
        );
        assert_eq!(
            store.get_account(pk1, Commitment::Processed).unwrap(),
            Some(account_1.clone())
        );
        assert_eq!(
            store.get_account(pk2, Commitment::Processed).unwrap(),
            Some(account_2.clone())
        );
        assert_eq!(store.get_account(pk2, Commitment::Confirmed).unwrap(), None);
        assert_eq!(store.get_account(pk2, Commitment::Finalized).unwrap(), None);
        assert_eq!(store.get_account(pk3, Commitment::Processed).unwrap(), None);
        assert_eq!(store.get_account(pk3, Commitment::Confirmed).unwrap(), None);
        assert_eq!(store.get_account(pk3, Commitment::Processed).unwrap(), None);
        assert_eq!(
            store
                .get_program_accounts(program_1, None, Commitment::Processed)
                .unwrap(),
            vec![account_1.clone()]
        );
        assert_eq!(
            store
                .get_program_accounts(program_1, None, Commitment::Confirmed)
                .unwrap(),
            vec![account_1.clone()]
        );
        assert_eq!(
            store
                .get_program_accounts(program_1, None, Commitment::Finalized)
                .unwrap(),
            vec![account_1.clone()]
        );
        assert_eq!(
            store
                .get_program_accounts(program_2, None, Commitment::Processed)
                .unwrap(),
            vec![account_2.clone()]
        );
        assert_eq!(
            store
                .get_program_accounts(program_2, None, Commitment::Confirmed)
                .unwrap(),
            vec![]
        );
        assert_eq!(
            store
                .get_program_accounts(program_2, None, Commitment::Finalized)
                .unwrap(),
            vec![]
        );
        assert_eq!(
            store.get_program_accounts(program_3, None, Commitment::Processed),
            Err(AccountLoadingError::ConfigDoesNotContainRequiredFilters)
        );
        assert_eq!(
            store.get_program_accounts(program_3, None, Commitment::Confirmed),
            Err(AccountLoadingError::ConfigDoesNotContainRequiredFilters)
        );
        assert_eq!(
            store.get_program_accounts(program_3, None, Commitment::Finalized),
            Err(AccountLoadingError::ConfigDoesNotContainRequiredFilters)
        );

        store.process_slot_data(new_slot_info(1), Commitment::Confirmed);
        // next slots/ change owner for account1 and account 2
        let account_1_slot2 = create_random_account(&mut rng, 2, pk1, program_2);
        let account_2_slot2 = create_random_account(&mut rng, 2, pk2, program_1);
        store.update_account(account_1_slot2.clone(), Commitment::Processed);
        store.update_account(account_2_slot2.clone(), Commitment::Processed);

        assert_eq!(
            store.get_account(pk1, Commitment::Finalized).unwrap(),
            Some(account_1.clone())
        );
        assert_eq!(
            store.get_account(pk1, Commitment::Confirmed).unwrap(),
            Some(account_1.clone())
        );
        assert_eq!(
            store.get_account(pk1, Commitment::Processed).unwrap(),
            Some(account_1_slot2.clone())
        );
        assert_eq!(
            store.get_account(pk2, Commitment::Processed).unwrap(),
            Some(account_2_slot2.clone())
        );
        assert_eq!(
            store.get_account(pk2, Commitment::Confirmed).unwrap(),
            Some(account_2.clone())
        );
        assert_eq!(store.get_account(pk2, Commitment::Finalized).unwrap(), None);
        assert_eq!(
            store
                .get_program_accounts(program_1, None, Commitment::Processed)
                .unwrap(),
            vec![account_2_slot2.clone()]
        );
        assert_eq!(
            store
                .get_program_accounts(program_1, None, Commitment::Confirmed)
                .unwrap(),
            vec![account_1.clone()]
        );
        assert_eq!(
            store
                .get_program_accounts(program_1, None, Commitment::Finalized)
                .unwrap(),
            vec![account_1.clone()]
        );
        assert_eq!(
            store
                .get_program_accounts(program_2, None, Commitment::Processed)
                .unwrap(),
            vec![account_1_slot2.clone()]
        );
        assert_eq!(
            store
                .get_program_accounts(program_2, None, Commitment::Confirmed)
                .unwrap(),
            vec![account_2.clone()]
        );
        assert_eq!(
            store.get_program_accounts(program_2, None, Commitment::Finalized),
            Ok(vec![])
        );

        store.process_slot_data(new_slot_info(1), Commitment::Finalized);
        store.process_slot_data(new_slot_info(2), Commitment::Confirmed);
        // next slots/ change owner for account1 and account 2
        assert_eq!(
            store.get_account(pk1, Commitment::Finalized).unwrap(),
            Some(account_1.clone())
        );
        assert_eq!(
            store.get_account(pk1, Commitment::Confirmed).unwrap(),
            Some(account_1_slot2.clone())
        );
        assert_eq!(
            store.get_account(pk1, Commitment::Processed).unwrap(),
            Some(account_1_slot2.clone())
        );
        assert_eq!(
            store.get_account(pk2, Commitment::Processed).unwrap(),
            Some(account_2_slot2.clone())
        );
        assert_eq!(
            store.get_account(pk2, Commitment::Confirmed).unwrap(),
            Some(account_2_slot2.clone())
        );
        assert_eq!(
            store.get_account(pk2, Commitment::Finalized).unwrap(),
            Some(account_2.clone())
        );
        assert_eq!(
            store
                .get_program_accounts(program_1, None, Commitment::Processed)
                .unwrap(),
            vec![account_2_slot2.clone()]
        );
        assert_eq!(
            store
                .get_program_accounts(program_1, None, Commitment::Confirmed)
                .unwrap(),
            vec![account_2_slot2.clone()]
        );
        assert_eq!(
            store
                .get_program_accounts(program_1, None, Commitment::Finalized)
                .unwrap(),
            vec![account_1.clone()]
        );
        assert_eq!(
            store
                .get_program_accounts(program_2, None, Commitment::Processed)
                .unwrap(),
            vec![account_1_slot2.clone()]
        );
        assert_eq!(
            store
                .get_program_accounts(program_2, None, Commitment::Confirmed)
                .unwrap(),
            vec![account_1_slot2.clone()]
        );
        assert_eq!(
            store
                .get_program_accounts(program_2, None, Commitment::Finalized)
                .unwrap(),
            vec![account_2.clone()]
        );

        store.process_slot_data(new_slot_info(2), Commitment::Finalized);
        assert_eq!(
            store.get_account(pk1, Commitment::Finalized).unwrap(),
            Some(account_1_slot2.clone())
        );
        assert_eq!(
            store.get_account(pk1, Commitment::Confirmed).unwrap(),
            Some(account_1_slot2.clone())
        );
        assert_eq!(
            store.get_account(pk1, Commitment::Processed).unwrap(),
            Some(account_1_slot2.clone())
        );
        assert_eq!(
            store.get_account(pk2, Commitment::Processed).unwrap(),
            Some(account_2_slot2.clone())
        );
        assert_eq!(
            store.get_account(pk2, Commitment::Confirmed).unwrap(),
            Some(account_2_slot2.clone())
        );
        assert_eq!(
            store.get_account(pk2, Commitment::Finalized).unwrap(),
            Some(account_2_slot2.clone())
        );
        assert_eq!(
            store
                .get_program_accounts(program_1, None, Commitment::Processed)
                .unwrap(),
            vec![account_2_slot2.clone()]
        );
        assert_eq!(
            store
                .get_program_accounts(program_1, None, Commitment::Confirmed)
                .unwrap(),
            vec![account_2_slot2.clone()]
        );
        assert_eq!(
            store
                .get_program_accounts(program_1, None, Commitment::Finalized)
                .unwrap(),
            vec![account_2_slot2.clone()]
        );
        assert_eq!(
            store
                .get_program_accounts(program_2, None, Commitment::Processed)
                .unwrap(),
            vec![account_1_slot2.clone()]
        );
        assert_eq!(
            store
                .get_program_accounts(program_2, None, Commitment::Confirmed)
                .unwrap(),
            vec![account_1_slot2.clone()]
        );
        assert_eq!(
            store
                .get_program_accounts(program_2, None, Commitment::Finalized)
                .unwrap(),
            vec![account_1_slot2.clone()]
        );

        // changing user for account 1 and account 2 to program 3 / account 3 in program 1
        let account_1_slot5 = create_random_account(&mut rng, 5, pk1, program_3);
        let account_2_slot5 = create_random_account(&mut rng, 5, pk2, program_3);
        let account_3_slot5 = create_random_account(&mut rng, 5, pk3, program_1);
        store.update_account(account_1_slot5.clone(), Commitment::Processed);
        store.update_account(account_2_slot5.clone(), Commitment::Processed);
        store.update_account(account_3_slot5.clone(), Commitment::Processed);

        assert_eq!(
            store.get_account(pk1, Commitment::Processed).unwrap(),
            Some(account_1_slot5.clone())
        );
        assert_eq!(
            store.get_account(pk1, Commitment::Confirmed).unwrap(),
            Some(account_1_slot2.clone())
        );
        assert_eq!(
            store.get_account(pk1, Commitment::Finalized).unwrap(),
            Some(account_1_slot2.clone())
        );
        assert_eq!(
            store.get_account(pk2, Commitment::Processed).unwrap(),
            Some(account_2_slot5.clone())
        );
        assert_eq!(
            store.get_account(pk2, Commitment::Confirmed).unwrap(),
            Some(account_2_slot2.clone())
        );
        assert_eq!(
            store.get_account(pk2, Commitment::Finalized).unwrap(),
            Some(account_2_slot2.clone())
        );
        assert_eq!(
            store.get_account(pk3, Commitment::Processed).unwrap(),
            Some(account_3_slot5.clone())
        );
        assert_eq!(store.get_account(pk3, Commitment::Confirmed).unwrap(), None);
        assert_eq!(store.get_account(pk3, Commitment::Finalized).unwrap(), None);
        assert_eq!(
            store
                .get_program_accounts(program_1, None, Commitment::Processed)
                .unwrap(),
            vec![account_3_slot5.clone()]
        );
        assert_eq!(
            store
                .get_program_accounts(program_1, None, Commitment::Confirmed)
                .unwrap(),
            vec![account_2_slot2.clone()]
        );
        assert_eq!(
            store
                .get_program_accounts(program_1, None, Commitment::Finalized)
                .unwrap(),
            vec![account_2_slot2.clone()]
        );
        assert_eq!(
            store
                .get_program_accounts(program_2, None, Commitment::Processed)
                .unwrap(),
            vec![]
        );
        assert_eq!(
            store
                .get_program_accounts(program_2, None, Commitment::Confirmed)
                .unwrap(),
            vec![account_1_slot2.clone()]
        );
        assert_eq!(
            store
                .get_program_accounts(program_2, None, Commitment::Finalized)
                .unwrap(),
            vec![account_1_slot2.clone()]
        );
        assert_eq!(
            store.get_program_accounts(program_3, None, Commitment::Processed),
            Err(AccountLoadingError::ConfigDoesNotContainRequiredFilters)
        );
        assert_eq!(
            store.get_program_accounts(program_3, None, Commitment::Confirmed),
            Err(AccountLoadingError::ConfigDoesNotContainRequiredFilters)
        );
        assert_eq!(
            store.get_program_accounts(program_3, None, Commitment::Finalized),
            Err(AccountLoadingError::ConfigDoesNotContainRequiredFilters)
        );

        store.process_slot_data(new_slot_info(5), Commitment::Confirmed);
        // next slots/ change owner for account1 and account 2
        assert_eq!(
            store.get_account(pk1, Commitment::Processed).unwrap(),
            Some(account_1_slot5.clone())
        );
        assert_eq!(
            store.get_account(pk1, Commitment::Confirmed).unwrap(),
            Some(account_1_slot5.clone())
        );
        assert_eq!(
            store.get_account(pk1, Commitment::Finalized).unwrap(),
            Some(account_1_slot2.clone())
        );
        assert_eq!(
            store.get_account(pk2, Commitment::Processed).unwrap(),
            Some(account_2_slot5.clone())
        );
        assert_eq!(
            store.get_account(pk2, Commitment::Confirmed).unwrap(),
            Some(account_2_slot5.clone())
        );
        assert_eq!(
            store.get_account(pk2, Commitment::Finalized).unwrap(),
            Some(account_2_slot2.clone())
        );
        assert_eq!(
            store.get_account(pk3, Commitment::Processed).unwrap(),
            Some(account_3_slot5.clone())
        );
        assert_eq!(
            store.get_account(pk3, Commitment::Confirmed).unwrap(),
            Some(account_3_slot5.clone())
        );
        assert_eq!(store.get_account(pk3, Commitment::Finalized).unwrap(), None);
        assert_eq!(
            store
                .get_program_accounts(program_1, None, Commitment::Processed)
                .unwrap(),
            vec![account_3_slot5.clone()]
        );
        assert_eq!(
            store
                .get_program_accounts(program_1, None, Commitment::Confirmed)
                .unwrap(),
            vec![account_3_slot5.clone()]
        );
        assert_eq!(
            store
                .get_program_accounts(program_1, None, Commitment::Finalized)
                .unwrap(),
            vec![account_2_slot2.clone()]
        );
        assert_eq!(
            store
                .get_program_accounts(program_2, None, Commitment::Processed)
                .unwrap(),
            vec![]
        );
        assert_eq!(
            store
                .get_program_accounts(program_2, None, Commitment::Confirmed)
                .unwrap(),
            vec![]
        );
        assert_eq!(
            store
                .get_program_accounts(program_2, None, Commitment::Finalized)
                .unwrap(),
            vec![account_1_slot2.clone()]
        );
        assert_eq!(
            store.get_program_accounts(program_3, None, Commitment::Processed),
            Err(AccountLoadingError::ConfigDoesNotContainRequiredFilters)
        );
        assert_eq!(
            store.get_program_accounts(program_3, None, Commitment::Confirmed),
            Err(AccountLoadingError::ConfigDoesNotContainRequiredFilters)
        );
        assert_eq!(
            store.get_program_accounts(program_3, None, Commitment::Finalized),
            Err(AccountLoadingError::ConfigDoesNotContainRequiredFilters)
        );

        // accounts deleted as they do not satisfy filter criterias
        store.process_slot_data(new_slot_info(5), Commitment::Finalized);
        assert_eq!(store.get_account(pk1, Commitment::Processed).unwrap(), None);
        assert_eq!(store.get_account(pk1, Commitment::Confirmed).unwrap(), None);
        assert_eq!(store.get_account(pk1, Commitment::Finalized).unwrap(), None);
        assert_eq!(store.get_account(pk2, Commitment::Processed).unwrap(), None);
        assert_eq!(store.get_account(pk2, Commitment::Confirmed).unwrap(), None);
        assert_eq!(store.get_account(pk2, Commitment::Finalized).unwrap(), None);
        assert_eq!(
            store.get_account(pk3, Commitment::Processed).unwrap(),
            Some(account_3_slot5.clone())
        );
        assert_eq!(
            store.get_account(pk3, Commitment::Confirmed).unwrap(),
            Some(account_3_slot5.clone())
        );
        assert_eq!(
            store.get_account(pk3, Commitment::Finalized).unwrap(),
            Some(account_3_slot5.clone())
        );
        assert_eq!(
            store
                .get_program_accounts(program_1, None, Commitment::Processed)
                .unwrap(),
            vec![account_3_slot5.clone()]
        );
        assert_eq!(
            store
                .get_program_accounts(program_1, None, Commitment::Confirmed)
                .unwrap(),
            vec![account_3_slot5.clone()]
        );
        assert_eq!(
            store
                .get_program_accounts(program_1, None, Commitment::Finalized)
                .unwrap(),
            vec![account_3_slot5.clone()]
        );
        assert_eq!(
            store
                .get_program_accounts(program_2, None, Commitment::Processed)
                .unwrap(),
            vec![]
        );
        assert_eq!(
            store
                .get_program_accounts(program_2, None, Commitment::Confirmed)
                .unwrap(),
            vec![]
        );
        assert_eq!(
            store
                .get_program_accounts(program_2, None, Commitment::Finalized)
                .unwrap(),
            vec![]
        );
        assert_eq!(
            store.get_program_accounts(program_3, None, Commitment::Processed),
            Err(AccountLoadingError::ConfigDoesNotContainRequiredFilters)
        );
        assert_eq!(
            store.get_program_accounts(program_3, None, Commitment::Confirmed),
            Err(AccountLoadingError::ConfigDoesNotContainRequiredFilters)
        );
        assert_eq!(
            store.get_program_accounts(program_3, None, Commitment::Finalized),
            Err(AccountLoadingError::ConfigDoesNotContainRequiredFilters)
        );

        let account_2_slot6 = create_random_account(&mut rng, 6, pk2, program_1);
        store.update_account(account_2_slot6.clone(), Commitment::Processed);
        assert_eq!(store.get_account(pk1, Commitment::Processed).unwrap(), None);
        assert_eq!(store.get_account(pk1, Commitment::Confirmed).unwrap(), None);
        assert_eq!(store.get_account(pk1, Commitment::Finalized).unwrap(), None);
        assert_eq!(
            store.get_account(pk2, Commitment::Processed).unwrap(),
            Some(account_2_slot6.clone())
        );
        assert_eq!(store.get_account(pk2, Commitment::Confirmed).unwrap(), None);
        assert_eq!(store.get_account(pk2, Commitment::Finalized).unwrap(), None);
        assert_eq!(
            store.get_account(pk3, Commitment::Processed).unwrap(),
            Some(account_3_slot5.clone())
        );
        assert_eq!(
            store.get_account(pk3, Commitment::Confirmed).unwrap(),
            Some(account_3_slot5.clone())
        );
        assert_eq!(
            store.get_account(pk3, Commitment::Finalized).unwrap(),
            Some(account_3_slot5.clone())
        );
        assert!(store
            .get_program_accounts(program_1, None, Commitment::Processed)
            .unwrap()
            .contains(&account_2_slot6));
        assert!(store
            .get_program_accounts(program_1, None, Commitment::Processed)
            .unwrap()
            .contains(&account_3_slot5));
        assert_eq!(
            store
                .get_program_accounts(program_1, None, Commitment::Confirmed)
                .unwrap(),
            vec![account_3_slot5.clone()]
        );
        assert_eq!(
            store
                .get_program_accounts(program_1, None, Commitment::Finalized)
                .unwrap(),
            vec![account_3_slot5.clone()]
        );
        assert_eq!(
            store
                .get_program_accounts(program_2, None, Commitment::Processed)
                .unwrap(),
            vec![]
        );
        assert_eq!(
            store
                .get_program_accounts(program_2, None, Commitment::Confirmed)
                .unwrap(),
            vec![]
        );
        assert_eq!(
            store
                .get_program_accounts(program_2, None, Commitment::Finalized)
                .unwrap(),
            vec![]
        );
        assert_eq!(
            store.get_program_accounts(program_3, None, Commitment::Processed),
            Err(AccountLoadingError::ConfigDoesNotContainRequiredFilters)
        );
        assert_eq!(
            store.get_program_accounts(program_3, None, Commitment::Confirmed),
            Err(AccountLoadingError::ConfigDoesNotContainRequiredFilters)
        );
        assert_eq!(
            store.get_program_accounts(program_3, None, Commitment::Finalized),
            Err(AccountLoadingError::ConfigDoesNotContainRequiredFilters)
        );

        store.process_slot_data(new_slot_info(6), Commitment::Confirmed);
        assert_eq!(store.get_account(pk1, Commitment::Processed).unwrap(), None);
        assert_eq!(store.get_account(pk1, Commitment::Confirmed).unwrap(), None);
        assert_eq!(store.get_account(pk1, Commitment::Finalized).unwrap(), None);
        assert_eq!(
            store.get_account(pk2, Commitment::Processed).unwrap(),
            Some(account_2_slot6.clone())
        );
        assert_eq!(
            store.get_account(pk2, Commitment::Confirmed).unwrap(),
            Some(account_2_slot6.clone())
        );
        assert_eq!(store.get_account(pk2, Commitment::Finalized).unwrap(), None);
        assert_eq!(
            store.get_account(pk3, Commitment::Processed).unwrap(),
            Some(account_3_slot5.clone())
        );
        assert_eq!(
            store.get_account(pk3, Commitment::Confirmed).unwrap(),
            Some(account_3_slot5.clone())
        );
        assert_eq!(
            store.get_account(pk3, Commitment::Finalized).unwrap(),
            Some(account_3_slot5.clone())
        );
        assert!(store
            .get_program_accounts(program_1, None, Commitment::Processed)
            .unwrap()
            .contains(&account_2_slot6));
        assert!(store
            .get_program_accounts(program_1, None, Commitment::Processed)
            .unwrap()
            .contains(&account_3_slot5));
        assert!(store
            .get_program_accounts(program_1, None, Commitment::Confirmed)
            .unwrap()
            .contains(&account_2_slot6));
        assert!(store
            .get_program_accounts(program_1, None, Commitment::Confirmed)
            .unwrap()
            .contains(&account_3_slot5));
        assert_eq!(
            store
                .get_program_accounts(program_1, None, Commitment::Finalized)
                .unwrap(),
            vec![account_3_slot5.clone()]
        );
        assert_eq!(
            store
                .get_program_accounts(program_2, None, Commitment::Processed)
                .unwrap(),
            vec![]
        );
        assert_eq!(
            store
                .get_program_accounts(program_2, None, Commitment::Confirmed)
                .unwrap(),
            vec![]
        );
        assert_eq!(
            store
                .get_program_accounts(program_2, None, Commitment::Finalized)
                .unwrap(),
            vec![]
        );
        assert_eq!(
            store.get_program_accounts(program_3, None, Commitment::Processed),
            Err(AccountLoadingError::ConfigDoesNotContainRequiredFilters)
        );
        assert_eq!(
            store.get_program_accounts(program_3, None, Commitment::Confirmed),
            Err(AccountLoadingError::ConfigDoesNotContainRequiredFilters)
        );
        assert_eq!(
            store.get_program_accounts(program_3, None, Commitment::Finalized),
            Err(AccountLoadingError::ConfigDoesNotContainRequiredFilters)
        );

        store.process_slot_data(new_slot_info(6), Commitment::Finalized);
        assert_eq!(store.get_account(pk1, Commitment::Processed).unwrap(), None);
        assert_eq!(store.get_account(pk1, Commitment::Confirmed).unwrap(), None);
        assert_eq!(store.get_account(pk1, Commitment::Finalized).unwrap(), None);
        assert_eq!(
            store.get_account(pk2, Commitment::Processed).unwrap(),
            Some(account_2_slot6.clone())
        );
        assert_eq!(
            store.get_account(pk2, Commitment::Confirmed).unwrap(),
            Some(account_2_slot6.clone())
        );
        assert_eq!(
            store.get_account(pk2, Commitment::Finalized).unwrap(),
            Some(account_2_slot6.clone())
        );
        assert_eq!(
            store.get_account(pk3, Commitment::Processed).unwrap(),
            Some(account_3_slot5.clone())
        );
        assert_eq!(
            store.get_account(pk3, Commitment::Confirmed).unwrap(),
            Some(account_3_slot5.clone())
        );
        assert_eq!(
            store.get_account(pk3, Commitment::Finalized).unwrap(),
            Some(account_3_slot5.clone())
        );
        assert!(store
            .get_program_accounts(program_1, None, Commitment::Processed)
            .unwrap()
            .contains(&account_2_slot6));
        assert!(store
            .get_program_accounts(program_1, None, Commitment::Processed)
            .unwrap()
            .contains(&account_3_slot5));
        assert!(store
            .get_program_accounts(program_1, None, Commitment::Confirmed)
            .unwrap()
            .contains(&account_2_slot6));
        assert!(store
            .get_program_accounts(program_1, None, Commitment::Confirmed)
            .unwrap()
            .contains(&account_3_slot5));
        assert!(store
            .get_program_accounts(program_1, None, Commitment::Finalized)
            .unwrap()
            .contains(&account_2_slot6));
        assert!(store
            .get_program_accounts(program_1, None, Commitment::Finalized)
            .unwrap()
            .contains(&account_3_slot5));
        assert_eq!(
            store
                .get_program_accounts(program_2, None, Commitment::Processed)
                .unwrap(),
            vec![]
        );
        assert_eq!(
            store
                .get_program_accounts(program_2, None, Commitment::Confirmed)
                .unwrap(),
            vec![]
        );
        assert_eq!(
            store
                .get_program_accounts(program_2, None, Commitment::Finalized)
                .unwrap(),
            vec![]
        );
        assert_eq!(
            store.get_program_accounts(program_3, None, Commitment::Processed),
            Err(AccountLoadingError::ConfigDoesNotContainRequiredFilters)
        );
        assert_eq!(
            store.get_program_accounts(program_3, None, Commitment::Confirmed),
            Err(AccountLoadingError::ConfigDoesNotContainRequiredFilters)
        );
        assert_eq!(
            store.get_program_accounts(program_3, None, Commitment::Finalized),
            Err(AccountLoadingError::ConfigDoesNotContainRequiredFilters)
        );
    }

    #[test]
    pub fn test_account_deletions() {
        let program_1 = Pubkey::new_unique();
        let filter_store = new_filter_store(vec![AccountFilter {
            program_id: Some(program_1),
            accounts: vec![],
            filters: None,
        }]);

        let store = InmemoryAccountStore::new(filter_store);
        let mut rng = rand::thread_rng();
        let pk1 = Pubkey::new_unique();

        // add and test account
        let account_1 = create_random_account_with_write_version(&mut rng, 1, pk1, program_1, 1);
        store.initialize_or_update_account(account_1.clone());
        assert_eq!(
            store.get_account(pk1, Commitment::Processed).unwrap(),
            Some(account_1.clone())
        );
        assert_eq!(
            store.get_account(pk1, Commitment::Confirmed).unwrap(),
            Some(account_1.clone())
        );
        assert_eq!(
            store.get_account(pk1, Commitment::Finalized).unwrap(),
            Some(account_1.clone())
        );

        assert_eq!(
            store
                .get_program_accounts(program_1, None, Commitment::Processed)
                .unwrap(),
            vec![account_1.clone()]
        );
        assert_eq!(
            store
                .get_program_accounts(program_1, None, Commitment::Confirmed)
                .unwrap(),
            vec![account_1.clone()]
        );
        assert_eq!(
            store
                .get_program_accounts(program_1, None, Commitment::Finalized)
                .unwrap(),
            vec![account_1.clone()]
        );

        // delete account 1
        let pk2 = Pubkey::new_unique();
        let account_1_2 = create_random_account_with_write_version(&mut rng, 2, pk1, program_1, 2);
        store.update_account(account_1_2, Commitment::Processed);
        let account_1_3 = AccountData {
            pubkey: pk1,
            account: Arc::new(Account {
                lamports: 0,
                data: lite_account_manager_common::account_data::Data::Uncompressed(vec![]),
                owner: Pubkey::default(),
                executable: false,
                rent_epoch: u64::MAX,
            }),
            updated_slot: 2,
            write_version: 3,
        };
        let random_deletion = AccountData {
            pubkey: pk2,
            account: Arc::new(Account {
                lamports: 0,
                data: lite_account_manager_common::account_data::Data::Uncompressed(vec![]),
                owner: Pubkey::default(),
                executable: false,
                rent_epoch: u64::MAX,
            }),
            updated_slot: 2,
            write_version: 4,
        };
        store.update_account(account_1_3.clone(), Commitment::Processed);
        store.update_account(random_deletion.clone(), Commitment::Processed);

        assert_eq!(store.get_account(pk1, Commitment::Processed).unwrap(), None);
        assert_eq!(
            store.get_account(pk1, Commitment::Confirmed).unwrap(),
            Some(account_1.clone())
        );
        assert_eq!(
            store.get_account(pk1, Commitment::Finalized).unwrap(),
            Some(account_1.clone())
        );
        assert_eq!(store.get_account(pk2, Commitment::Processed).unwrap(), None);
        assert_eq!(store.get_account(pk2, Commitment::Confirmed).unwrap(), None);
        assert_eq!(store.get_account(pk2, Commitment::Finalized).unwrap(), None);

        assert_eq!(
            store
                .get_program_accounts(program_1, None, Commitment::Processed)
                .unwrap(),
            vec![]
        );
        assert_eq!(
            store
                .get_program_accounts(program_1, None, Commitment::Confirmed)
                .unwrap(),
            vec![account_1.clone()]
        );
        assert_eq!(
            store
                .get_program_accounts(program_1, None, Commitment::Finalized)
                .unwrap(),
            vec![account_1.clone()]
        );

        let updated = store.process_slot_data(new_slot_info(2), Commitment::Confirmed);
        assert_eq!(updated, vec![account_1_3.clone()]);

        assert_eq!(store.get_account(pk1, Commitment::Processed).unwrap(), None);
        assert_eq!(store.get_account(pk1, Commitment::Confirmed).unwrap(), None);
        assert_eq!(
            store.get_account(pk1, Commitment::Finalized).unwrap(),
            Some(account_1.clone())
        );
        assert_eq!(store.get_account(pk2, Commitment::Processed).unwrap(), None);
        assert_eq!(store.get_account(pk2, Commitment::Confirmed).unwrap(), None);
        assert_eq!(store.get_account(pk2, Commitment::Finalized).unwrap(), None);

        assert_eq!(
            store
                .get_program_accounts(program_1, None, Commitment::Processed)
                .unwrap(),
            vec![]
        );
        assert_eq!(
            store
                .get_program_accounts(program_1, None, Commitment::Confirmed)
                .unwrap(),
            vec![]
        );
        assert_eq!(
            store
                .get_program_accounts(program_1, None, Commitment::Finalized)
                .unwrap(),
            vec![account_1.clone()]
        );

        let updated = store.process_slot_data(new_slot_info(2), Commitment::Finalized);
        assert_eq!(updated, vec![account_1_3]);

        assert_eq!(store.get_account(pk1, Commitment::Processed).unwrap(), None);
        assert_eq!(store.get_account(pk1, Commitment::Confirmed).unwrap(), None);
        assert_eq!(store.get_account(pk1, Commitment::Finalized).unwrap(), None);
        assert_eq!(store.get_account(pk2, Commitment::Processed).unwrap(), None);
        assert_eq!(store.get_account(pk2, Commitment::Confirmed).unwrap(), None);
        assert_eq!(store.get_account(pk2, Commitment::Finalized).unwrap(), None);

        assert_eq!(
            store
                .get_program_accounts(program_1, None, Commitment::Processed)
                .unwrap(),
            vec![]
        );
        assert_eq!(
            store
                .get_program_accounts(program_1, None, Commitment::Confirmed)
                .unwrap(),
            vec![]
        );
        assert_eq!(
            store
                .get_program_accounts(program_1, None, Commitment::Finalized)
                .unwrap(),
            vec![]
        );
    }

    #[test]
    pub fn test_snapshot_creation_and_loading() {
        let program_1 = Pubkey::new_unique();
        let program_2 = Pubkey::new_unique();
        let filter_store = new_filter_store(vec![
            AccountFilter {
                program_id: Some(program_1),
                accounts: vec![],
                filters: None,
            },
            AccountFilter {
                program_id: Some(program_2),
                accounts: vec![],
                filters: None,
            },
        ]);

        let store = InmemoryAccountStore::new(filter_store.clone());
        let mut rng = rand::thread_rng();
        let pk1 = Pubkey::new_unique();
        let pk2 = Pubkey::new_unique();
        let pk3 = Pubkey::new_unique();
        let pk4 = Pubkey::new_unique();

        let account1 = create_random_account(&mut rng, 1, pk1, program_1);
        let account2 = create_random_account(&mut rng, 1, pk2, program_1);
        let account3 = create_random_account(&mut rng, 1, pk3, program_2);
        let account4 = create_random_account(&mut rng, 1, pk4, program_2);

        store.initialize_or_update_account(account1);
        store.initialize_or_update_account(account2);
        store.initialize_or_update_account(account3);
        store.initialize_or_update_account(account4);

        let account1 = create_random_account(&mut rng, 2, pk1, program_1);
        let account2 = create_random_account(&mut rng, 2, pk2, program_1);
        let account3 = create_random_account(&mut rng, 2, pk3, program_2);
        let account4 = create_random_account(&mut rng, 2, pk4, program_2);

        store.update_account(account1.clone(), Commitment::Processed);
        store.update_account(account2.clone(), Commitment::Processed);
        store.update_account(account3.clone(), Commitment::Processed);
        store.update_account(account4.clone(), Commitment::Processed);
        let account1_bis = create_random_account(&mut rng, 3, pk1, program_1);
        let account3_bis = create_random_account(&mut rng, 3, pk3, program_2);
        store.update_account(account1_bis.clone(), Commitment::Processed);
        store.update_account(account3_bis.clone(), Commitment::Processed);

        store.process_slot_data(
            SlotInfo {
                parent: 1,
                slot: 2,
                root: 0,
            },
            Commitment::Finalized,
        );

        let gpa_1 = store
            .get_program_accounts(program_1, None, Commitment::Processed)
            .unwrap();
        assert!(
            gpa_1 == vec![account1_bis.clone(), account2.clone()]
                || gpa_1 == vec![account2.clone(), account1_bis.clone()]
        );

        let gpa_2 = store
            .get_program_accounts(program_2, None, Commitment::Processed)
            .unwrap();
        assert!(
            gpa_2 == vec![account3_bis.clone(), account4.clone()]
                || gpa_2 == vec![account4.clone(), account3_bis.clone()]
        );

        // create snapshot and load
        let snapshot_program_1 = store.create_snapshot(program_1).unwrap();
        let snapshot_program_2 = store.create_snapshot(program_2).unwrap();

        let snapshot_store = InmemoryAccountStore::new(filter_store);
        // is empty
        let gpa_1 = snapshot_store
            .get_program_accounts(program_1, None, Commitment::Processed)
            .unwrap();
        assert_eq!(gpa_1, vec![]);

        let gpa_2 = snapshot_store
            .get_program_accounts(program_2, None, Commitment::Processed)
            .unwrap();
        assert_eq!(gpa_2, vec![]);

        snapshot_store
            .load_from_snapshot(program_1, snapshot_program_1)
            .unwrap();
        snapshot_store
            .load_from_snapshot(program_2, snapshot_program_2)
            .unwrap();

        let gpa_1 = snapshot_store
            .get_program_accounts(program_1, None, Commitment::Processed)
            .unwrap();
        assert!(
            gpa_1 == vec![account1.clone(), account2.clone()]
                || gpa_1 == vec![account2.clone(), account1.clone()]
        );

        let gpa_2 = snapshot_store
            .get_program_accounts(program_2, None, Commitment::Processed)
            .unwrap();
        assert!(
            gpa_2 == vec![account3.clone(), account4.clone()]
                || gpa_2 == vec![account4.clone(), account3.clone()]
        );
    }
}
