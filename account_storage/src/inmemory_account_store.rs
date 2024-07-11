use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
};

use crate::account_data_by_commitment::AccountDataByCommitment;
use async_trait::async_trait;
use dashmap::DashMap;
use lite_account_manager_common::{
    account_data::AccountData,
    account_filter::{AccountFilter, AccountFilterType},
    account_filters_interface::AccountFiltersStoreInterface,
    account_store_interface::{AccountLoadingError, AccountStorageInterface},
    commitment::Commitment,
    pubkey_container_utils::PartialPubkey,
    slot_info::SlotInfo,
};
use prometheus::{opts, register_int_gauge, IntGauge};
use solana_sdk::{pubkey::Pubkey, slot_history::Slot};
use std::collections::BTreeMap;
use tokio::sync::Mutex;

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
    pub accounts_updated: HashSet<Pubkey>,
    pub parent: Option<Slot>,
}

pub struct InmemoryAccountStore {
    account_store: Arc<DashMap<PartialPubkey<8>, Vec<AccountDataByCommitment>>>,
    accounts_by_owner: Arc<DashMap<Pubkey, HashSet<Pubkey>>>,
    slots_status: Arc<Mutex<BTreeMap<Slot, SlotStatus>>>,
    filtered_accounts: Arc<dyn AccountFiltersStoreInterface>,
}

impl InmemoryAccountStore {
    pub fn new(filtered_accounts: Arc<dyn AccountFiltersStoreInterface>) -> Self {
        Self {
            account_store: Arc::new(DashMap::new()),
            accounts_by_owner: Arc::new(DashMap::new()),
            slots_status: Arc::new(Mutex::new(BTreeMap::new())),
            filtered_accounts,
        }
    }

    fn add_account_owner(&self, account: Pubkey, owner: Pubkey) {
        match self.accounts_by_owner.entry(owner) {
            dashmap::mapref::entry::Entry::Occupied(mut occ) => {
                occ.get_mut().insert(account);
            }
            dashmap::mapref::entry::Entry::Vacant(vc) => {
                let mut set = HashSet::new();
                set.insert(account);
                vc.insert(set);
            }
        }
    }

    // here if the commitment is processed and the account has changed owner from A->B we keep the key for both A and B
    // then we remove the key from A for finalized commitment
    // returns true if entry needs to be deleted
    async fn update_owner_delete_if_necessary(
        &self,
        prev_account_data: &AccountData,
        new_account_data: &AccountData,
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
                        occ.get_mut().remove(&prev_account_data.pubkey);
                    }
                    dashmap::mapref::entry::Entry::Vacant(_) => {
                        // do nothing
                    }
                }

                // account is deleted or if new does not satisfies the filter criterias
                if Self::is_deleted(new_account_data)
                    || !self.satisfies_filters(new_account_data).await
                {
                    return true;
                }
            }
            if !Self::is_deleted(new_account_data) && self.satisfies_filters(new_account_data).await
            {
                // update owner if account was not deleted but owner was change and the filter criterias are satisfied
                self.add_account_owner(new_account_data.pubkey, new_account_data.account.owner);
            }
        }
        false
    }

    async fn maybe_update_slot_status(
        &self,
        account_data: &AccountData,
        commitment: Commitment,
    ) -> Commitment {
        let slot = account_data.updated_slot;
        let mut lk = self.slots_status.lock().await;
        let slot_status = match lk.get_mut(&slot) {
            Some(x) => x,
            None => {
                lk.insert(
                    slot,
                    SlotStatus {
                        commitment,
                        accounts_updated: HashSet::new(),
                        parent: None,
                    },
                );
                lk.get_mut(&slot).unwrap()
            }
        };
        match commitment {
            Commitment::Processed | Commitment::Confirmed => {
                // insert account into slot status
                slot_status.accounts_updated.insert(account_data.pubkey);
                slot_status.commitment
            }
            Commitment::Finalized => commitment,
        }
    }

    pub async fn satisfies_filters(&self, account: &AccountData) -> bool {
        self.filtered_accounts.satisfies(account).await
    }

    pub fn is_deleted(account: &AccountData) -> bool {
        account.account.lamports == 0
    }

    pub fn account_store_contains_key(&self, pubkey: &Pubkey) -> bool {
        match self.account_store.entry(pubkey.into()) {
            dashmap::mapref::entry::Entry::Occupied(occ) => {
                occ.get().iter().any(|x| x.pubkey == *pubkey)
            }
            dashmap::mapref::entry::Entry::Vacant(_) => false,
        }
    }
}

#[async_trait]
impl AccountStorageInterface for InmemoryAccountStore {
    async fn update_account(&self, account_data: AccountData, commitment: Commitment) -> bool {
        SLOT_FOR_LATEST_ACCOUNT_UPDATE.set(account_data.updated_slot as i64);

        // account is neither deleted, nor tracked, not satifying any filters
        if !Self::is_deleted(&account_data)
            && !self.satisfies_filters(&account_data).await
            && !self.account_store_contains_key(&account_data.pubkey)
        {
            return false;
        }

        // check if the blockhash and slot is already confirmed
        let commitment = self
            .maybe_update_slot_status(&account_data, commitment)
            .await;

        match self.account_store.entry(account_data.pubkey.into()) {
            dashmap::mapref::entry::Entry::Occupied(mut occ) => {
                let account_datas = occ.get_mut();

                for account_data_in_store in account_datas {
                    if account_data.pubkey == account_data_in_store.pubkey {
                        let prev_account = account_data_in_store.get_account_data(commitment);
                        // if account has been updated
                        if account_data_in_store.update(account_data.clone(), commitment) {
                            if let Some(prev_account) = prev_account {
                                if self
                                    .update_owner_delete_if_necessary(
                                        &prev_account,
                                        &account_data,
                                        commitment,
                                    )
                                    .await
                                {
                                    occ.remove_entry();
                                }
                            }
                            return true;
                        } else {
                            return false;
                        }
                    }
                }
                false
            }
            dashmap::mapref::entry::Entry::Vacant(vac) => {
                if self.satisfies_filters(&account_data).await {
                    ACCOUNT_STORED_IN_MEMORY.inc();
                    self.add_account_owner(account_data.pubkey, account_data.account.owner);
                    vac.insert(vec![AccountDataByCommitment::new(
                        account_data.clone(),
                        commitment,
                    )]);
                    true
                } else {
                    false
                }
            }
        }
    }

    async fn initilize_or_update_account(&self, account_data: AccountData) {
        if !Self::is_deleted(&account_data) && !self.satisfies_filters(&account_data).await {
            return;
        }

        self.maybe_update_slot_status(&account_data, Commitment::Finalized)
            .await;

        match self.account_store.entry(account_data.pubkey.into()) {
            dashmap::mapref::entry::Entry::Occupied(mut occ) => {
                {
                    for acc in occ.get_mut() {
                        if acc.pubkey == account_data.pubkey {
                            self.update_account(account_data, Commitment::Finalized)
                                .await;
                            return;
                        }
                    }
                }
                ACCOUNT_STORED_IN_MEMORY.inc();
                self.add_account_owner(account_data.pubkey, account_data.account.owner);
                occ.get_mut()
                    .push(AccountDataByCommitment::initialize(account_data));
            }
            dashmap::mapref::entry::Entry::Vacant(vac) => {
                ACCOUNT_STORED_IN_MEMORY.inc();
                self.add_account_owner(account_data.pubkey, account_data.account.owner);
                vac.insert(vec![AccountDataByCommitment::initialize(account_data)]);
            }
        }
    }

    async fn get_account(
        &self,
        account_pk: Pubkey,
        commitment: Commitment,
    ) -> Result<Option<AccountData>, AccountLoadingError> {
        match self.account_store.entry(account_pk.into()) {
            dashmap::mapref::entry::Entry::Occupied(occ) => {
                let accs = occ.get();
                for acc in accs {
                    if acc.pubkey == account_pk {
                        let acc = acc.get_account_data(commitment);
                        if let Some(account) = &acc {
                            if account.account.lamports > 0 {
                                return Ok(acc);
                            } else {
                                return Ok(None);
                            }
                        } else {
                            return Ok(None);
                        }
                    }
                }
                Ok(None)
            }
            dashmap::mapref::entry::Entry::Vacant(_) => Ok(None),
        }
    }

    async fn get_program_accounts(
        &self,
        program_pubkey: Pubkey,
        account_filters: Option<Vec<AccountFilterType>>,
        commitment: Commitment,
    ) -> Result<Vec<AccountData>, AccountLoadingError> {
        if !self
            .filtered_accounts
            .contains_filter(&AccountFilter {
                accounts: vec![],
                program_id: Some(program_pubkey),
                filters: account_filters.clone(),
            })
            .await
        {
            return Err(AccountLoadingError::ConfigDoesnotContainRequiredFilters);
        }
        if let Some(program_accounts) = self.accounts_by_owner.get(&program_pubkey) {
            let mut return_vec = vec![];
            for program_account in program_accounts.iter() {
                let account_data = self.get_account(*program_account, commitment).await;
                if let Ok(Some(account_data)) = account_data {
                    // recheck program owner and filters
                    if account_data.account.owner.eq(&program_pubkey) {
                        match &account_filters {
                            Some(filters) => {
                                let data = account_data.account.data.data();
                                if filters.iter().all(|filter| filter.allows(&data)) {
                                    return_vec.push(account_data.clone());
                                }
                            }
                            None => {
                                return_vec.push(account_data.clone());
                            }
                        }
                    }
                }
            }
            Ok(return_vec)
        } else {
            Ok(vec![])
        }
    }

    async fn process_slot_data(
        &self,
        slot_info: SlotInfo,
        commitment: Commitment,
    ) -> Vec<AccountData> {
        let slot = slot_info.slot;
        let writable_accounts = {
            let mut lk = self.slots_status.lock().await;
            let mut current_slot = Some((slot, Some(slot_info.parent)));
            let mut writable_accounts: HashMap<Pubkey, Slot> = HashMap::new();
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
                                accounts_updated: HashSet::new(),
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
        for (writable_account, update_slot) in writable_accounts {
            match self.account_store.entry(writable_account.into()) {
                dashmap::mapref::entry::Entry::Occupied(mut occ) => {
                    let mut do_delete = false;
                    for acc in occ.get_mut() {
                        if acc.pubkey == writable_account {
                            if let Some((account_data, prev_account_data)) = acc
                                .promote_slot_commitment(writable_account, update_slot, commitment)
                            {
                                if let Some(prev_account_data) = prev_account_data {
                                    // check if owner has changed
                                    if prev_account_data.account.owner != account_data.account.owner
                                        && self
                                            .update_owner_delete_if_necessary(
                                                &prev_account_data,
                                                &account_data,
                                                commitment,
                                            )
                                            .await
                                    {
                                        do_delete = true;
                                    }

                                    //check if account data has changed
                                    if prev_account_data != account_data {
                                        updated_accounts.push(account_data);
                                    }
                                } else {
                                    // account has been confirmed first time
                                    updated_accounts.push(account_data);
                                }
                            }
                        }
                    }

                    if do_delete {
                        // only has one element i.e it is element that we want to remove
                        if occ.get().len() == 1 {
                            occ.remove_entry();
                        } else {
                            let index = occ
                                .get()
                                .iter()
                                .enumerate()
                                .find(|x| x.1.pubkey == writable_account)
                                .map(|(index, _)| index)
                                .unwrap();
                            occ.get_mut().swap_remove(index);
                        }
                    }
                }
                dashmap::mapref::entry::Entry::Vacant(_) => {
                    // do nothing
                }
            }
        }
        updated_accounts
    }

    async fn create_snapshot(&self, program_id: Pubkey) -> Result<Vec<u8>, AccountLoadingError> {
        let accounts = self
            .get_program_accounts(program_id, None, Commitment::Finalized)
            .await?;
        Ok(bincode::serialize(&accounts).unwrap())
    }

    async fn load_from_snapshot(
        &self,
        _program_id: Pubkey,
        snapshot: Vec<u8>,
    ) -> Result<(), AccountLoadingError> {
        let accounts = bincode::deserialize::<Vec<AccountData>>(&snapshot)
            .map_err(|_| AccountLoadingError::DeserializationIssues)?;
        for account_data in accounts {
            self.initilize_or_update_account(account_data).await;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::{collections::HashSet, sync::Arc};

    use crate::inmemory_account_store::InmemoryAccountStore;
    use itertools::Itertools;
    use lite_account_manager_common::{
        account_data::{Account, AccountData, CompressionMethod},
        account_filter::{AccountFilter, AccountFilters},
        account_filters_interface::AccountFiltersStoreInterface,
        account_store_interface::{AccountLoadingError, AccountStorageInterface},
        commitment::Commitment,
        simple_filter_store::SimpleFilterStore,
        slot_info::SlotInfo,
    };
    use rand::{rngs::ThreadRng, Rng};
    use solana_sdk::{account::Account as SolanaAccount, pubkey::Pubkey, slot_history::Slot};

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

    #[tokio::test]
    pub async fn test_account_store() {
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
        store
            .initilize_or_update_account(account_data_0.clone())
            .await;

        let account_data_1 = create_random_account(&mut rng, 0, pk2, program);

        let mut pubkeys = HashSet::new();
        pubkeys.insert(pk1);
        pubkeys.insert(pk2);

        store
            .initilize_or_update_account(account_data_1.clone())
            .await;

        assert_eq!(
            store.get_account(pk1, Commitment::Processed).await,
            Ok(Some(account_data_0.clone()))
        );
        assert_eq!(
            store.get_account(pk1, Commitment::Confirmed).await,
            Ok(Some(account_data_0.clone()))
        );
        assert_eq!(
            store.get_account(pk1, Commitment::Finalized).await,
            Ok(Some(account_data_0.clone()))
        );

        assert_eq!(
            store.get_account(pk2, Commitment::Processed).await,
            Ok(Some(account_data_1.clone()))
        );
        assert_eq!(
            store.get_account(pk2, Commitment::Confirmed).await,
            Ok(Some(account_data_1.clone()))
        );
        assert_eq!(
            store.get_account(pk2, Commitment::Finalized).await,
            Ok(Some(account_data_1.clone()))
        );

        let account_data_2 = create_random_account(&mut rng, 1, pk1, program);
        let account_data_3 = create_random_account(&mut rng, 2, pk1, program);
        let account_data_4 = create_random_account(&mut rng, 3, pk1, program);
        let account_data_5 = create_random_account(&mut rng, 4, pk1, program);

        store
            .update_account(account_data_2.clone(), Commitment::Processed)
            .await;
        store
            .update_account(account_data_3.clone(), Commitment::Processed)
            .await;
        store
            .update_account(account_data_4.clone(), Commitment::Processed)
            .await;
        store
            .update_account(account_data_5.clone(), Commitment::Processed)
            .await;

        assert_eq!(
            store.get_account(pk1, Commitment::Processed).await,
            Ok(Some(account_data_5.clone()))
        );
        assert_eq!(
            store.get_account(pk1, Commitment::Confirmed).await,
            Ok(Some(account_data_0.clone()))
        );
        assert_eq!(
            store.get_account(pk1, Commitment::Finalized).await,
            Ok(Some(account_data_0.clone()))
        );

        store
            .process_slot_data(new_slot_info(1), Commitment::Confirmed)
            .await;

        assert_eq!(
            store.get_account(pk1, Commitment::Processed).await,
            Ok(Some(account_data_5.clone()))
        );
        assert_eq!(
            store.get_account(pk1, Commitment::Confirmed).await,
            Ok(Some(account_data_2.clone()))
        );
        assert_eq!(
            store.get_account(pk1, Commitment::Finalized).await,
            Ok(Some(account_data_0.clone()))
        );

        store
            .process_slot_data(new_slot_info(2), Commitment::Confirmed)
            .await;

        assert_eq!(
            store.get_account(pk1, Commitment::Processed).await,
            Ok(Some(account_data_5.clone()))
        );
        assert_eq!(
            store.get_account(pk1, Commitment::Confirmed).await,
            Ok(Some(account_data_3.clone()))
        );
        assert_eq!(
            store.get_account(pk1, Commitment::Finalized).await,
            Ok(Some(account_data_0.clone()))
        );

        store
            .process_slot_data(new_slot_info(1), Commitment::Finalized)
            .await;

        assert_eq!(
            store.get_account(pk1, Commitment::Processed).await,
            Ok(Some(account_data_5.clone()))
        );
        assert_eq!(
            store.get_account(pk1, Commitment::Confirmed).await,
            Ok(Some(account_data_3.clone()))
        );
        assert_eq!(
            store.get_account(pk1, Commitment::Finalized).await,
            Ok(Some(account_data_2.clone()))
        );
    }

    #[tokio::test]
    pub async fn test_account_store_if_finalized_clears_old_processed_slots() {
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

        store
            .initilize_or_update_account(create_random_account(&mut rng, 0, pk1, program))
            .await;

        store
            .update_account(
                create_random_account(&mut rng, 1, pk1, program),
                Commitment::Processed,
            )
            .await;
        store
            .update_account(
                create_random_account(&mut rng, 1, pk1, program),
                Commitment::Processed,
            )
            .await;
        store
            .update_account(
                create_random_account(&mut rng, 2, pk1, program),
                Commitment::Processed,
            )
            .await;
        store
            .update_account(
                create_random_account(&mut rng, 3, pk1, program),
                Commitment::Processed,
            )
            .await;
        store
            .update_account(
                create_random_account(&mut rng, 4, pk1, program),
                Commitment::Processed,
            )
            .await;
        store
            .update_account(
                create_random_account(&mut rng, 5, pk1, program),
                Commitment::Processed,
            )
            .await;
        store
            .update_account(
                create_random_account(&mut rng, 6, pk1, program),
                Commitment::Processed,
            )
            .await;
        store
            .update_account(
                create_random_account(&mut rng, 7, pk1, program),
                Commitment::Processed,
            )
            .await;

        let account_8 = create_random_account(&mut rng, 8, pk1, program);
        store
            .update_account(account_8.clone(), Commitment::Processed)
            .await;
        store
            .update_account(
                create_random_account(&mut rng, 9, pk1, program),
                Commitment::Processed,
            )
            .await;
        store
            .update_account(
                create_random_account(&mut rng, 10, pk1, program),
                Commitment::Processed,
            )
            .await;

        let last_account = create_random_account(&mut rng, 11, pk1, program);
        store
            .update_account(last_account.clone(), Commitment::Processed)
            .await;

        assert_eq!(
            store
                .account_store
                .get(&pk1.into())
                .unwrap()
                .iter()
                .map(|x| x.processed_accounts.len())
                .sum::<usize>(),
            12
        );
        store
            .process_slot_data(new_slot_info(11), Commitment::Finalized)
            .await;
        assert_eq!(
            store
                .account_store
                .get(&pk1.into())
                .unwrap()
                .iter()
                .map(|x| x.processed_accounts.len())
                .sum::<usize>(),
            1
        );

        assert_eq!(
            store.get_account(pk1, Commitment::Finalized).await,
            Ok(Some(last_account.clone())),
        );

        // check finalizing previous commitment does not affect
        store
            .process_slot_data(new_slot_info(8), Commitment::Finalized)
            .await;

        assert_eq!(
            store.get_account(pk1, Commitment::Finalized).await,
            Ok(Some(last_account)),
        );
    }

    #[tokio::test]
    pub async fn test_get_program_account() {
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

        store
            .update_account(
                create_random_account(&mut rng, 1, pks[0], prog_1),
                Commitment::Confirmed,
            )
            .await;
        store
            .update_account(
                create_random_account(&mut rng, 1, pks[1], prog_1),
                Commitment::Confirmed,
            )
            .await;
        store
            .update_account(
                create_random_account(&mut rng, 1, pks[2], prog_1),
                Commitment::Confirmed,
            )
            .await;
        store
            .update_account(
                create_random_account(&mut rng, 1, pks[3], prog_1),
                Commitment::Confirmed,
            )
            .await;

        store
            .update_account(
                create_random_account(&mut rng, 1, pks[4], prog_2),
                Commitment::Confirmed,
            )
            .await;

        let acc_prgram_1 = store
            .get_program_accounts(prog_1, None, Commitment::Processed)
            .await;
        assert!(acc_prgram_1.is_ok());
        assert_eq!(acc_prgram_1.unwrap().len(), 4);
        let acc_prgram_1 = store
            .get_program_accounts(prog_1, None, Commitment::Confirmed)
            .await;
        assert!(acc_prgram_1.is_ok());
        assert_eq!(acc_prgram_1.unwrap().len(), 4);
        let acc_prgram_1 = store
            .get_program_accounts(prog_1, None, Commitment::Finalized)
            .await;
        assert!(acc_prgram_1.is_ok());
        assert!(acc_prgram_1.unwrap().is_empty());

        let acc_prgram_2 = store
            .get_program_accounts(prog_2, None, Commitment::Processed)
            .await;
        assert!(acc_prgram_2.is_ok());
        assert_eq!(acc_prgram_2.unwrap().len(), 1);
        let acc_prgram_2 = store
            .get_program_accounts(prog_2, None, Commitment::Confirmed)
            .await;
        assert!(acc_prgram_2.is_ok());
        assert_eq!(acc_prgram_2.unwrap().len(), 1);
        let acc_prgram_2 = store
            .get_program_accounts(prog_2, None, Commitment::Finalized)
            .await;
        assert!(acc_prgram_2.is_ok());
        assert!(acc_prgram_2.unwrap().is_empty());

        let acc_prgram_3 = store
            .get_program_accounts(Pubkey::new_unique(), None, Commitment::Processed)
            .await;
        assert!(acc_prgram_3.is_err());
        let acc_prgram_3 = store
            .get_program_accounts(Pubkey::new_unique(), None, Commitment::Confirmed)
            .await;
        assert!(acc_prgram_3.is_err());
        let acc_prgram_3 = store
            .get_program_accounts(Pubkey::new_unique(), None, Commitment::Finalized)
            .await;
        assert!(acc_prgram_3.is_err());

        store
            .process_slot_data(new_slot_info(1), Commitment::Finalized)
            .await;

        let acc_prgram_1 = store
            .get_program_accounts(prog_1, None, Commitment::Finalized)
            .await;
        assert!(acc_prgram_1.is_ok());
        assert_eq!(acc_prgram_1.unwrap().len(), 4);
        let acc_prgram_2 = store
            .get_program_accounts(prog_2, None, Commitment::Finalized)
            .await;
        assert!(acc_prgram_2.is_ok());
        assert_eq!(acc_prgram_2.unwrap().len(), 1);

        let pk = Pubkey::new_unique();

        let account_finalized = create_random_account(&mut rng, 2, pk, prog_3);
        store
            .update_account(account_finalized.clone(), Commitment::Finalized)
            .await;
        store
            .process_slot_data(new_slot_info(2), Commitment::Finalized)
            .await;

        let account_confirmed = create_random_account(&mut rng, 3, pk, prog_3);
        store
            .update_account(account_confirmed.clone(), Commitment::Confirmed)
            .await;

        let account_processed = create_random_account(&mut rng, 4, pk, prog_4);
        store
            .update_account(account_processed.clone(), Commitment::Processed)
            .await;

        let f = store
            .get_program_accounts(prog_3, None, Commitment::Finalized)
            .await;

        let c = store
            .get_program_accounts(prog_3, None, Commitment::Confirmed)
            .await;

        let p_3 = store
            .get_program_accounts(prog_3, None, Commitment::Processed)
            .await;

        let p_4 = store
            .get_program_accounts(prog_4, None, Commitment::Processed)
            .await;

        assert_eq!(c, Ok(vec![account_confirmed.clone()]));
        assert_eq!(p_3, Ok(vec![]));
        assert_eq!(p_4, Ok(vec![account_processed.clone()]));

        assert_eq!(f, Ok(vec![account_finalized.clone()]));

        store
            .process_slot_data(new_slot_info(3), Commitment::Finalized)
            .await;
        store
            .process_slot_data(new_slot_info(4), Commitment::Confirmed)
            .await;

        let f = store
            .get_program_accounts(prog_3, None, Commitment::Finalized)
            .await;

        let p_3 = store
            .get_program_accounts(prog_3, None, Commitment::Confirmed)
            .await;

        let p_4 = store
            .get_program_accounts(prog_4, None, Commitment::Confirmed)
            .await;

        assert_eq!(f, Ok(vec![account_confirmed.clone()]));
        assert_eq!(p_3, Ok(vec![]));
        assert_eq!(p_4, Ok(vec![account_processed.clone()]));

        store
            .process_slot_data(new_slot_info(4), Commitment::Finalized)
            .await;
        let p_3 = store
            .get_program_accounts(prog_3, None, Commitment::Finalized)
            .await;

        let p_4 = store
            .get_program_accounts(prog_4, None, Commitment::Finalized)
            .await;

        assert_eq!(p_3, Ok(vec![]));
        assert_eq!(p_4, Ok(vec![account_processed.clone()]));
    }

    #[tokio::test]
    pub async fn writing_old_account_state() {
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
        store
            .initilize_or_update_account(account_data_0.clone())
            .await;

        assert_eq!(
            store.get_account(pk1, Commitment::Processed).await,
            Ok(Some(account_data_0.clone()))
        );
        assert_eq!(
            store.get_account(pk1, Commitment::Confirmed).await,
            Ok(Some(account_data_0.clone()))
        );

        // updating state for processed at slot 3
        let account_data_slot_3 = create_random_account(&mut rng, 3, pk1, program);
        store
            .update_account(account_data_slot_3.clone(), Commitment::Processed)
            .await;
        assert_eq!(
            store.get_account(pk1, Commitment::Processed).await,
            Ok(Some(account_data_slot_3.clone()))
        );
        assert_eq!(
            store.get_account(pk1, Commitment::Confirmed).await,
            Ok(Some(account_data_0.clone()))
        );

        // updating state for processed at slot 2
        let account_data_slot_2 = create_random_account(&mut rng, 2, pk1, program);
        store
            .update_account(account_data_slot_2.clone(), Commitment::Processed)
            .await;
        assert_eq!(
            store.get_account(pk1, Commitment::Processed).await,
            Ok(Some(account_data_slot_3.clone()))
        );
        assert_eq!(
            store.get_account(pk1, Commitment::Confirmed).await,
            Ok(Some(account_data_0.clone()))
        );

        // confirming slot 2
        let updates = store
            .process_slot_data(new_slot_info(2), Commitment::Confirmed)
            .await;
        assert_eq!(updates, vec![account_data_slot_2.clone()]);
        assert_eq!(
            store.get_account(pk1, Commitment::Processed).await,
            Ok(Some(account_data_slot_3.clone()))
        );
        assert_eq!(
            store.get_account(pk1, Commitment::Confirmed).await,
            Ok(Some(account_data_slot_2.clone()))
        );
        assert_eq!(
            store.get_account(pk1, Commitment::Finalized).await,
            Ok(Some(account_data_0.clone()))
        );

        // confirming random state at slot 1 / does not do anything as slot 2 has already been confrimed
        let account_data_slot_1 = create_random_account(&mut rng, 1, pk1, program);
        store
            .update_account(account_data_slot_1.clone(), Commitment::Confirmed)
            .await;
        assert_eq!(
            store.get_account(pk1, Commitment::Processed).await,
            Ok(Some(account_data_slot_3.clone()))
        );
        assert_eq!(
            store.get_account(pk1, Commitment::Confirmed).await,
            Ok(Some(account_data_slot_2.clone()))
        );
        assert_eq!(
            store.get_account(pk1, Commitment::Finalized).await,
            Ok(Some(account_data_0.clone()))
        );

        // making slot 3 finalized
        let updates = store
            .process_slot_data(new_slot_info(3), Commitment::Finalized)
            .await;
        assert_eq!(updates, vec![account_data_slot_3.clone()]);
        assert_eq!(
            store.get_account(pk1, Commitment::Processed).await,
            Ok(Some(account_data_slot_3.clone()))
        );
        assert_eq!(
            store.get_account(pk1, Commitment::Confirmed).await,
            Ok(Some(account_data_slot_3.clone()))
        );
        assert_eq!(
            store.get_account(pk1, Commitment::Finalized).await,
            Ok(Some(account_data_slot_3.clone()))
        );

        // making slot 2 finalized
        let updates = store
            .process_slot_data(new_slot_info(2), Commitment::Finalized)
            .await;
        assert!(updates.is_empty());
        assert_eq!(
            store.get_account(pk1, Commitment::Processed).await,
            Ok(Some(account_data_slot_3.clone()))
        );
        assert_eq!(
            store.get_account(pk1, Commitment::Confirmed).await,
            Ok(Some(account_data_slot_3.clone()))
        );
        assert_eq!(
            store.get_account(pk1, Commitment::Finalized).await,
            Ok(Some(account_data_slot_3.clone()))
        );

        // useless old updates
        let account_data_slot_1_2 = create_random_account(&mut rng, 1, pk1, program);
        let account_data_slot_2_2 = create_random_account(&mut rng, 2, pk1, program);
        store
            .update_account(account_data_slot_1_2.clone(), Commitment::Processed)
            .await;
        store
            .update_account(account_data_slot_2_2.clone(), Commitment::Confirmed)
            .await;
        assert_eq!(
            store.get_account(pk1, Commitment::Processed).await,
            Ok(Some(account_data_slot_3.clone()))
        );
        assert_eq!(
            store.get_account(pk1, Commitment::Confirmed).await,
            Ok(Some(account_data_slot_3.clone()))
        );
        assert_eq!(
            store.get_account(pk1, Commitment::Finalized).await,
            Ok(Some(account_data_slot_3.clone()))
        );
    }

    #[tokio::test]
    pub async fn account_states_with_different_write_version() {
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
        store
            .update_account(account_data_10.clone(), Commitment::Processed)
            .await;
        assert_eq!(
            store.get_account(pk1, Commitment::Processed).await,
            Ok(Some(account_data_10.clone()))
        );
        assert_eq!(
            store.get_account(pk1, Commitment::Confirmed).await,
            Ok(None)
        );
        assert_eq!(
            store.get_account(pk1, Commitment::Finalized).await,
            Ok(None)
        );

        // with higher write version process account is updated
        let account_data_11 =
            create_random_account_with_write_version(&mut rng, 1, pk1, program, 11);
        store
            .update_account(account_data_11.clone(), Commitment::Processed)
            .await;
        assert_eq!(
            store.get_account(pk1, Commitment::Processed).await,
            Ok(Some(account_data_11.clone()))
        );
        assert_eq!(
            store.get_account(pk1, Commitment::Confirmed).await,
            Ok(None)
        );
        assert_eq!(
            store.get_account(pk1, Commitment::Finalized).await,
            Ok(None)
        );

        // with lower write version process account is not updated
        let account_data_9 = create_random_account_with_write_version(&mut rng, 1, pk1, program, 9);
        store
            .update_account(account_data_9.clone(), Commitment::Processed)
            .await;
        assert_eq!(
            store.get_account(pk1, Commitment::Processed).await,
            Ok(Some(account_data_11.clone()))
        );
        assert_eq!(
            store.get_account(pk1, Commitment::Confirmed).await,
            Ok(None)
        );
        assert_eq!(
            store.get_account(pk1, Commitment::Finalized).await,
            Ok(None)
        );

        // with finalized commitment all the last account version is taken into account
        store
            .process_slot_data(new_slot_info(1), Commitment::Finalized)
            .await;
        assert_eq!(
            store.get_account(pk1, Commitment::Processed).await,
            Ok(Some(account_data_11.clone()))
        );
        assert_eq!(
            store.get_account(pk1, Commitment::Confirmed).await,
            Ok(Some(account_data_11.clone()))
        );
        assert_eq!(
            store.get_account(pk1, Commitment::Finalized).await,
            Ok(Some(account_data_11.clone()))
        );

        // if the account for slot is updated after with higher account write version both processed and finalized slots are updated
        let account_data_12 =
            create_random_account_with_write_version(&mut rng, 1, pk1, program, 12);
        store
            .update_account(account_data_12.clone(), Commitment::Processed)
            .await;
        assert_eq!(
            store.get_account(pk1, Commitment::Processed).await,
            Ok(Some(account_data_12.clone()))
        );
        assert_eq!(
            store.get_account(pk1, Commitment::Confirmed).await,
            Ok(Some(account_data_12.clone()))
        );
        assert_eq!(
            store.get_account(pk1, Commitment::Finalized).await,
            Ok(Some(account_data_12.clone()))
        );
    }

    #[tokio::test]
    pub async fn test_owner_changes() {
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
        store.initilize_or_update_account(account_1.clone()).await;
        store
            .update_account(account_2.clone(), Commitment::Processed)
            .await;
        store
            .update_account(account_3.clone(), Commitment::Processed)
            .await;
        assert_eq!(
            store.get_account(pk1, Commitment::Finalized).await.unwrap(),
            Some(account_1.clone())
        );
        assert_eq!(
            store.get_account(pk1, Commitment::Confirmed).await.unwrap(),
            Some(account_1.clone())
        );
        assert_eq!(
            store.get_account(pk1, Commitment::Processed).await.unwrap(),
            Some(account_1.clone())
        );
        assert_eq!(
            store.get_account(pk2, Commitment::Processed).await.unwrap(),
            Some(account_2.clone())
        );
        assert_eq!(
            store.get_account(pk2, Commitment::Confirmed).await.unwrap(),
            None
        );
        assert_eq!(
            store.get_account(pk2, Commitment::Finalized).await.unwrap(),
            None
        );
        assert_eq!(
            store.get_account(pk3, Commitment::Processed).await.unwrap(),
            None
        );
        assert_eq!(
            store.get_account(pk3, Commitment::Confirmed).await.unwrap(),
            None
        );
        assert_eq!(
            store.get_account(pk3, Commitment::Processed).await.unwrap(),
            None
        );
        assert_eq!(
            store
                .get_program_accounts(program_1, None, Commitment::Processed)
                .await
                .unwrap(),
            vec![account_1.clone()]
        );
        assert_eq!(
            store
                .get_program_accounts(program_1, None, Commitment::Confirmed)
                .await
                .unwrap(),
            vec![account_1.clone()]
        );
        assert_eq!(
            store
                .get_program_accounts(program_1, None, Commitment::Finalized)
                .await
                .unwrap(),
            vec![account_1.clone()]
        );
        assert_eq!(
            store
                .get_program_accounts(program_2, None, Commitment::Processed)
                .await
                .unwrap(),
            vec![account_2.clone()]
        );
        assert_eq!(
            store
                .get_program_accounts(program_2, None, Commitment::Confirmed)
                .await
                .unwrap(),
            vec![]
        );
        assert_eq!(
            store
                .get_program_accounts(program_2, None, Commitment::Finalized)
                .await
                .unwrap(),
            vec![]
        );
        assert_eq!(
            store
                .get_program_accounts(program_3, None, Commitment::Processed)
                .await,
            Err(AccountLoadingError::ConfigDoesnotContainRequiredFilters)
        );
        assert_eq!(
            store
                .get_program_accounts(program_3, None, Commitment::Confirmed)
                .await,
            Err(AccountLoadingError::ConfigDoesnotContainRequiredFilters)
        );
        assert_eq!(
            store
                .get_program_accounts(program_3, None, Commitment::Finalized)
                .await,
            Err(AccountLoadingError::ConfigDoesnotContainRequiredFilters)
        );

        store
            .process_slot_data(new_slot_info(1), Commitment::Confirmed)
            .await;
        // next slots/ change owner for account1 and account 2
        let account_1_slot2 = create_random_account(&mut rng, 2, pk1, program_2);
        let account_2_slot2 = create_random_account(&mut rng, 2, pk2, program_1);
        store
            .update_account(account_1_slot2.clone(), Commitment::Processed)
            .await;
        store
            .update_account(account_2_slot2.clone(), Commitment::Processed)
            .await;

        assert_eq!(
            store.get_account(pk1, Commitment::Finalized).await.unwrap(),
            Some(account_1.clone())
        );
        assert_eq!(
            store.get_account(pk1, Commitment::Confirmed).await.unwrap(),
            Some(account_1.clone())
        );
        assert_eq!(
            store.get_account(pk1, Commitment::Processed).await.unwrap(),
            Some(account_1_slot2.clone())
        );
        assert_eq!(
            store.get_account(pk2, Commitment::Processed).await.unwrap(),
            Some(account_2_slot2.clone())
        );
        assert_eq!(
            store.get_account(pk2, Commitment::Confirmed).await.unwrap(),
            Some(account_2.clone())
        );
        assert_eq!(
            store.get_account(pk2, Commitment::Finalized).await.unwrap(),
            None
        );
        assert_eq!(
            store
                .get_program_accounts(program_1, None, Commitment::Processed)
                .await
                .unwrap(),
            vec![account_2_slot2.clone()]
        );
        assert_eq!(
            store
                .get_program_accounts(program_1, None, Commitment::Confirmed)
                .await
                .unwrap(),
            vec![account_1.clone()]
        );
        assert_eq!(
            store
                .get_program_accounts(program_1, None, Commitment::Finalized)
                .await
                .unwrap(),
            vec![account_1.clone()]
        );
        assert_eq!(
            store
                .get_program_accounts(program_2, None, Commitment::Processed)
                .await
                .unwrap(),
            vec![account_1_slot2.clone()]
        );
        assert_eq!(
            store
                .get_program_accounts(program_2, None, Commitment::Confirmed)
                .await
                .unwrap(),
            vec![account_2.clone()]
        );
        assert_eq!(
            store
                .get_program_accounts(program_2, None, Commitment::Finalized)
                .await,
            Ok(vec![])
        );

        store
            .process_slot_data(new_slot_info(1), Commitment::Finalized)
            .await;
        store
            .process_slot_data(new_slot_info(2), Commitment::Confirmed)
            .await;
        // next slots/ change owner for account1 and account 2
        assert_eq!(
            store.get_account(pk1, Commitment::Finalized).await.unwrap(),
            Some(account_1.clone())
        );
        assert_eq!(
            store.get_account(pk1, Commitment::Confirmed).await.unwrap(),
            Some(account_1_slot2.clone())
        );
        assert_eq!(
            store.get_account(pk1, Commitment::Processed).await.unwrap(),
            Some(account_1_slot2.clone())
        );
        assert_eq!(
            store.get_account(pk2, Commitment::Processed).await.unwrap(),
            Some(account_2_slot2.clone())
        );
        assert_eq!(
            store.get_account(pk2, Commitment::Confirmed).await.unwrap(),
            Some(account_2_slot2.clone())
        );
        assert_eq!(
            store.get_account(pk2, Commitment::Finalized).await.unwrap(),
            Some(account_2.clone())
        );
        assert_eq!(
            store
                .get_program_accounts(program_1, None, Commitment::Processed)
                .await
                .unwrap(),
            vec![account_2_slot2.clone()]
        );
        assert_eq!(
            store
                .get_program_accounts(program_1, None, Commitment::Confirmed)
                .await
                .unwrap(),
            vec![account_2_slot2.clone()]
        );
        assert_eq!(
            store
                .get_program_accounts(program_1, None, Commitment::Finalized)
                .await
                .unwrap(),
            vec![account_1.clone()]
        );
        assert_eq!(
            store
                .get_program_accounts(program_2, None, Commitment::Processed)
                .await
                .unwrap(),
            vec![account_1_slot2.clone()]
        );
        assert_eq!(
            store
                .get_program_accounts(program_2, None, Commitment::Confirmed)
                .await
                .unwrap(),
            vec![account_1_slot2.clone()]
        );
        assert_eq!(
            store
                .get_program_accounts(program_2, None, Commitment::Finalized)
                .await
                .unwrap(),
            vec![account_2.clone()]
        );

        store
            .process_slot_data(new_slot_info(2), Commitment::Finalized)
            .await;
        assert_eq!(
            store.get_account(pk1, Commitment::Finalized).await.unwrap(),
            Some(account_1_slot2.clone())
        );
        assert_eq!(
            store.get_account(pk1, Commitment::Confirmed).await.unwrap(),
            Some(account_1_slot2.clone())
        );
        assert_eq!(
            store.get_account(pk1, Commitment::Processed).await.unwrap(),
            Some(account_1_slot2.clone())
        );
        assert_eq!(
            store.get_account(pk2, Commitment::Processed).await.unwrap(),
            Some(account_2_slot2.clone())
        );
        assert_eq!(
            store.get_account(pk2, Commitment::Confirmed).await.unwrap(),
            Some(account_2_slot2.clone())
        );
        assert_eq!(
            store.get_account(pk2, Commitment::Finalized).await.unwrap(),
            Some(account_2_slot2.clone())
        );
        assert_eq!(
            store
                .get_program_accounts(program_1, None, Commitment::Processed)
                .await
                .unwrap(),
            vec![account_2_slot2.clone()]
        );
        assert_eq!(
            store
                .get_program_accounts(program_1, None, Commitment::Confirmed)
                .await
                .unwrap(),
            vec![account_2_slot2.clone()]
        );
        assert_eq!(
            store
                .get_program_accounts(program_1, None, Commitment::Finalized)
                .await
                .unwrap(),
            vec![account_2_slot2.clone()]
        );
        assert_eq!(
            store
                .get_program_accounts(program_2, None, Commitment::Processed)
                .await
                .unwrap(),
            vec![account_1_slot2.clone()]
        );
        assert_eq!(
            store
                .get_program_accounts(program_2, None, Commitment::Confirmed)
                .await
                .unwrap(),
            vec![account_1_slot2.clone()]
        );
        assert_eq!(
            store
                .get_program_accounts(program_2, None, Commitment::Finalized)
                .await
                .unwrap(),
            vec![account_1_slot2.clone()]
        );

        // changing user for account 1 and account 2 to program 3 / account 3 in program 1
        let account_1_slot5 = create_random_account(&mut rng, 5, pk1, program_3);
        let account_2_slot5 = create_random_account(&mut rng, 5, pk2, program_3);
        let account_3_slot5 = create_random_account(&mut rng, 5, pk3, program_1);
        store
            .update_account(account_1_slot5.clone(), Commitment::Processed)
            .await;
        store
            .update_account(account_2_slot5.clone(), Commitment::Processed)
            .await;
        store
            .update_account(account_3_slot5.clone(), Commitment::Processed)
            .await;

        assert_eq!(
            store.get_account(pk1, Commitment::Processed).await.unwrap(),
            Some(account_1_slot5.clone())
        );
        assert_eq!(
            store.get_account(pk1, Commitment::Confirmed).await.unwrap(),
            Some(account_1_slot2.clone())
        );
        assert_eq!(
            store.get_account(pk1, Commitment::Finalized).await.unwrap(),
            Some(account_1_slot2.clone())
        );
        assert_eq!(
            store.get_account(pk2, Commitment::Processed).await.unwrap(),
            Some(account_2_slot5.clone())
        );
        assert_eq!(
            store.get_account(pk2, Commitment::Confirmed).await.unwrap(),
            Some(account_2_slot2.clone())
        );
        assert_eq!(
            store.get_account(pk2, Commitment::Finalized).await.unwrap(),
            Some(account_2_slot2.clone())
        );
        assert_eq!(
            store.get_account(pk3, Commitment::Processed).await.unwrap(),
            Some(account_3_slot5.clone())
        );
        assert_eq!(
            store.get_account(pk3, Commitment::Confirmed).await.unwrap(),
            None
        );
        assert_eq!(
            store.get_account(pk3, Commitment::Finalized).await.unwrap(),
            None
        );
        assert_eq!(
            store
                .get_program_accounts(program_1, None, Commitment::Processed)
                .await
                .unwrap(),
            vec![account_3_slot5.clone()]
        );
        assert_eq!(
            store
                .get_program_accounts(program_1, None, Commitment::Confirmed)
                .await
                .unwrap(),
            vec![account_2_slot2.clone()]
        );
        assert_eq!(
            store
                .get_program_accounts(program_1, None, Commitment::Finalized)
                .await
                .unwrap(),
            vec![account_2_slot2.clone()]
        );
        assert_eq!(
            store
                .get_program_accounts(program_2, None, Commitment::Processed)
                .await
                .unwrap(),
            vec![]
        );
        assert_eq!(
            store
                .get_program_accounts(program_2, None, Commitment::Confirmed)
                .await
                .unwrap(),
            vec![account_1_slot2.clone()]
        );
        assert_eq!(
            store
                .get_program_accounts(program_2, None, Commitment::Finalized)
                .await
                .unwrap(),
            vec![account_1_slot2.clone()]
        );
        assert_eq!(
            store
                .get_program_accounts(program_3, None, Commitment::Processed)
                .await,
            Err(AccountLoadingError::ConfigDoesnotContainRequiredFilters)
        );
        assert_eq!(
            store
                .get_program_accounts(program_3, None, Commitment::Confirmed)
                .await,
            Err(AccountLoadingError::ConfigDoesnotContainRequiredFilters)
        );
        assert_eq!(
            store
                .get_program_accounts(program_3, None, Commitment::Finalized)
                .await,
            Err(AccountLoadingError::ConfigDoesnotContainRequiredFilters)
        );

        store
            .process_slot_data(new_slot_info(5), Commitment::Confirmed)
            .await;
        // next slots/ change owner for account1 and account 2
        assert_eq!(
            store.get_account(pk1, Commitment::Processed).await.unwrap(),
            Some(account_1_slot5.clone())
        );
        assert_eq!(
            store.get_account(pk1, Commitment::Confirmed).await.unwrap(),
            Some(account_1_slot5.clone())
        );
        assert_eq!(
            store.get_account(pk1, Commitment::Finalized).await.unwrap(),
            Some(account_1_slot2.clone())
        );
        assert_eq!(
            store.get_account(pk2, Commitment::Processed).await.unwrap(),
            Some(account_2_slot5.clone())
        );
        assert_eq!(
            store.get_account(pk2, Commitment::Confirmed).await.unwrap(),
            Some(account_2_slot5.clone())
        );
        assert_eq!(
            store.get_account(pk2, Commitment::Finalized).await.unwrap(),
            Some(account_2_slot2.clone())
        );
        assert_eq!(
            store.get_account(pk3, Commitment::Processed).await.unwrap(),
            Some(account_3_slot5.clone())
        );
        assert_eq!(
            store.get_account(pk3, Commitment::Confirmed).await.unwrap(),
            Some(account_3_slot5.clone())
        );
        assert_eq!(
            store.get_account(pk3, Commitment::Finalized).await.unwrap(),
            None
        );
        assert_eq!(
            store
                .get_program_accounts(program_1, None, Commitment::Processed)
                .await
                .unwrap(),
            vec![account_3_slot5.clone()]
        );
        assert_eq!(
            store
                .get_program_accounts(program_1, None, Commitment::Confirmed)
                .await
                .unwrap(),
            vec![account_3_slot5.clone()]
        );
        assert_eq!(
            store
                .get_program_accounts(program_1, None, Commitment::Finalized)
                .await
                .unwrap(),
            vec![account_2_slot2.clone()]
        );
        assert_eq!(
            store
                .get_program_accounts(program_2, None, Commitment::Processed)
                .await
                .unwrap(),
            vec![]
        );
        assert_eq!(
            store
                .get_program_accounts(program_2, None, Commitment::Confirmed)
                .await
                .unwrap(),
            vec![]
        );
        assert_eq!(
            store
                .get_program_accounts(program_2, None, Commitment::Finalized)
                .await
                .unwrap(),
            vec![account_1_slot2.clone()]
        );
        assert_eq!(
            store
                .get_program_accounts(program_3, None, Commitment::Processed)
                .await,
            Err(AccountLoadingError::ConfigDoesnotContainRequiredFilters)
        );
        assert_eq!(
            store
                .get_program_accounts(program_3, None, Commitment::Confirmed)
                .await,
            Err(AccountLoadingError::ConfigDoesnotContainRequiredFilters)
        );
        assert_eq!(
            store
                .get_program_accounts(program_3, None, Commitment::Finalized)
                .await,
            Err(AccountLoadingError::ConfigDoesnotContainRequiredFilters)
        );

        // accounts deleted as they do not satisfy filter criterias
        store
            .process_slot_data(new_slot_info(5), Commitment::Finalized)
            .await;
        assert_eq!(
            store.get_account(pk1, Commitment::Processed).await.unwrap(),
            None
        );
        assert_eq!(
            store.get_account(pk1, Commitment::Confirmed).await.unwrap(),
            None
        );
        assert_eq!(
            store.get_account(pk1, Commitment::Finalized).await.unwrap(),
            None
        );
        assert_eq!(
            store.get_account(pk2, Commitment::Processed).await.unwrap(),
            None
        );
        assert_eq!(
            store.get_account(pk2, Commitment::Confirmed).await.unwrap(),
            None
        );
        assert_eq!(
            store.get_account(pk2, Commitment::Finalized).await.unwrap(),
            None
        );
        assert_eq!(
            store.get_account(pk3, Commitment::Processed).await.unwrap(),
            Some(account_3_slot5.clone())
        );
        assert_eq!(
            store.get_account(pk3, Commitment::Confirmed).await.unwrap(),
            Some(account_3_slot5.clone())
        );
        assert_eq!(
            store.get_account(pk3, Commitment::Finalized).await.unwrap(),
            Some(account_3_slot5.clone())
        );
        assert_eq!(
            store
                .get_program_accounts(program_1, None, Commitment::Processed)
                .await
                .unwrap(),
            vec![account_3_slot5.clone()]
        );
        assert_eq!(
            store
                .get_program_accounts(program_1, None, Commitment::Confirmed)
                .await
                .unwrap(),
            vec![account_3_slot5.clone()]
        );
        assert_eq!(
            store
                .get_program_accounts(program_1, None, Commitment::Finalized)
                .await
                .unwrap(),
            vec![account_3_slot5.clone()]
        );
        assert_eq!(
            store
                .get_program_accounts(program_2, None, Commitment::Processed)
                .await
                .unwrap(),
            vec![]
        );
        assert_eq!(
            store
                .get_program_accounts(program_2, None, Commitment::Confirmed)
                .await
                .unwrap(),
            vec![]
        );
        assert_eq!(
            store
                .get_program_accounts(program_2, None, Commitment::Finalized)
                .await
                .unwrap(),
            vec![]
        );
        assert_eq!(
            store
                .get_program_accounts(program_3, None, Commitment::Processed)
                .await,
            Err(AccountLoadingError::ConfigDoesnotContainRequiredFilters)
        );
        assert_eq!(
            store
                .get_program_accounts(program_3, None, Commitment::Confirmed)
                .await,
            Err(AccountLoadingError::ConfigDoesnotContainRequiredFilters)
        );
        assert_eq!(
            store
                .get_program_accounts(program_3, None, Commitment::Finalized)
                .await,
            Err(AccountLoadingError::ConfigDoesnotContainRequiredFilters)
        );

        let account_2_slot6 = create_random_account(&mut rng, 6, pk2, program_1);
        store
            .update_account(account_2_slot6.clone(), Commitment::Processed)
            .await;
        assert_eq!(
            store.get_account(pk1, Commitment::Processed).await.unwrap(),
            None
        );
        assert_eq!(
            store.get_account(pk1, Commitment::Confirmed).await.unwrap(),
            None
        );
        assert_eq!(
            store.get_account(pk1, Commitment::Finalized).await.unwrap(),
            None
        );
        assert_eq!(
            store.get_account(pk2, Commitment::Processed).await.unwrap(),
            Some(account_2_slot6.clone())
        );
        assert_eq!(
            store.get_account(pk2, Commitment::Confirmed).await.unwrap(),
            None
        );
        assert_eq!(
            store.get_account(pk2, Commitment::Finalized).await.unwrap(),
            None
        );
        assert_eq!(
            store.get_account(pk3, Commitment::Processed).await.unwrap(),
            Some(account_3_slot5.clone())
        );
        assert_eq!(
            store.get_account(pk3, Commitment::Confirmed).await.unwrap(),
            Some(account_3_slot5.clone())
        );
        assert_eq!(
            store.get_account(pk3, Commitment::Finalized).await.unwrap(),
            Some(account_3_slot5.clone())
        );
        assert!(store
            .get_program_accounts(program_1, None, Commitment::Processed)
            .await
            .unwrap()
            .contains(&account_2_slot6));
        assert!(store
            .get_program_accounts(program_1, None, Commitment::Processed)
            .await
            .unwrap()
            .contains(&account_3_slot5));
        assert_eq!(
            store
                .get_program_accounts(program_1, None, Commitment::Confirmed)
                .await
                .unwrap(),
            vec![account_3_slot5.clone(),]
        );
        assert_eq!(
            store
                .get_program_accounts(program_1, None, Commitment::Finalized)
                .await
                .unwrap(),
            vec![account_3_slot5.clone(),]
        );
        assert_eq!(
            store
                .get_program_accounts(program_2, None, Commitment::Processed)
                .await
                .unwrap(),
            vec![]
        );
        assert_eq!(
            store
                .get_program_accounts(program_2, None, Commitment::Confirmed)
                .await
                .unwrap(),
            vec![]
        );
        assert_eq!(
            store
                .get_program_accounts(program_2, None, Commitment::Finalized)
                .await
                .unwrap(),
            vec![]
        );
        assert_eq!(
            store
                .get_program_accounts(program_3, None, Commitment::Processed)
                .await,
            Err(AccountLoadingError::ConfigDoesnotContainRequiredFilters)
        );
        assert_eq!(
            store
                .get_program_accounts(program_3, None, Commitment::Confirmed)
                .await,
            Err(AccountLoadingError::ConfigDoesnotContainRequiredFilters)
        );
        assert_eq!(
            store
                .get_program_accounts(program_3, None, Commitment::Finalized)
                .await,
            Err(AccountLoadingError::ConfigDoesnotContainRequiredFilters)
        );

        store
            .process_slot_data(new_slot_info(6), Commitment::Confirmed)
            .await;
        assert_eq!(
            store.get_account(pk1, Commitment::Processed).await.unwrap(),
            None
        );
        assert_eq!(
            store.get_account(pk1, Commitment::Confirmed).await.unwrap(),
            None
        );
        assert_eq!(
            store.get_account(pk1, Commitment::Finalized).await.unwrap(),
            None
        );
        assert_eq!(
            store.get_account(pk2, Commitment::Processed).await.unwrap(),
            Some(account_2_slot6.clone())
        );
        assert_eq!(
            store.get_account(pk2, Commitment::Confirmed).await.unwrap(),
            Some(account_2_slot6.clone())
        );
        assert_eq!(
            store.get_account(pk2, Commitment::Finalized).await.unwrap(),
            None
        );
        assert_eq!(
            store.get_account(pk3, Commitment::Processed).await.unwrap(),
            Some(account_3_slot5.clone())
        );
        assert_eq!(
            store.get_account(pk3, Commitment::Confirmed).await.unwrap(),
            Some(account_3_slot5.clone())
        );
        assert_eq!(
            store.get_account(pk3, Commitment::Finalized).await.unwrap(),
            Some(account_3_slot5.clone())
        );
        assert!(store
            .get_program_accounts(program_1, None, Commitment::Processed)
            .await
            .unwrap()
            .contains(&account_2_slot6));
        assert!(store
            .get_program_accounts(program_1, None, Commitment::Processed)
            .await
            .unwrap()
            .contains(&account_3_slot5));
        assert!(store
            .get_program_accounts(program_1, None, Commitment::Confirmed)
            .await
            .unwrap()
            .contains(&account_2_slot6));
        assert!(store
            .get_program_accounts(program_1, None, Commitment::Confirmed)
            .await
            .unwrap()
            .contains(&account_3_slot5));
        assert_eq!(
            store
                .get_program_accounts(program_1, None, Commitment::Finalized)
                .await
                .unwrap(),
            vec![account_3_slot5.clone(),]
        );
        assert_eq!(
            store
                .get_program_accounts(program_2, None, Commitment::Processed)
                .await
                .unwrap(),
            vec![]
        );
        assert_eq!(
            store
                .get_program_accounts(program_2, None, Commitment::Confirmed)
                .await
                .unwrap(),
            vec![]
        );
        assert_eq!(
            store
                .get_program_accounts(program_2, None, Commitment::Finalized)
                .await
                .unwrap(),
            vec![]
        );
        assert_eq!(
            store
                .get_program_accounts(program_3, None, Commitment::Processed)
                .await,
            Err(AccountLoadingError::ConfigDoesnotContainRequiredFilters)
        );
        assert_eq!(
            store
                .get_program_accounts(program_3, None, Commitment::Confirmed)
                .await,
            Err(AccountLoadingError::ConfigDoesnotContainRequiredFilters)
        );
        assert_eq!(
            store
                .get_program_accounts(program_3, None, Commitment::Finalized)
                .await,
            Err(AccountLoadingError::ConfigDoesnotContainRequiredFilters)
        );

        store
            .process_slot_data(new_slot_info(6), Commitment::Finalized)
            .await;
        assert_eq!(
            store.get_account(pk1, Commitment::Processed).await.unwrap(),
            None
        );
        assert_eq!(
            store.get_account(pk1, Commitment::Confirmed).await.unwrap(),
            None
        );
        assert_eq!(
            store.get_account(pk1, Commitment::Finalized).await.unwrap(),
            None
        );
        assert_eq!(
            store.get_account(pk2, Commitment::Processed).await.unwrap(),
            Some(account_2_slot6.clone())
        );
        assert_eq!(
            store.get_account(pk2, Commitment::Confirmed).await.unwrap(),
            Some(account_2_slot6.clone())
        );
        assert_eq!(
            store.get_account(pk2, Commitment::Finalized).await.unwrap(),
            Some(account_2_slot6.clone())
        );
        assert_eq!(
            store.get_account(pk3, Commitment::Processed).await.unwrap(),
            Some(account_3_slot5.clone())
        );
        assert_eq!(
            store.get_account(pk3, Commitment::Confirmed).await.unwrap(),
            Some(account_3_slot5.clone())
        );
        assert_eq!(
            store.get_account(pk3, Commitment::Finalized).await.unwrap(),
            Some(account_3_slot5.clone())
        );
        assert!(store
            .get_program_accounts(program_1, None, Commitment::Processed)
            .await
            .unwrap()
            .contains(&account_2_slot6));
        assert!(store
            .get_program_accounts(program_1, None, Commitment::Processed)
            .await
            .unwrap()
            .contains(&account_3_slot5));
        assert!(store
            .get_program_accounts(program_1, None, Commitment::Confirmed)
            .await
            .unwrap()
            .contains(&account_2_slot6));
        assert!(store
            .get_program_accounts(program_1, None, Commitment::Confirmed)
            .await
            .unwrap()
            .contains(&account_3_slot5));
        assert!(store
            .get_program_accounts(program_1, None, Commitment::Finalized)
            .await
            .unwrap()
            .contains(&account_2_slot6));
        assert!(store
            .get_program_accounts(program_1, None, Commitment::Finalized)
            .await
            .unwrap()
            .contains(&account_3_slot5));
        assert_eq!(
            store
                .get_program_accounts(program_2, None, Commitment::Processed)
                .await
                .unwrap(),
            vec![]
        );
        assert_eq!(
            store
                .get_program_accounts(program_2, None, Commitment::Confirmed)
                .await
                .unwrap(),
            vec![]
        );
        assert_eq!(
            store
                .get_program_accounts(program_2, None, Commitment::Finalized)
                .await
                .unwrap(),
            vec![]
        );
        assert_eq!(
            store
                .get_program_accounts(program_3, None, Commitment::Processed)
                .await,
            Err(AccountLoadingError::ConfigDoesnotContainRequiredFilters)
        );
        assert_eq!(
            store
                .get_program_accounts(program_3, None, Commitment::Confirmed)
                .await,
            Err(AccountLoadingError::ConfigDoesnotContainRequiredFilters)
        );
        assert_eq!(
            store
                .get_program_accounts(program_3, None, Commitment::Finalized)
                .await,
            Err(AccountLoadingError::ConfigDoesnotContainRequiredFilters)
        );
    }

    #[tokio::test]
    pub async fn test_account_deletions() {
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
        store.initilize_or_update_account(account_1.clone()).await;
        assert_eq!(
            store.get_account(pk1, Commitment::Processed).await.unwrap(),
            Some(account_1.clone())
        );
        assert_eq!(
            store.get_account(pk1, Commitment::Confirmed).await.unwrap(),
            Some(account_1.clone())
        );
        assert_eq!(
            store.get_account(pk1, Commitment::Finalized).await.unwrap(),
            Some(account_1.clone())
        );

        assert_eq!(
            store
                .get_program_accounts(program_1, None, Commitment::Processed)
                .await
                .unwrap(),
            vec![account_1.clone()]
        );
        assert_eq!(
            store
                .get_program_accounts(program_1, None, Commitment::Confirmed)
                .await
                .unwrap(),
            vec![account_1.clone()]
        );
        assert_eq!(
            store
                .get_program_accounts(program_1, None, Commitment::Finalized)
                .await
                .unwrap(),
            vec![account_1.clone()]
        );

        // delete account 1
        let pk2 = Pubkey::new_unique();
        let account_1_2 = create_random_account_with_write_version(&mut rng, 2, pk1, program_1, 2);
        store
            .update_account(account_1_2, Commitment::Processed)
            .await;
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
        store
            .update_account(account_1_3.clone(), Commitment::Processed)
            .await;
        store
            .update_account(random_deletion.clone(), Commitment::Processed)
            .await;

        assert_eq!(
            store.get_account(pk1, Commitment::Processed).await.unwrap(),
            None
        );
        assert_eq!(
            store.get_account(pk1, Commitment::Confirmed).await.unwrap(),
            Some(account_1.clone())
        );
        assert_eq!(
            store.get_account(pk1, Commitment::Finalized).await.unwrap(),
            Some(account_1.clone())
        );
        assert_eq!(
            store.get_account(pk2, Commitment::Processed).await.unwrap(),
            None
        );
        assert_eq!(
            store.get_account(pk2, Commitment::Confirmed).await.unwrap(),
            None
        );
        assert_eq!(
            store.get_account(pk2, Commitment::Finalized).await.unwrap(),
            None
        );

        assert_eq!(
            store
                .get_program_accounts(program_1, None, Commitment::Processed)
                .await
                .unwrap(),
            vec![]
        );
        assert_eq!(
            store
                .get_program_accounts(program_1, None, Commitment::Confirmed)
                .await
                .unwrap(),
            vec![account_1.clone()]
        );
        assert_eq!(
            store
                .get_program_accounts(program_1, None, Commitment::Finalized)
                .await
                .unwrap(),
            vec![account_1.clone()]
        );

        let updated = store
            .process_slot_data(new_slot_info(2), Commitment::Confirmed)
            .await;
        assert_eq!(updated, vec![account_1_3.clone()]);

        assert_eq!(
            store.get_account(pk1, Commitment::Processed).await.unwrap(),
            None
        );
        assert_eq!(
            store.get_account(pk1, Commitment::Confirmed).await.unwrap(),
            None
        );
        assert_eq!(
            store.get_account(pk1, Commitment::Finalized).await.unwrap(),
            Some(account_1.clone())
        );
        assert_eq!(
            store.get_account(pk2, Commitment::Processed).await.unwrap(),
            None
        );
        assert_eq!(
            store.get_account(pk2, Commitment::Confirmed).await.unwrap(),
            None
        );
        assert_eq!(
            store.get_account(pk2, Commitment::Finalized).await.unwrap(),
            None
        );

        assert_eq!(
            store
                .get_program_accounts(program_1, None, Commitment::Processed)
                .await
                .unwrap(),
            vec![]
        );
        assert_eq!(
            store
                .get_program_accounts(program_1, None, Commitment::Confirmed)
                .await
                .unwrap(),
            vec![]
        );
        assert_eq!(
            store
                .get_program_accounts(program_1, None, Commitment::Finalized)
                .await
                .unwrap(),
            vec![account_1.clone()]
        );

        let updated = store
            .process_slot_data(new_slot_info(2), Commitment::Finalized)
            .await;
        assert_eq!(updated, vec![account_1_3]);

        assert_eq!(
            store.get_account(pk1, Commitment::Processed).await.unwrap(),
            None
        );
        assert_eq!(
            store.get_account(pk1, Commitment::Confirmed).await.unwrap(),
            None
        );
        assert_eq!(
            store.get_account(pk1, Commitment::Finalized).await.unwrap(),
            None
        );
        assert_eq!(
            store.get_account(pk2, Commitment::Processed).await.unwrap(),
            None
        );
        assert_eq!(
            store.get_account(pk2, Commitment::Confirmed).await.unwrap(),
            None
        );
        assert_eq!(
            store.get_account(pk2, Commitment::Finalized).await.unwrap(),
            None
        );

        assert_eq!(
            store
                .get_program_accounts(program_1, None, Commitment::Processed)
                .await
                .unwrap(),
            vec![]
        );
        assert_eq!(
            store
                .get_program_accounts(program_1, None, Commitment::Confirmed)
                .await
                .unwrap(),
            vec![]
        );
        assert_eq!(
            store
                .get_program_accounts(program_1, None, Commitment::Finalized)
                .await
                .unwrap(),
            vec![]
        );
    }

    #[tokio::test]
    pub async fn test_snapshot_creation_and_loading() {
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

        store.initilize_or_update_account(account1).await;
        store.initilize_or_update_account(account2).await;
        store.initilize_or_update_account(account3).await;
        store.initilize_or_update_account(account4).await;

        let account1 = create_random_account(&mut rng, 2, pk1, program_1);
        let account2 = create_random_account(&mut rng, 2, pk2, program_1);
        let account3 = create_random_account(&mut rng, 2, pk3, program_2);
        let account4 = create_random_account(&mut rng, 2, pk4, program_2);

        store
            .update_account(account1.clone(), Commitment::Processed)
            .await;
        store
            .update_account(account2.clone(), Commitment::Processed)
            .await;
        store
            .update_account(account3.clone(), Commitment::Processed)
            .await;
        store
            .update_account(account4.clone(), Commitment::Processed)
            .await;
        let account1_bis = create_random_account(&mut rng, 3, pk1, program_1);
        let account3_bis = create_random_account(&mut rng, 3, pk3, program_2);
        store
            .update_account(account1_bis.clone(), Commitment::Processed)
            .await;
        store
            .update_account(account3_bis.clone(), Commitment::Processed)
            .await;

        store
            .process_slot_data(
                SlotInfo {
                    parent: 1,
                    slot: 2,
                    root: 0,
                },
                Commitment::Finalized,
            )
            .await;

        let gpa_1 = store
            .get_program_accounts(program_1, None, Commitment::Processed)
            .await
            .unwrap();
        assert!(
            gpa_1 == vec![account1_bis.clone(), account2.clone()]
                || gpa_1 == vec![account2.clone(), account1_bis.clone()]
        );

        let gpa_2 = store
            .get_program_accounts(program_2, None, Commitment::Processed)
            .await
            .unwrap();
        assert!(
            gpa_2 == vec![account3_bis.clone(), account4.clone()]
                || gpa_2 == vec![account4.clone(), account3_bis.clone()]
        );

        // create snapshot and load
        let snapshot_program_1 = store.create_snapshot(program_1).await.unwrap();
        let snapshot_program_2 = store.create_snapshot(program_2).await.unwrap();

        let snapshot_store = InmemoryAccountStore::new(filter_store);
        // is empty
        let gpa_1 = snapshot_store
            .get_program_accounts(program_1, None, Commitment::Processed)
            .await
            .unwrap();
        assert_eq!(gpa_1, vec![]);

        let gpa_2 = snapshot_store
            .get_program_accounts(program_2, None, Commitment::Processed)
            .await
            .unwrap();
        assert_eq!(gpa_2, vec![]);

        snapshot_store
            .load_from_snapshot(program_1, snapshot_program_1)
            .await
            .unwrap();
        snapshot_store
            .load_from_snapshot(program_2, snapshot_program_2)
            .await
            .unwrap();

        let gpa_1 = snapshot_store
            .get_program_accounts(program_1, None, Commitment::Processed)
            .await
            .unwrap();
        assert!(
            gpa_1 == vec![account1.clone(), account2.clone()]
                || gpa_1 == vec![account2.clone(), account1.clone()]
        );

        let gpa_2 = snapshot_store
            .get_program_accounts(program_2, None, Commitment::Processed)
            .await
            .unwrap();
        assert!(
            gpa_2 == vec![account3.clone(), account4.clone()]
                || gpa_2 == vec![account4.clone(), account3.clone()]
        );
    }
}
