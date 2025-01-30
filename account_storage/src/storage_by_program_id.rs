use lite_account_manager_common::{
    account_data::AccountData,
    account_filter::AccountFilterType,
    account_store_interface::{AccountLoadingError, AccountStorageInterface},
    commitment::Commitment,
    slot_info::SlotInfo,
};
use solana_sdk::pubkey::Pubkey;
use std::{collections::HashMap, sync::Arc};

pub struct StorageByProgramId {
    program_id_dispatch_map: HashMap<Pubkey, Arc<dyn AccountStorageInterface>>,
    default: Arc<dyn AccountStorageInterface>,
}

impl StorageByProgramId {
    pub fn new(
        program_id_dispatch_map: HashMap<Pubkey, Arc<dyn AccountStorageInterface>>,
        default: Arc<dyn AccountStorageInterface>,
    ) -> Self {
        Self {
            program_id_dispatch_map,
            default,
        }
    }
}

impl AccountStorageInterface for StorageByProgramId {
    fn update_account(&self, account_data: AccountData, commitment: Commitment) -> bool {
        for (_, interface) in self.program_id_dispatch_map.iter() {
            if interface.update_account(account_data.clone(), commitment) {
                return true;
            }
        }
        self.default.update_account(account_data, commitment)
    }

    fn initialize_or_update_account(&self, account_data: AccountData) {
        match self
            .program_id_dispatch_map
            .get(&account_data.account.owner)
        {
            Some(val) => val.initialize_or_update_account(account_data),
            None => self.default.initialize_or_update_account(account_data),
        }
    }

    fn get_account(
        &self,
        account_pk: Pubkey,
        commitment: Commitment,
    ) -> Result<Option<AccountData>, AccountLoadingError> {
        for (_, interface) in self.program_id_dispatch_map.iter() {
            if let Ok(accounts) = interface.get_account(account_pk, commitment) {
                return Ok(accounts);
            }
        }
        self.default.get_account(account_pk, commitment)
    }

    fn get_program_accounts(
        &self,
        program_pubkey: Pubkey,
        account_filter: Option<Vec<AccountFilterType>>,
        commitment: Commitment,
    ) -> Result<Vec<AccountData>, AccountLoadingError> {
        match self.program_id_dispatch_map.get(&program_pubkey) {
            Some(occ) => occ.get_program_accounts(program_pubkey, account_filter, commitment),
            None => self
                .default
                .get_program_accounts(program_pubkey, account_filter, commitment),
        }
    }

    fn process_slot_data(&self, slot_info: SlotInfo, commitment: Commitment) {
        for (_, interface) in self.program_id_dispatch_map.iter() {
            interface.process_slot_data(slot_info, commitment);
        }
        self.default.process_slot_data(slot_info, commitment);
    }

    // snapshot should always be created at finalized slot
    fn create_snapshot(&self, program_id: Pubkey) -> Result<Vec<u8>, AccountLoadingError> {
        match self.program_id_dispatch_map.get(&program_id) {
            Some(occ) => occ.create_snapshot(program_id),
            None => self.default.create_snapshot(program_id),
        }
    }

    fn load_from_snapshot(
        &self,
        program_id: Pubkey,
        snapshot: Vec<u8>,
    ) -> Result<(), AccountLoadingError> {
        match self.program_id_dispatch_map.get(&program_id) {
            Some(occ) => occ.load_from_snapshot(program_id, snapshot),
            None => self.default.load_from_snapshot(program_id, snapshot),
        }
    }
}
