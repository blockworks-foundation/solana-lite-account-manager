use std::{collections::HashMap, sync::Arc};

use async_trait::async_trait;
use lite_account_manager_common::{
    account_data::AccountData,
    account_filter::AccountFilterType,
    account_store_interface::{AccountLoadingError, AccountStorageInterface},
    commitment::Commitment,
    slot_info::SlotInfo,
};
use solana_sdk::pubkey::Pubkey;

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

#[async_trait]
impl AccountStorageInterface for StorageByProgramId {
    async fn update_account(&self, account_data: AccountData, commitment: Commitment) -> bool {
        for (_, interface) in self.program_id_dispatch_map.iter() {
            if interface
                .update_account(account_data.clone(), commitment)
                .await
            {
                return true;
            }
        }
        self.default.update_account(account_data, commitment).await
    }

    async fn initilize_or_update_account(&self, account_data: AccountData) {
        match self
            .program_id_dispatch_map
            .get(&account_data.account.owner)
        {
            Some(val) => val.initilize_or_update_account(account_data).await,
            None => self.default.initilize_or_update_account(account_data).await,
        }
    }

    async fn get_account(
        &self,
        account_pk: Pubkey,
        commitment: Commitment,
    ) -> Result<Option<AccountData>, AccountLoadingError> {
        for (_, interface) in self.program_id_dispatch_map.iter() {
            if let Ok(accounts) = interface.get_account(account_pk, commitment).await {
                return Ok(accounts);
            }
        }
        self.default.get_account(account_pk, commitment).await
    }

    async fn get_program_accounts(
        &self,
        program_pubkey: Pubkey,
        account_filter: Option<Vec<AccountFilterType>>,
        commitment: Commitment,
    ) -> Result<Vec<AccountData>, AccountLoadingError> {
        match self.program_id_dispatch_map.get(&program_pubkey) {
            Some(occ) => {
                occ.get_program_accounts(program_pubkey, account_filter, commitment)
                    .await
            }
            None => {
                self.default
                    .get_program_accounts(program_pubkey, account_filter, commitment)
                    .await
            }
        }
    }

    async fn process_slot_data(
        &self,
        slot_info: SlotInfo,
        commitment: Commitment,
    ) -> Vec<AccountData> {
        let mut return_vec = vec![];
        for (_, interface) in self.program_id_dispatch_map.iter() {
            let mut t = interface.process_slot_data(slot_info, commitment).await;
            return_vec.append(&mut t);
        }
        let mut t = self.default.process_slot_data(slot_info, commitment).await;
        return_vec.append(&mut t);
        return_vec
    }

    // snapshot should always be created at finalized slot
    async fn create_snapshot(&self, program_id: Pubkey) -> Result<Vec<u8>, AccountLoadingError> {
        match self.program_id_dispatch_map.get(&program_id) {
            Some(occ) => occ.create_snapshot(program_id).await,
            None => self.default.create_snapshot(program_id).await,
        }
    }

    async fn load_from_snapshot(
        &self,
        program_id: Pubkey,
        snapshot: Vec<u8>,
    ) -> Result<(), AccountLoadingError> {
        match self.program_id_dispatch_map.get(&program_id) {
            Some(occ) => occ.load_from_snapshot(program_id, snapshot).await,
            None => self.default.load_from_snapshot(program_id, snapshot).await,
        }
    }
}
