use crate::accountsdb::AccountsDb;
use lite_account_manager_common::account_store_interface::AccountStorageInterface;
use lite_accounts_from_snapshot::{start_import_from_snapshot, Config};
use log::info;
use std::sync::Arc;
use tokio::task::JoinHandle;

pub mod account_data_by_commitment;
pub mod accountsdb;
pub mod inmemory_account_store;
pub mod storage_by_program_id;

pub fn start_backfill_import_from_snapshot(cfg: Config, db: Arc<AccountsDb>) -> JoinHandle<()> {
    tokio::spawn(async move {
        info!("Start importing accounts from recent snapshot");

        let (_jh_importer, mut rx_account_data) = start_import_from_snapshot(cfg);

        while let Some(account) = rx_account_data.recv().await {
            db.initialize_or_update_account(account)
        }

        info!("Finished importing accounts from snapshots")
    })
}
