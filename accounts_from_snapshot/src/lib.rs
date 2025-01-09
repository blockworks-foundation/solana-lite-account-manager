use std::fmt::{Display, Formatter};
use std::num::NonZeroUsize;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use log::{info, warn};
use solana_sdk::clock::Slot;
use tokio::task::JoinHandle;
use tokio::time::sleep;

pub use download::*;
use lite_account_manager_common::account_store_interface::AccountStorageInterface;
use lite_account_storage::accountsdb::AccountsDb;
use {
    crate::solana::{
        deserialize_from, AccountsDbFields, DeserializableVersionedBank,
        SerializableAccountStorageEntry,
    },
    std::str::FromStr,
};

use crate::import::import_archive;

mod append_vec;
mod archived;
mod core;
mod download;
mod find;
pub mod import;
mod solana;

#[derive(Clone, Debug)]
pub struct HostUrl(String);

impl Display for HostUrl {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl FromStr for HostUrl {
    type Err = ();
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Ok(HostUrl(s.to_string()))
    }
}

pub struct Config {
    pub hosts: Box<[HostUrl]>,
    pub not_before_slot: Slot,
    pub full_snapshot_path: PathBuf,
    pub incremental_snapshot_path: PathBuf,
    pub maximum_full_snapshot_archives_to_retain: NonZeroUsize,
    pub maximum_incremental_snapshot_archives_to_retain: NonZeroUsize,
}

pub fn start_backfill_import_from_snapshot(cfg: Config, db: Arc<AccountsDb>) -> JoinHandle<()> {
    tokio::spawn(async move {
        let loader = Loader::new(cfg);

        // TODO use code from find_and_load

        // propagate error
        let SnapshotArchives {
            full_snapshot_archive_file,
            incremental_snapshot_archive_dir,
        } = loader
            .find_and_load()
            .await
            .expect("Failed to find and load snapshots");

        info!("Start importing accounts from full snapshot");
        let (mut accounts_rx, _) = import_archive(full_snapshot_archive_file).await;
        while let Some(account) = accounts_rx.recv().await {
            db.initilize_or_update_account(account)
        }

        info!("Start importing accounts from incremental snapshot");
        let (mut accounts_rx, _) = import_archive(incremental_snapshot_archive_dir).await;
        while let Some(account) = accounts_rx.recv().await {
            db.initilize_or_update_account(account)
        }

        info!("Finished importing accounts from snapshots")
    })
}
