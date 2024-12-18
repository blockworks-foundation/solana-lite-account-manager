use std::fmt::{Display, Formatter};
use std::num::NonZeroUsize;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use log::{info, warn};
use solana_sdk::clock::Slot;
use tokio::task::JoinHandle;
use tokio::time::sleep;

use {
    crate::solana::{
        AccountsDbFields, DeserializableVersionedBank, deserialize_from,
        SerializableAccountStorageEntry,
    },
    std::str::FromStr,
};
pub use download::*;
use lite_account_manager_common::account_store_interface::AccountStorageInterface;
use lite_account_storage::accountsdb::AccountsDb;

use crate::import::import_archive;

mod append_vec;
mod archived;
pub(crate) mod import;
mod solana;
mod download;
mod find;
mod core;

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

pub fn import(cfg: Config, db: Arc<AccountsDb>) -> JoinHandle<()> {
    tokio::spawn(async move {
        let loader = Loader::new(cfg);

        let incremental_snapshot = loop {
            match loader.load_latest_incremental_snapshot().await {
                Ok(snapshot) => break snapshot,
                Err(e) => {
                    warn!("Unable to download incremental snapshot: {}", e.to_string());
                    sleep(Duration::from_secs(30)).await;
                }
            }
        };
        info!("{incremental_snapshot:#?}");

        let full_snapshot = loop {
            match loader.load_latest_snapshot().await {
                Ok(snapshot) => break snapshot,
                Err(e) => {
                    warn!("Unable to download full snapshot: {}", e.to_string());
                    sleep(Duration::from_secs(30)).await;
                }
            }
        };
        info!("{full_snapshot:#?}");

        info!("Start importing accounts from full snapshot");
        let (mut accounts_rx, _) = import_archive(full_snapshot.path).await;
        while let Some(account) = accounts_rx.recv().await {
            db.initilize_or_update_account(account)
        }

        info!("Start importing accounts from incremental snapshot");
        let (mut accounts_rx, _) = import_archive(incremental_snapshot.path).await;
        while let Some(account) = accounts_rx.recv().await {
            db.initilize_or_update_account(account)
        }

        info!("Finished importing accounts from snapshots")
    })
}



