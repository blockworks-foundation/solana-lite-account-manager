use std::fmt::{Display, Formatter};
use std::num::NonZeroUsize;
use std::path::PathBuf;

use log::{debug, info};
use solana_sdk::clock::Slot;
use tokio::sync::mpsc::{Receiver, Sender};
use tokio::task::JoinHandle;

pub use download::*;
use lite_account_manager_common::account_data::AccountData;
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
pub mod debouncer_instant;
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

pub fn start_import_from_snapshot(cfg: Config) -> (JoinHandle<()>, Receiver<AccountData>) {
    let (tx, rx) = tokio::sync::mpsc::channel(1024);
    let jh = tokio::spawn(async move {
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
        let (accounts_rx, _) = import_archive(full_snapshot_archive_file).await;
        forward(&tx, accounts_rx).await;

        info!("Start importing accounts from incremental snapshot");
        let (accounts_rx, _) = import_archive(incremental_snapshot_archive_dir).await;
        forward(&tx, accounts_rx).await;

        info!("Finished importing accounts from snapshots")
    });

    (jh, rx)
}

async fn forward(tx: &Sender<AccountData>, mut accounts_rx: Receiver<AccountData>) {
    while let Some(account) = accounts_rx.recv().await {
        match tx.send(account).await {
            Ok(()) => {}
            Err(_) => {
                // receiver dropped
                debug!("Receiver dropped, stopping import");
                break;
            }
        }
    }
}
