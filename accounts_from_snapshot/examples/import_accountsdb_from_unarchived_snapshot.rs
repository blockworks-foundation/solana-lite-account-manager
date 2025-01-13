use std::path::PathBuf;
use std::str::FromStr;
use std::sync::Arc;
use std::sync::atomic::AtomicU32;
use std::time::Instant;

use clap::Parser;
use log::info;
use solana_accounts_db::account_storage::AccountStorageMap;
use solana_accounts_db::accounts_db::AtomicAccountsFileId;
use solana_accounts_db::accounts_file::StorageAccess;
use solana_program::hash::Hash;
use solana_runtime::snapshot_archive_info::{FullSnapshotArchiveInfo, SnapshotArchiveInfo};
use solana_runtime::snapshot_bank_utils::bank_from_latest_snapshot_dir;
use solana_runtime::snapshot_hash::SnapshotHash;
use solana_runtime::snapshot_utils::{ArchiveFormat, BankSnapshotInfo, BankSnapshotKind, rebuild_storages_from_snapshot_dir, SnapshotVersion, UnarchivedSnapshot, verify_and_unarchive_snapshots};

use lite_account_manager_common::account_store_interface::AccountStorageInterface;
use lite_account_storage::accountsdb::AccountsDb;
use lite_accounts_from_snapshot::import::import_archive;

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
pub struct Args {
    #[arg(long)]
    pub snapshot_archive_path: String,
}

#[tokio::main]
pub async fn main() {
    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("info")).init();

    // TODO add to other places
    solana_logger::setup_with_default("info");

    let Args {
        snapshot_archive_path,
    } = Args::parse();

    let snapshot_archive_path_file = PathBuf::from_str(&snapshot_archive_path).unwrap();

    let next_append_vec_id = Arc::new(AtomicU32::new(33000000));

    // ~/work/snapshots/snapshot-312734832-8rTnxYEstpNFavGV5syBXJ1SaLphFFCHYpPXdCaEP4dC.tar.zst
    let (
        un,
        un2,
        fields
    ) = verify_and_unarchive_snapshots(
        &PathBuf::from_str("bank_snapshot_dir").unwrap(),
        &FullSnapshotArchiveInfo::new_from_path(snapshot_archive_path_file).unwrap(),
        None, // no incremental snapshot
        &[PathBuf::from_str("accounts-workdir").unwrap()],
        StorageAccess::Mmap,
    ).expect("verify_and_unarchive_snapshots failed");


        // &BankSnapshotInfo {
        //     slot: 0, // what is this?
        //     snapshot_skind: BankSnapshotKind::Pre,
        //     snapshot_dir: snapshot_dir.clone(),
        //     snapshot_version: SnapshotVersion::V1_2_0,
        // },
        // &[PathBuf::from_str("accounts-workdir").unwrap()],
        // next_append_vec_id,
        // StorageAccess::Mmap,
    // ).expect("rebuild_storages_from_snapshot_dir failed");

    // info!("account_storage_map: {:?}", account_storage_map.len());

    // let db = Arc::new(AccountsDb::new());
    //
    //
    // info!("Start importing accounts from full snapshot");
    // let started_at = Instant::now();
    // let (mut accounts_rx, _) =
    //     import_archive(archive_path).await;
    // while let Some(account) = accounts_rx.recv().await {
    //     db.initialize_or_update_account(account)
    // }

    // info!(
    //     "Importing accounts from full snapshot took {:?}",
    //     started_at.elapsed()
    // );
}
