use std::path::PathBuf;
use std::str::FromStr;
use std::sync::atomic::AtomicU32;
use std::sync::Arc;

use clap::Parser;
use log::info;
use solana_accounts_db::accounts_file::StorageAccess;
use solana_runtime::snapshot_archive_info::{FullSnapshotArchiveInfo, SnapshotArchiveInfo};
use solana_runtime::snapshot_utils::{
    rebuild_storages_from_snapshot_dir, verify_and_unarchive_snapshots, ArchiveFormat,
    BankSnapshotInfo, BankSnapshotKind, SnapshotVersion, UnarchivedSnapshot,
};

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
pub struct Args {
    /// path to snapshot archive file (e.g. snapshot-312734832-8rTnxYEstpNFavGV5syBXJ1SaLphFFCHYpPXdCaEP4dC.tar.zst)
    #[arg(long)]
    pub snapshot_archive_path: String,

    /// where to unpack snapshot to; directory must exist
    #[arg(long)]
    pub snapshot_dir: String,

    /// where to build the accounts db; directory must exist
    #[arg(long)]
    pub accounts_path: String,
}

#[tokio::main]
pub async fn main() {
    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("info")).init();
    solana_logger::setup_with_default("info");

    let Args {
        snapshot_archive_path,
        snapshot_dir,
        accounts_path,
    } = Args::parse();

    assert!(
        PathBuf::from_str(&snapshot_dir).unwrap().is_dir(),
        "snapshot_dir must exist and must be a directory"
    );
    assert!(
        PathBuf::from_str(&accounts_path).unwrap().is_dir(),
        "accounts_path must exist and must be a directory"
    );

    let snapshot_archive_path_file = PathBuf::from_str(&snapshot_archive_path).unwrap();
    let accounts_path = PathBuf::from_str(&accounts_path).unwrap();

    let next_append_vec_id = Arc::new(AtomicU32::new(33000000));

    info!(
        "Loading from snapshot archive {}, unpacking to {} with accounts in {}",
        snapshot_archive_path_file.display(),
        snapshot_dir,
        accounts_path.display()
    );

    // ~/work/snapshots/snapshot-312734832-8rTnxYEstpNFavGV5syBXJ1SaLphFFCHYpPXdCaEP4dC.tar.zst
    let (unarchived_full_snapshot, unarchived_incremental_snapshot, next_append_vec_id) =
        verify_and_unarchive_snapshots(
            &snapshot_dir,
            &FullSnapshotArchiveInfo::new_from_path(snapshot_archive_path_file).unwrap(),
            None, // no incremental snapshot
            &[accounts_path],
            StorageAccess::Mmap,
        )
        .expect("verify_and_unarchive_snapshots failed");

    info!(
        "unarchived_full_snapshot: {:?}",
        unarchived_full_snapshot.storage.len()
    );
}
