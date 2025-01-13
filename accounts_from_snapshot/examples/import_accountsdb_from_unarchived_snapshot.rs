use std::path::PathBuf;
use std::str::FromStr;
use std::sync::Arc;
use std::sync::atomic::AtomicU32;

use clap::Parser;
use solana_accounts_db::accounts_file::StorageAccess;
use solana_runtime::snapshot_archive_info::{FullSnapshotArchiveInfo, SnapshotArchiveInfo};
use solana_runtime::snapshot_utils::{ArchiveFormat, BankSnapshotInfo, BankSnapshotKind, rebuild_storages_from_snapshot_dir, SnapshotVersion, UnarchivedSnapshot, verify_and_unarchive_snapshots};


#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
pub struct Args {
    #[arg(long)]
    pub snapshot_archive_path: String,
}

#[tokio::main]
pub async fn main() {
    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("info")).init();
    solana_logger::setup_with_default("info");

    let Args {
        snapshot_archive_path,
    } = Args::parse();

    let snapshot_archive_path_file = PathBuf::from_str(&snapshot_archive_path).unwrap();

    let next_append_vec_id = Arc::new(AtomicU32::new(33000000));

    // ~/work/snapshots/snapshot-312734832-8rTnxYEstpNFavGV5syBXJ1SaLphFFCHYpPXdCaEP4dC.tar.zst
    let (
        unarchived_full_snapshot,
        unarchived_incremental_snapshot,
        next_append_vec_id
    ) = verify_and_unarchive_snapshots(
        &PathBuf::from_str("bank_snapshot_dir").unwrap(),
        &FullSnapshotArchiveInfo::new_from_path(snapshot_archive_path_file).unwrap(),
        None, // no incremental snapshot
        &[PathBuf::from_str("accounts-workdir").unwrap()],
        StorageAccess::Mmap,
    ).expect("verify_and_unarchive_snapshots failed");

}
