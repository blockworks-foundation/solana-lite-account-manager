use std::env;
use std::num::NonZeroUsize;
use std::path::PathBuf;
use std::str::FromStr;
use log::info;

use solana_runtime::snapshot_archive_info::SnapshotArchiveInfoGetter;
use solana_sdk::epoch_schedule::Slot;

use lite_accounts_from_snapshot::{Config, HostUrl, Loader};

#[tokio::main]
async fn main() {
    env_logger::init_from_env(
        env_logger::Env::default().filter_or(env_logger::DEFAULT_FILTER_ENV, "info"),
    );

    let temp_dir = env::temp_dir();
    let full_snapshot_path = temp_dir.join("full-snapshot");
    let incremental_snapshot_path = temp_dir.join("incremental-snapshot-incr");

    let loader = Loader::new(Config {
        hosts: Box::new([HostUrl::from_str("https://api.testnet.solana.com").unwrap()]),
        not_before_slot: Slot::from(312666355u64),
        full_snapshot_path,
        incremental_snapshot_path,
        maximum_full_snapshot_archives_to_retain: NonZeroUsize::new(100).unwrap(),
        maximum_incremental_snapshot_archives_to_retain: NonZeroUsize::new(100).unwrap(),
    });

    let snapshot = loader.load_latest_incremental_snapshot().await.unwrap();
    print!("{snapshot:#?}");
}
