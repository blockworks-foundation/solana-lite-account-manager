use std::num::NonZeroUsize;
use std::path::PathBuf;
use std::str::FromStr;

use solana_runtime::snapshot_archive_info::SnapshotArchiveInfoGetter;
use solana_sdk::epoch_schedule::Slot;

use lite_accounts_from_storage::{Config, HostUrl, Loader};

pub struct TestConsumer {}

#[tokio::main]
async fn main() {
    let loader = Loader::new(Config {
        hosts: Box::new([ HostUrl::from_str("https://api.testnet.solana.com").unwrap()]),
        not_before_slot: Slot::from(0u64),
        full_snapshot_path: PathBuf::from_str("/tmp/lite-full").unwrap(),
        incremental_snapshot_path: PathBuf::from_str("/tmp/lite-incr").unwrap(),
        maximum_full_snapshot_archives_to_retain: NonZeroUsize::new(100).unwrap(),
        maximum_incremental_snapshot_archives_to_retain: NonZeroUsize::new(100).unwrap(),
    });

    let snapshot = loader.load_latest_incremental_snapshot().await.unwrap();
    print!("{snapshot:#?}");
}
