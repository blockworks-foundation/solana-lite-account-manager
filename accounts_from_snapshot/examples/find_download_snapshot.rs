use std::env;
use std::num::NonZeroUsize;
use std::str::FromStr;

use solana_sdk::epoch_schedule::Slot;

use lite_accounts_from_snapshot::{Config, HostUrl, Loader};

#[tokio::main]
async fn main() {
    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("info")).init();

    let temp_dir = env::temp_dir();
    let full_snapshot_path = temp_dir.join("full-snapshot");
    let incremental_snapshot_path = temp_dir.join("incremental-snapshot-incr");

    let loader = Loader::new(Config {
        hosts: Box::new([HostUrl::from_str("https://api.testnet.solana.com").unwrap()]),
        not_before_slot: Slot::from(9999u64),
        full_snapshot_path,
        incremental_snapshot_path,
        maximum_full_snapshot_archives_to_retain: NonZeroUsize::new(100).unwrap(),
        maximum_incremental_snapshot_archives_to_retain: NonZeroUsize::new(100).unwrap(),
    });

    let _snapshot = loader.find_and_load().await;
    // print!("{_snapshot:#?}");
}

/*
```
[2025-01-08T15:49:27Z INFO  lite_accounts_from_snapshot::download] FullSnapshot {
        host: HostUrl(
            "https://api.testnet.solana.com",
        ),
        slot: 311178098,
        hash: SnapshotHash(
            CSG4mvwaNQaMqtFb88pE3S2WTmqtR8Dyagz8xRdPic1r,
        ),
    }
[2025-01-08T15:49:27Z INFO  lite_accounts_from_snapshot::download] IncrementalSnapshot {
        host: HostUrl(
            "https://api.testnet.solana.com",
        ),
        full_slot: 311178098,
        incremental_slot: 311191827,
        hash: SnapshotHash(
            4Y5QvsG1XRKxruP6x2jKJoSXdWgF3u6zeH5HCsegEAkQ,
        ),
    }
```
 */
