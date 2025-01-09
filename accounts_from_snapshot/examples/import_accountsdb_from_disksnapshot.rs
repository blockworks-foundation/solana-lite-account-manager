use std::cell::OnceCell;
use std::collections::HashMap;
use std::env;
use std::num::NonZeroUsize;
use std::path::PathBuf;
use std::str::FromStr;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering::Relaxed;
use std::sync::{Arc, Once};
use std::thread::spawn;
use std::time::{Instant, SystemTime, UNIX_EPOCH};

use clap::Parser;
use futures::executor::block_on;
use geyser_grpc_connector::grpc_subscription_autoreconnect_tasks::create_geyser_autoconnection_task_with_mpsc;
use geyser_grpc_connector::{GrpcConnectionTimeouts, GrpcSourceConfig, Message};
use log::{info, warn};
use solana_sdk::clock::{Slot, UnixTimestamp};
use solana_sdk::pubkey::Pubkey;
use tokio::sync::mpsc::Receiver;
use tokio::task::{spawn_blocking, JoinHandle};
use tokio::time::{sleep, Duration};
use yellowstone_grpc_proto::geyser::subscribe_update::UpdateOneof;
use yellowstone_grpc_proto::geyser::{
    SubscribeRequest, SubscribeRequestFilterAccounts, SubscribeRequestFilterBlocksMeta,
    SubscribeRequestFilterSlots,
};

use lite_account_manager_common::account_data::{Account, AccountData, Data};
use lite_account_manager_common::account_store_interface::AccountStorageInterface;
use lite_account_storage::accountsdb::AccountsDb;
use lite_accounts_from_snapshot::import::import_archive;
use lite_accounts_from_snapshot::{start_backfill_import_from_snapshot, Config, HostUrl, Loader};

type AtomicSlot = Arc<AtomicU64>;

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
pub struct Args {
    #[arg(long)]
    pub snapshot_archive_path: String,
}

#[tokio::main]
pub async fn main() {
    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("info")).init();

    let Args {
        snapshot_archive_path,
    } = Args::parse();

    let db = Arc::new(AccountsDb::new());

    info!("Start importing accounts from full snapshot");
    let started_at = Instant::now();
    let (mut accounts_rx, _) =
        import_archive(PathBuf::from_str(&snapshot_archive_path).unwrap()).await;
    while let Some(account) = accounts_rx.recv().await {
        db.initialize_or_update_account(account)
    }

    info!(
        "Importing accounts from full snapshot took {:?}",
        started_at.elapsed()
    );
}
