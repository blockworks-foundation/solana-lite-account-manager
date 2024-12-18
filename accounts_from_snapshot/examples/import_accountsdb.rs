use std::collections::HashMap;
use std::env;
use std::num::NonZeroUsize;
use std::path::PathBuf;
use std::str::FromStr;
use std::sync::{Arc, Once};
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering::Relaxed;
use std::time::{SystemTime, UNIX_EPOCH};

use futures::executor::block_on;
use geyser_grpc_connector::{GrpcConnectionTimeouts, GrpcSourceConfig, Message};
use geyser_grpc_connector::grpc_subscription_autoreconnect_tasks::create_geyser_autoconnection_task_with_mpsc;
use log::{info, warn};
use solana_sdk::clock::{Slot, UnixTimestamp};
use solana_sdk::pubkey::Pubkey;
use tokio::sync::mpsc::Receiver;
use tokio::sync::OnceCell;
use tokio::task::{JoinHandle, spawn_blocking};
use tokio::time::{Duration, sleep};
use yellowstone_grpc_proto::geyser::{
    SubscribeRequest, SubscribeRequestFilterAccounts, SubscribeRequestFilterBlocksMeta,
    SubscribeRequestFilterSlots,
};
use yellowstone_grpc_proto::geyser::subscribe_update::UpdateOneof;

use lite_account_manager_common::account_data::{Account, AccountData, Data};
use lite_account_manager_common::account_store_interface::AccountStorageInterface;
use lite_account_storage::accountsdb::AccountsDb;
use lite_accounts_from_storage::{Config, HostUrl, import, Loader};

type AtomicSlot = Arc<AtomicU64>;

#[tokio::main]
pub async fn main() {
     tracing_subscriber::fmt::init();

    let grpc_addr = env::var("GRPC_ADDR").expect("need grpc url");
    let grpc_x_token = env::var("GRPC_X_TOKEN").ok();

    info!("Using grpc source on {} ({})",grpc_addr,grpc_x_token.is_some());

    let timeouts = GrpcConnectionTimeouts {
        connect_timeout: Duration::from_secs(25),
        request_timeout: Duration::from_secs(25),
        subscribe_timeout: Duration::from_secs(25),
        receive_timeout: Duration::from_secs(25),
    };

    let config = GrpcSourceConfig::new(grpc_addr, grpc_x_token, None, timeouts.clone());

    let (autoconnect_tx, geyser_rx) = tokio::sync::mpsc::channel(10);
    let (_exit_tx, exit_rx) = tokio::sync::broadcast::channel::<()>(1);

    let _all_accounts = create_geyser_autoconnection_task_with_mpsc(
        config.clone(),
        all_accounts(),
        autoconnect_tx.clone(),
        exit_rx.resubscribe(),
    );

    static IMPORT_SNAPSHOT_ONCE: Once = Once::new();
    let db = Arc::new(AccountsDb::new());
    let mut accounts_rx = account_stream(geyser_rx);

    loop {
        let account = accounts_rx.recv().await.unwrap();
        let slot = account.updated_slot;

        IMPORT_SNAPSHOT_ONCE.call_once(|| { import_snapshots(slot, db.clone()) });
        db.initilize_or_update_account(account);
    }
}

fn import_snapshots(slot: Slot, db: Arc<AccountsDb>) {
    let config = Config {
        hosts: vec![
            HostUrl::from_str("http://147.28.178.75:8899").unwrap(),
            HostUrl::from_str("http://204.13.239.110:8899").unwrap(),
            HostUrl::from_str("http://149.50.110.119:8899").unwrap(),
            HostUrl::from_str("http://146.59.54.19:8899").unwrap(),
            HostUrl::from_str("http://74.50.77.158:80").unwrap(),
            HostUrl::from_str("http://149.50.104.41:8899").unwrap(),
            HostUrl::from_str("http://205.209.109.158:8899").unwrap(),
        ].into_boxed_slice(),
        not_before_slot: slot,
        full_snapshot_path: PathBuf::from_str("/tmp/full-snapshot").unwrap(),
        incremental_snapshot_path: PathBuf::from_str("/tmp/incremental-snapshot").unwrap(),
        maximum_full_snapshot_archives_to_retain: NonZeroUsize::new(10).unwrap(),
        maximum_incremental_snapshot_archives_to_retain: NonZeroUsize::new(10).unwrap(),
    };
    let _ = import(config, db);
}


 fn account_stream(mut geyser_messages_rx: Receiver<Message>) -> Receiver<AccountData> {
    let (accounts_tx, result) = tokio::sync::mpsc::channel::<AccountData>(10);

    tokio::spawn(async move {
        loop {
            match geyser_messages_rx.recv().await {
                Some(Message::GeyserSubscribeUpdate(update)) => match update.update_oneof {
                    Some(UpdateOneof::Account(update)) => {
                        let info = update.account.unwrap();
                        let slot = update.slot;
                        accounts_tx.send(AccountData {
                            pubkey: Pubkey::try_from(info.pubkey).unwrap(),
                            account: Arc::new(Account {
                                lamports: info.lamports,
                                data: Data::Uncompressed(info.data),
                                owner: Pubkey::try_from(info.owner).unwrap(),
                                executable: info.executable,
                                rent_epoch: info.rent_epoch,
                            }),
                            updated_slot: slot,
                            write_version: info.write_version,
                        }).await.expect("Failed to send account");
                    }
                    None => {}
                    _ => {}
                },
                None => {
                    log::warn!("multiplexer channel closed - aborting");
                    return;
                }
                Some(Message::Connecting(_)) => {}
            }
        }
    });

    return result;
}

pub fn all_accounts() -> SubscribeRequest {
    let mut accounts_subs = HashMap::new();
    accounts_subs.insert(
        "client".to_string(),
        SubscribeRequestFilterAccounts {
            account: vec![],
            owner: vec![],
            filters: vec![],
        },
    );

    SubscribeRequest {
        accounts: accounts_subs,
        ..Default::default()
    }
}
