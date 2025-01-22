use std::num::NonZeroUsize;
use std::str::FromStr;
use std::time::Duration;
use std::{env, sync::Arc};

use geyser_grpc_connector::grpc_subscription_autoreconnect_tasks::create_geyser_autoconnection_task_with_mpsc;
use geyser_grpc_connector::{GrpcConnectionTimeouts, GrpcSourceConfig};
use log::{debug, info};
use solana_sdk::clock::Slot;
use tokio::sync::mpsc::Receiver;
use tokio::task::JoinHandle;

use crate::grpc_source::{all_accounts, process_stream};
use lite_account_manager_common::slot_info::SlotInfoWithCommitment;
use lite_account_manager_common::{
    account_data::AccountData, account_store_interface::AccountStorageInterface,
    commitment::Commitment,
};
use lite_account_storage::accountsdb::AccountsDb;
use lite_account_storage::start_backfill_import_from_snapshot;
use lite_accounts_from_snapshot::{Config, HostUrl};

use crate::rpc_server::RpcServerImpl;

mod cli;
mod grpc_source;
mod rpc_server;

#[tokio::main(worker_threads = 2)]
async fn main() {
    tracing_subscriber::fmt::init();

    let grpc_addr = env::var("GRPC_ADDR").expect("need grpc url");
    let grpc_x_token = env::var("GRPC_X_TOKEN").ok();

    info!(
        "Using grpc source on {} ({})",
        grpc_addr,
        grpc_x_token.is_some()
    );

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

    let db = Arc::new(AccountsDb::new());

    let (mut slots_rx, accounts_rx) = process_stream(geyser_rx);
    process_account_updates(db.clone(), accounts_rx);

    info!("Waiting for most recent finalised block");
    let slot = loop {
        let slot = slots_rx.recv().await.unwrap();
        debug!("slot {} - {}", slot.info.slot, slot.commitment);
        if slot.commitment == Commitment::Finalized {
            break slot;
        }
    };

    process_slot_updates(db.clone(), slots_rx);
    start_backfill(slot.info.slot, db.clone());

    info!("Storage initialized with snapshot");

    let rpc_server = RpcServerImpl::new(db.clone(), None);
    let rpc_server_handle = rpc_server.start_serving("[::]:10700").await.unwrap();
    rpc_server_handle.stopped().await;
    log::error!("RPC HTTP server stopped");
}

fn process_slot_updates(
    db: Arc<AccountsDb>,
    mut slots_rx: Receiver<SlotInfoWithCommitment>,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        loop {
            let slot = slots_rx.recv().await.unwrap();
            db.process_slot_data(slot.info, slot.commitment);
        }
    })
}

fn process_account_updates(
    db: Arc<AccountsDb>,
    mut accounts_rx: Receiver<AccountData>,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        loop {
            let account = accounts_rx.recv().await.unwrap();
            db.initialize_or_update_account(account);
        }
    })
}

fn start_backfill(not_before_slot: Slot, db: Arc<AccountsDb>) -> JoinHandle<()> {
    let temp_dir = env::temp_dir();
    let full_snapshot_path = temp_dir.join("full-snapshot");
    let incremental_snapshot_path = temp_dir.join("incremental-snapshot");

    let config = Config {
        hosts: vec![
            HostUrl::from_str("http://147.28.178.75:8899").unwrap(),
            HostUrl::from_str("http://204.13.239.110:8899").unwrap(),
            HostUrl::from_str("http://149.50.110.119:8899").unwrap(),
            HostUrl::from_str("http://146.59.54.19:8899").unwrap(),
            HostUrl::from_str("http://74.50.77.158:80").unwrap(),
            HostUrl::from_str("http://149.50.104.41:8899").unwrap(),
            HostUrl::from_str("http://205.209.109.158:8899").unwrap(),
        ]
        .into_boxed_slice(),
        not_before_slot,
        full_snapshot_path,
        incremental_snapshot_path,
        maximum_full_snapshot_archives_to_retain: NonZeroUsize::new(10).unwrap(),
        maximum_incremental_snapshot_archives_to_retain: NonZeroUsize::new(10).unwrap(),
    };

    start_backfill_import_from_snapshot(config, db)
}
