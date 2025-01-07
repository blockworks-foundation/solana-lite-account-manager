use std::time::Duration;
use std::{env, sync::Arc};

use geyser_grpc_connector::grpc_subscription_autoreconnect_tasks::create_geyser_autoconnection_task_with_mpsc;
use geyser_grpc_connector::{GrpcConnectionTimeouts, GrpcSourceConfig};
use log::{debug, info};
use tokio::sync::mpsc::Receiver;
use tokio::task::JoinHandle;

use lite_account_manager_common::slot_info::SlotInfoWithCommitment;
use lite_account_manager_common::{
    account_data::AccountData, account_store_interface::AccountStorageInterface,
    commitment::Commitment,
};
use lite_account_storage::accountsdb::AccountsDb;

use crate::rpc_server::RpcServerImpl;
use crate::util::{all_accounts, import_snapshots, process_stream};

mod cli;
mod rpc_server;
mod util;

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
    import_snapshots(slot.info.slot, db.clone());

    let rpc_server = RpcServerImpl::new(db.clone());

    info!("Storage Initialized with snapshot");
    let jh = RpcServerImpl::start_serving(rpc_server, 10700)
        .await
        .unwrap();
    let _ = jh.await;
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
            db.initilize_or_update_account(account);
        }
    })
}
