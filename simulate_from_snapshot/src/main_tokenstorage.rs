use std::{path::PathBuf, str::FromStr, sync::Arc};

use clap::Parser;
use cli::Args;
use lite_account_manager_common::{
    account_data::{Account, AccountData, CompressionMethod, Data},
    account_store_interface::AccountStorageInterface,
    commitment::Commitment,
    slot_info::SlotInfo,
};
use lite_accounts_from_snapshot::import::import_archive;
use lite_token_account_storage::{
    inmemory_token_account_storage::InmemoryTokenAccountStorage,
    inmemory_token_storage::TokenProgramAccountsStorage,
};
use quic_geyser_common::{
    filters::Filter, message::Message, types::connections_parameters::ConnectionParameters,
};
use solana_sdk::commitment_config::CommitmentConfig;

use crate::rpc_server::RpcServerImpl;

pub mod cli;
pub mod rpc_server;

#[tokio::main(worker_threads = 2)]
async fn main() {
    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("info")).init();

    let args = Args::parse();
    println!("tester args : {:?}", args);

    let Args {
        snapshot_archive_path,
        quic_url,
    } = args;

    let token_account_storage = Arc::new(InmemoryTokenAccountStorage::default());
    let token_storage: Arc<TokenProgramAccountsStorage> =
        Arc::new(TokenProgramAccountsStorage::new(token_account_storage));

    // fill from quic geyser stream
    if let Some(quic_url) = quic_url {
        let token_storage = token_storage.clone();
        tokio::spawn(async move {
            let (quic_client, mut receiver, _jh) =
                quic_geyser_client::non_blocking::client::Client::new(
                    quic_url,
                    ConnectionParameters::default(),
                )
                .await
                .unwrap();
            quic_client
                .subscribe(vec![Filter::AccountsAll, Filter::Slot])
                .await
                .unwrap();
            while let Some(message) = receiver.recv().await {
                match message {
                    Message::AccountMsg(account) => {
                        let compression_method = match account.compression_type {
                            quic_geyser_common::compression::CompressionType::None => {
                                CompressionMethod::None
                            }
                            quic_geyser_common::compression::CompressionType::Lz4Fast(v) => {
                                CompressionMethod::Lz4(v)
                            }
                            quic_geyser_common::compression::CompressionType::Lz4(v) => {
                                CompressionMethod::Lz4(v)
                            }
                        };
                        let account_data = AccountData {
                            pubkey: account.pubkey,
                            account: Arc::new(Account {
                                lamports: account.lamports,
                                data: Data::new(&account.data, compression_method),
                                owner: account.owner,
                                executable: account.executable,
                                rent_epoch: account.rent_epoch,
                            }),
                            updated_slot: account.slot_identifier.slot,
                            write_version: account.write_version,
                        };
                        token_storage.update_account(
                            account_data,
                            lite_account_manager_common::commitment::Commitment::Processed,
                        );
                    }
                    Message::SlotMsg(slot_msg) => {
                        if slot_msg.commitment_config == CommitmentConfig::confirmed()
                            || slot_msg.commitment_config == CommitmentConfig::finalized()
                        {
                            let commitment =
                                if slot_msg.commitment_config == CommitmentConfig::confirmed() {
                                    Commitment::Confirmed
                                } else {
                                    Commitment::Finalized
                                };
                            token_storage.process_slot_data(
                                SlotInfo {
                                    slot: slot_msg.slot,
                                    parent: slot_msg.parent,
                                    root: 0,
                                },
                                commitment,
                            );
                        }
                    }
                    _ => {
                        //not supported
                    }
                }
            }

            println!("stopping geyser stream");
            log::error!("stopping geyser stream");
        });
    }

    // load accounts from snapshot
    let token_storage = token_storage.clone();
    let archive_path = PathBuf::from_str(snapshot_archive_path.as_str()).unwrap();

    log::info!("Start importing accounts from full snapshot");
    let (mut accounts_rx, _) = import_archive(archive_path).await;
    let mut cnt = 0u64;
    while let Some(AccountData {
        account, pubkey, ..
    }) = accounts_rx.recv().await
    {
        if account.owner != spl_token::ID && account.owner != spl_token_2022::ID {
            continue;
        }

        let data = account.data.clone();
        token_storage.initialize_or_update_account(AccountData {
            pubkey,
            account: Arc::new(Account {
                lamports: account.lamports,
                data,
                owner: account.owner,
                executable: account.executable,
                rent_epoch: account.rent_epoch,
            }),
            updated_slot: 0,
            write_version: 0,
        });
        cnt += 1;
        if cnt % 100_000 == 0 {
            log::info!("{} token accounts loaded", cnt);
        }
    }

    // FIXME: this also counts rejected accounts
    log::info!(
        "Storage Initialized with snapshot, {} token accounts loaded",
        cnt
    );
    let rpc_server = RpcServerImpl::new(token_storage.clone(), Some(token_storage));
    RpcServerImpl::start_serving(rpc_server, 10700)
        .await
        .unwrap();
}
