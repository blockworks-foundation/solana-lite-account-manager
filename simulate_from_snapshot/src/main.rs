use std::{fs::File, path::PathBuf, str::FromStr, sync::Arc};

use clap::Parser;
use log::info;
use cli::Args;
use lite_account_manager_common::{
    account_data::{Account, AccountData, CompressionMethod, Data},
    account_store_interface::AccountStorageInterface,
    commitment::Commitment,
    slot_info::SlotInfo,
};
use lite_token_account_storage::{
    inmemory_token_account_storage::InmemoryTokenAccountStorage,
    inmemory_token_storage::TokenProgramAccountsStorage,
};
use quic_geyser_common::{
    filters::Filter, message::Message, types::connections_parameters::ConnectionParameters,
};
use quic_geyser_common::filters::AccountFilter;
use quic_geyser_common::filters::Filter::AccountsAll;
use snapshot_utils::{append_vec_iter, archived::ArchiveSnapshotExtractor, SnapshotExtractor};
use solana_sdk::{commitment_config::CommitmentConfig, pubkey::Pubkey};
use lite_account_manager_common::except_filter_store::ExceptFilterStore;
use lite_account_manager_common::simple_filter_store::SimpleFilterStore;
use lite_account_storage::inmemory_account_store::InmemoryAccountStore;

use crate::rpc_server::RpcServerImpl;

pub mod cli;
pub mod rpc_server;
pub mod snapshot_utils;

#[tokio::main(worker_threads = 2)]
async fn main() {
    env_logger::init_from_env(
        env_logger::Env::default().filter_or(env_logger::DEFAULT_FILTER_ENV, "info"),
    );

    let args = Args::parse();
    println!("tester args : {:?}", args);

    let Args {
        snapshot_archive_path,
        quic_url,
    } = args;

    let token_account_storage = Arc::new(InmemoryTokenAccountStorage::default());
    let mut filter_store = Arc::new(ExceptFilterStore::default());
    let account_storage: Arc<dyn AccountStorageInterface> =
        Arc::new(InmemoryAccountStore::new(filter_store));

    // fill from quic geyser stream
    if let Some(quic_url) = quic_url {
        let token_storage = account_storage.clone();
        tokio::spawn(async move {
            let (quic_client, mut reciever, _jh) =
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
            while let Some(message) = reciever.recv().await {
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
    // let bk = {
    //     let token_storage = token_storage.clone();
    //     let token_program =
    //         Pubkey::from_str("TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA").unwrap();
    //     tokio::task::spawn_blocking(move || {
    //         let archive_path = PathBuf::from_str(snapshot_archive_path.as_str()).unwrap();
    //
    //         let mut loader: ArchiveSnapshotExtractor<File> =
    //             ArchiveSnapshotExtractor::open(&archive_path).unwrap();
    //         for vec in loader.iter() {
    //             let append_vec = vec.unwrap();
    //             // info!("size: {:?}", append_vec.len());
    //             for handle in append_vec_iter(&append_vec) {
    //                 let stored = handle.access().unwrap();
    //                 if stored.account_meta.owner != token_program {
    //                     continue;
    //                 }
    //
    //                 let data = stored.data;
    //                 let compressed_data = Data::new(data, CompressionMethod::None);
    //                 token_storage.initilize_or_update_account(AccountData {
    //                     pubkey: stored.meta.pubkey,
    //                     account: Arc::new(Account {
    //                         lamports: stored.account_meta.lamports,
    //                         data: compressed_data,
    //                         owner: stored.account_meta.owner,
    //                         executable: stored.account_meta.executable,
    //                         rent_epoch: stored.account_meta.rent_epoch,
    //                     }),
    //                     updated_slot: 0,
    //                     write_version: 0,
    //                 });
    //             }
    //         }
    //     })
    // };
    // // await for loading of snapshot to finish
    // bk.await.unwrap();


    let bk = {
        let storage = account_storage.clone();
        // let token_program =
        //     Pubkey::from_str("TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA").unwrap();
        tokio::task::spawn_blocking(move || {
            let archive_path = PathBuf::from_str(snapshot_archive_path.as_str()).unwrap();

            let mut limit_counter = 0;
            let mut loader: ArchiveSnapshotExtractor<File> =
                ArchiveSnapshotExtractor::open(&archive_path).unwrap();
            'snapshot_loop: for vec in loader.iter() {
                let append_vec = vec.unwrap();
                // info!("size: {:?}", append_vec.len());
                for handle in append_vec_iter(&append_vec) {
                    let stored = handle.access().unwrap();
                    // if stored.account_meta.owner != token_program {
                    //     continue;
                    // }

                    // info!("loading account: {:?}", stored.meta.pubkey);
                    let data = stored.data;
                    let compressed_data = Data::new(data, CompressionMethod::None);
                    storage.initilize_or_update_account(AccountData {
                        pubkey: stored.meta.pubkey,
                        account: Arc::new(Account {
                            lamports: stored.account_meta.lamports,
                            data: compressed_data,
                            owner: stored.account_meta.owner,
                            executable: stored.account_meta.executable,
                            rent_epoch: stored.account_meta.rent_epoch,
                        }),
                        updated_slot: 0,
                        write_version: 0,
                    });
                    limit_counter += 1;
                    // 7 secs
                    if limit_counter > 1_000_000 {
                        info!("snapshot loading limit reached");
                        break 'snapshot_loop;
                    }
                }
            }
        })
    };
    // await for loading of snapshot to finish
    bk.await.unwrap();


    log::info!("Storage Initialized with snapshot");
    let rpc_server = RpcServerImpl::new(account_storage);
    let jh = RpcServerImpl::start_serving(rpc_server, 10700)
        .await
        .unwrap();
    let _ = jh.await;
}
