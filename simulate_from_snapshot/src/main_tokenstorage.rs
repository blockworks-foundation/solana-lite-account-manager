use std::collections::HashMap;
use std::time::Duration;
use std::{env, path::PathBuf, str::FromStr, sync::Arc};

use clap::Parser;
use cli::Args;
use geyser_grpc_connector::grpc_subscription_autoreconnect_tasks::create_geyser_autoconnection_task_with_mpsc;
use geyser_grpc_connector::{GrpcConnectionTimeouts, GrpcSourceConfig};
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
use log::{info, warn};
use quic_geyser_common::{filters::Filter, types::connections_parameters::ConnectionParameters};
use solana_sdk::clock::Slot;
use solana_sdk::commitment_config::CommitmentConfig;
use solana_sdk::pubkey::Pubkey;
use tokio::task::JoinHandle;
use yellowstone_grpc_proto::geyser::subscribe_update::UpdateOneof;
use yellowstone_grpc_proto::geyser::{
    SubscribeRequest, SubscribeRequestFilterAccounts, SubscribeRequestFilterSlots,
    SubscribeUpdateSlot,
};

use crate::rpc_server::RpcServerImpl;

pub mod cli;
pub mod grpc_source;
pub mod rpc_server;

#[tokio::main(worker_threads = 2)]
async fn main() {
    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("info")).init();

    let args = Args::parse();
    println!("tester args : {:?}", args);

    let Args {
        snapshot_archive_path,
        quic_url,
        grpc_addr,
    } = args;

    let grpc_x_token = env::var("GRPC_X_TOKEN").ok();

    let token_account_storage = Arc::new(InmemoryTokenAccountStorage::default());
    let token_storage: Arc<TokenProgramAccountsStorage> = Arc::new(
        TokenProgramAccountsStorage::new(token_account_storage.clone()),
    );

    // load accounts from snapshot
    let token_storage = token_storage.clone();
    let archive_path = PathBuf::from_str(snapshot_archive_path.as_str()).unwrap();

    log::info!("Start importing accounts from full snapshot");
    let (mut accounts_rx, _) = import_archive(archive_path).await;
    let mut num_token_accounts = 0u64;
    let mut num_mint_accounts = 0u64;
    while let Some(AccountData {
        account, pubkey, ..
    }) = accounts_rx.recv().await
    {
        if account.owner != spl_token::ID && account.owner != spl_token_2022::ID {
            continue;
        }

        // load only mints from snapshot
        // if account.data.len() != 82 {
        //     continue;
        // }

        if account.data.len() != 82 {
            num_token_accounts += 1;
            continue;
        } else {
            num_mint_accounts += 1;
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
        if (num_token_accounts + num_mint_accounts) % 100_000 == 0 {
            log::info!(
                "{} spl token and {} mint accounts loaded",
                num_token_accounts,
                num_mint_accounts
            );
        }
    }

    log::info!(
        "Storage Initialized with snapshot, {} spl program accounts loaded",
        num_token_accounts + num_mint_accounts
    );

    if let Some(quic_url) = quic_url {
        info!("Using quic source on {}", quic_url);
        stream_accounts_from_quic_geyser_plugin(quic_url, token_storage.clone());
    }

    if let Some(grpc_addr) = grpc_addr {
        info!(
            "Using grpc source on {} (token: {})",
            grpc_addr,
            grpc_x_token.is_some()
        );
        let _jh =
            stream_accounts_from_yellowstone_grpc(grpc_addr, grpc_x_token, token_storage.clone());
    }

    let rpc_server = RpcServerImpl::new(token_storage.clone(), Some(token_storage));
    RpcServerImpl::start_serving(rpc_server, 10700)
        .await
        .unwrap();
}

fn stream_accounts_from_quic_geyser_plugin(
    quic_url: String,
    token_storage: Arc<TokenProgramAccountsStorage>,
) {
    use quic_geyser_common::message::Message;

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

fn stream_accounts_from_yellowstone_grpc(
    grpc_addr: String,
    grpc_x_token: Option<String>,
    token_storage: Arc<TokenProgramAccountsStorage>,
) -> JoinHandle<()> {
    use geyser_grpc_connector::Message;

    tokio::spawn(async move {
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

        let (autoconnect_tx, mut geyser_rx) = tokio::sync::mpsc::channel(10);
        let (_exit_tx, exit_rx) = tokio::sync::broadcast::channel::<()>(1);

        let _jh_grpc = create_geyser_autoconnection_task_with_mpsc(
            config.clone(),
            all_slots_and_accounts_together(),
            autoconnect_tx.clone(),
            exit_rx.resubscribe(),
        );

        'recv_loop: loop {
            let message = geyser_rx.recv().await;
            match message {
                Some(Message::GeyserSubscribeUpdate(update)) => match update.update_oneof {
                    Some(UpdateOneof::Account(update_account)) => {
                        let account_info = update_account.account.unwrap();
                        let slot = update_account.slot;
                        let pubkey =
                            Pubkey::new_from_array(account_info.pubkey.try_into().unwrap());
                        let lamports = account_info.lamports;
                        let data = account_info.data;
                        let owner = Pubkey::new_from_array(account_info.owner.try_into().unwrap());
                        let executable = account_info.executable;
                        let rent_epoch = account_info.rent_epoch;
                        let write_version = account_info.write_version;
                        let account_data = AccountData {
                            pubkey,
                            account: Arc::new(Account {
                                lamports,
                                data: Data::new(&data, CompressionMethod::None),
                                owner,
                                executable,
                                rent_epoch,
                            }),
                            updated_slot: slot,
                            write_version,
                        };
                        token_storage.update_account(
                            account_data,
                            lite_account_manager_common::commitment::Commitment::Processed,
                        );
                    }
                    Some(UpdateOneof::Slot(update_slot)) => {
                        let slot = update_slot.slot;
                        let parent = update_slot.parent.map(|v| v as Slot).unwrap_or_default();
                        let commitment_level = map_slot_status(&update_slot);

                        let commitment: Commitment = CommitmentConfig {
                            commitment: commitment_level,
                        }
                        .into();

                        token_storage.process_slot_data(
                            SlotInfo {
                                slot,
                                parent,
                                root: 0,
                            },
                            commitment,
                        );
                    }
                    _ => {
                        // message not interesting
                    }
                },
                Some(Message::Connecting(attempt)) => {
                    if attempt > 1 {
                        warn!("geyser grpc is reconnecting");
                    }
                }
                None => {
                    // stream ended
                    break 'recv_loop;
                }
            }
        } // -- recv loop

        log::error!("stopping geyser stream");
    })
}

/// 1. slots with all commitment levels
/// 2. spl accounts (token+mints) of spl-token and spl-token-2022 program, commitment level is processed (avoids buffering in yellowstone geyser plugin)
fn all_slots_and_accounts_together() -> SubscribeRequest {
    let slot_subs: HashMap<String, SubscribeRequestFilterSlots> = HashMap::from([(
        "sub_slots_all_commitment_levels".to_string(),
        SubscribeRequestFilterSlots {
            // implies all slots
            filter_by_commitment: None,
        },
    )]);
    let account_subs: HashMap<String, SubscribeRequestFilterAccounts> = HashMap::from([(
        "sub_spl_accounts".to_string(),
        SubscribeRequestFilterAccounts {
            account: vec![],
            owner: vec![spl_token::ID.to_string(), spl_token_2022::ID.to_string()],
            filters: vec![],
        },
    )]);

    SubscribeRequest {
        slots: slot_subs,
        accounts: account_subs,
        // implies "processed"
        commitment: None,
        ..Default::default()
    }
}

fn map_slot_status(
    slot_update: &SubscribeUpdateSlot,
) -> solana_sdk::commitment_config::CommitmentLevel {
    use solana_sdk::commitment_config::CommitmentLevel as solanaCL;
    use yellowstone_grpc_proto::geyser::CommitmentLevel as yCL;
    yellowstone_grpc_proto::geyser::CommitmentLevel::try_from(slot_update.status)
        .map(|v| match v {
            yCL::Processed => solanaCL::Processed,
            yCL::Confirmed => solanaCL::Confirmed,
            yCL::Finalized => solanaCL::Finalized,
        })
        .expect("valid commitment level")
}
