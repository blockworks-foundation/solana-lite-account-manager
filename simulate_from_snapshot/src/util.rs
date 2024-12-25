use std::collections::HashMap;
use std::num::NonZeroUsize;
use std::path::PathBuf;
use std::str::FromStr;
use std::sync::Arc;

use geyser_grpc_connector::Message;
use solana_sdk::clock::Slot;
use solana_sdk::hash::Hash;
use solana_sdk::pubkey::Pubkey;
use tokio::sync::mpsc::Receiver;
use tokio::task::JoinHandle;
use yellowstone_grpc_proto::geyser::{SubscribeRequest, SubscribeRequestFilterAccounts, SubscribeRequestFilterSlots, SubscribeUpdateSlot};
use yellowstone_grpc_proto::geyser::subscribe_update::UpdateOneof;

use lite_account_manager_common::account_data::{Account, AccountData, Data};
use lite_account_manager_common::commitment::Commitment;
use lite_account_manager_common::slot_info::{SlotInfo, SlotInfoWithCommitment};
use lite_account_storage::accountsdb::AccountsDb;
use lite_accounts_from_snapshot::{Config, HostUrl, import};

pub(crate) fn import_snapshots(slot: Slot, db: Arc<AccountsDb>) -> JoinHandle<()> {
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
        not_before_slot: slot,
        full_snapshot_path: PathBuf::from_str("/tmp/full-snapshot").unwrap(),
        incremental_snapshot_path: PathBuf::from_str("/tmp/incremental-snapshot").unwrap(),
        maximum_full_snapshot_archives_to_retain: NonZeroUsize::new(10).unwrap(),
        maximum_incremental_snapshot_archives_to_retain: NonZeroUsize::new(10).unwrap(),
    };

    import(config, db)
}

pub(crate) fn process_stream(mut geyser_messages_rx: Receiver<Message>) -> (Receiver<SlotInfoWithCommitment>, Receiver<AccountData>) {
    let (accounts_tx, accounts_rx) = tokio::sync::mpsc::channel::<AccountData>(1000);
    let (slots_tx, slots_rx) = tokio::sync::mpsc::channel::<SlotInfoWithCommitment>(10);

    tokio::spawn(async move {
        loop {
            match geyser_messages_rx.recv().await {
                Some(Message::GeyserSubscribeUpdate(update)) => match update.update_oneof {
                    Some(UpdateOneof::Account(update)) => {
                        let info = update.account.unwrap();
                        let slot = update.slot;

                        let account_pk = Pubkey::try_from(info.pubkey).unwrap();
                        let account_owner_pk = Pubkey::try_from(info.owner).unwrap();

                        accounts_tx
                            .send(AccountData {
                                pubkey: account_pk,
                                account: Arc::new(Account {
                                    lamports: info.lamports,
                                    data: Data::Uncompressed(info.data),
                                    owner: account_owner_pk,
                                    executable: info.executable,
                                    rent_epoch: info.rent_epoch,
                                }),
                                updated_slot: slot,
                                write_version: info.write_version,
                            })
                            .await
                            .expect("Failed to send account");
                    }
                    Some(UpdateOneof::Slot(slot)) => {
                        slots_tx.send(SlotInfoWithCommitment {
                            info: SlotInfo {
                                slot: slot.slot,
                                parent: slot.parent.unwrap_or(0),
                                root: 0,
                            },
                            commitment: Commitment::from(slot.status),
                        }).await
                            .expect("Failed to send slot info")
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

    return (slots_rx, accounts_rx);
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

    let mut slots_subs = HashMap::new();
    slots_subs.insert(
        "client".to_string(),
        SubscribeRequestFilterSlots { filter_by_commitment: None },
    );

    SubscribeRequest {
        accounts: accounts_subs,
        slots: slots_subs,
        ..Default::default()
    }
}
