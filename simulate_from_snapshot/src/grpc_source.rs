use geyser_grpc_connector::Message;
use lite_account_manager_common::account_data::{Account, AccountData, Data};
use lite_account_manager_common::commitment::Commitment;
use lite_account_manager_common::slot_info::{SlotInfo, SlotInfoWithCommitment};
use solana_sdk::pubkey::Pubkey;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::mpsc::Receiver;
use yellowstone_grpc_proto::geyser::subscribe_update::UpdateOneof;
use yellowstone_grpc_proto::geyser::{
    SubscribeRequest, SubscribeRequestFilterAccounts, SubscribeRequestFilterSlots,
};

pub(crate) fn process_stream(
    mut geyser_messages_rx: Receiver<Message>,
) -> (Receiver<SlotInfoWithCommitment>, Receiver<AccountData>) {
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
                    Some(UpdateOneof::Slot(slot)) => slots_tx
                        .send(SlotInfoWithCommitment {
                            info: SlotInfo {
                                slot: slot.slot,
                                parent: slot.parent.unwrap_or(0),
                                root: 0,
                            },
                            commitment: Commitment::from(slot.status),
                        })
                        .await
                        .expect("Failed to send slot info"),
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

    (slots_rx, accounts_rx)
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
        SubscribeRequestFilterSlots {
            filter_by_commitment: None,
        },
    );

    SubscribeRequest {
        accounts: accounts_subs,
        slots: slots_subs,
        ..Default::default()
    }
}
