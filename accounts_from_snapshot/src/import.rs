#![deny(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

use anyhow::bail;
use log::{error, info, trace, warn};
use std::fs::File;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Instant;

use solana_sdk::account::ReadableAccount;
use tokio::sync::mpsc;
use tokio::sync::mpsc::Receiver;
use tokio::task::JoinHandle;

use crate::append_vec::StoredAccountMeta;
use lite_account_manager_common::account_data::{Account, AccountData, Data};

use crate::archived::ArchiveSnapshotExtractor;
use crate::core::{append_vec_iter, SnapshotExtractor};

pub async fn import_archive(archive_path: PathBuf) -> (Receiver<AccountData>, JoinHandle<()>) {
    let (tx, rx) = mpsc::channel::<AccountData>(10_000);

    let handle: JoinHandle<()> = tokio::task::spawn_blocking(move || {
        let Ok(mut extractor) = ArchiveSnapshotExtractor::open(&archive_path) else {
            error!("Unable to load archive file: {}", archive_path.display());
            return;
        };

        let started_at = Instant::now();
        let mut cnt_append_vecs: u32 = 0;

        for append_vec in extractor.iter() {
            let tx = tx.clone();

            let Ok(append_vec) = append_vec else {
                // not sure when this could happen
                error!("Unable to load append vec - aborting import task");
                return;
            };

            for handle in append_vec_iter(&append_vec) {
                if let Some((
                    StoredAccountMeta {
                        meta,
                        account_meta,
                        data,
                        ..
                    },
                    _offset,
                )) = append_vec.get_account(handle.offset)
                {
                    cnt_append_vecs += 1;
                    if cnt_append_vecs % 1_000_000 == 0 {
                        trace!(
                            "{} append vecs loaded after {:.3}s (speed {:.0}/s)",
                            cnt_append_vecs,
                            started_at.elapsed().as_secs_f64(),
                            cnt_append_vecs as f64 / started_at.elapsed().as_secs_f64()
                        );
                        trace!(
                            "items in accounts mpsc channel: {}",
                            tx.max_capacity() - tx.capacity()
                        );
                    }

                    let send_result = tx.blocking_send(AccountData {
                        pubkey: meta.pubkey,
                        account: Arc::new(Account {
                            lamports: account_meta.lamports,
                            data: Data::Uncompressed(Vec::from(data)),
                            owner: account_meta.owner,
                            executable: account_meta.executable,
                            rent_epoch: account_meta.rent_epoch,
                        }),
                        updated_slot: append_vec.slot(),
                        write_version: 0,
                    });

                    if send_result.is_err() {
                        warn!("Failed to send account to mpsc channel - aborting import task");
                        return;
                    }
                }
            }
        }
    });

    (rx, handle)
}
