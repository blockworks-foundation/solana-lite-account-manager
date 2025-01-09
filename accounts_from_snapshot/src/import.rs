use std::fs::File;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Instant;
use log::info;

use solana_sdk::account::ReadableAccount;
use tokio::sync::mpsc;
use tokio::sync::mpsc::Receiver;
use tokio::task::JoinHandle;

use lite_account_manager_common::account_data::{Account, AccountData, Data};

use crate::archived::ArchiveSnapshotExtractor;
use crate::core::{append_vec_iter, SnapshotExtractor};

pub async fn import_archive(archive_path: PathBuf) -> (Receiver<AccountData>, JoinHandle<()>) {
    let (tx, rx) = mpsc::channel::<AccountData>(10_000);

    let handle: JoinHandle<()> = tokio::task::spawn_blocking(move || {
        let mut extractor: ArchiveSnapshotExtractor<File> =
            ArchiveSnapshotExtractor::open(&archive_path).expect(
                format!(
                    "Unable to load archive file: {}",
                    archive_path.to_str().unwrap()
                )
                .as_str(),
            );

        let started_at = Instant::now();
        let mut cnt_append_vecs: u32 = 0;

        for append_vec in extractor.iter() {
            let tx = tx.clone();

            let append_vec = append_vec.unwrap();

            for handle in append_vec_iter(&append_vec) {
                if let Some((account_meta, _offset)) = append_vec.get_account(handle.offset) {
                    cnt_append_vecs += 1;
                    if cnt_append_vecs % 100_000 == 0 {
                        info!("{} append vecs loaded after {:.3}s (speed {:.0}/s)",
                        cnt_append_vecs, started_at.elapsed().as_secs_f64(), cnt_append_vecs as f64 / started_at.elapsed().as_secs_f64());

                        info!("items in channel: {}", tx.max_capacity() - tx.capacity());
                    }

                    tx.blocking_send(AccountData {
                        pubkey: account_meta.meta.pubkey,
                        account: Arc::new(Account {
                            lamports: account_meta.account_meta.lamports,
                            data: Data::Uncompressed(Vec::from(account_meta.data)),
                            owner: account_meta.account_meta.owner.clone(),
                            executable: account_meta.account_meta.executable,
                            rent_epoch: account_meta.account_meta.rent_epoch,
                        }),
                        updated_slot: append_vec.slot(),
                        write_version: 0,
                    })
                    .expect("Failed to send account data");
                }
            }
        }
    });

    (rx, handle)
}
