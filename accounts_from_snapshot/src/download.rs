// Copyright 2024 Solana Foundation.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// This file contains code vendored from https://github.com/solana-labs/solana

use std::fs;
use std::fs::create_dir_all;
use std::num::NonZeroUsize;
use std::path::PathBuf;
use std::time::Duration;

use anyhow::bail;
use log::{debug, info};
use solana_download_utils::download_file;
use solana_runtime::snapshot_hash::SnapshotHash;
use solana_runtime::snapshot_package::SnapshotKind;
use solana_runtime::snapshot_utils;
use solana_runtime::snapshot_utils::ArchiveFormat;
use solana_sdk::clock::Slot;
use tokio::task;
use tokio::task::JoinHandle;
use tokio::time::sleep;

use crate::find::{find_full_snapshot, find_latest_incremental_snapshot};
use crate::{Config, HostUrl};

pub struct Loader {
    cfg: Config,
}

#[derive(Debug)]
pub struct FullSnapshot {
    pub path: PathBuf,
    pub slot: Slot,
}

#[derive(Debug)]
pub struct IncrementalSnapshot {
    pub path: PathBuf,
    pub full_slot: Slot,
    pub incremental_slot: Slot,
}

impl Loader {
    pub fn new(cfg: Config) -> Self {
        Self { cfg }
    }

    pub async fn load_full_snapshot_at_slot(&self, slot: Slot) -> anyhow::Result<FullSnapshot> {
        let snapshot = find_full_snapshot(self.cfg.hosts.to_vec(), slot).await?;

        self.ensure_paths_exists().await;

        let path = download_snapshot(
            snapshot.host,
            self.cfg.full_snapshot_path.clone(),
            self.cfg.incremental_snapshot_path.clone(),
            (snapshot.slot, snapshot.hash),
            SnapshotKind::FullSnapshot,
            self.cfg.maximum_full_snapshot_archives_to_retain,
            self.cfg.maximum_incremental_snapshot_archives_to_retain,
        )
        .await
        .await??;

        Ok(FullSnapshot {
            path,
            slot: snapshot.slot,
        })
    }

    pub async fn find_and_load(&self) -> anyhow::Result<()> {
        
        let hosts = self.cfg.hosts.to_vec();

        // TODO add max retries
        let (full_snapshot, incremental_snapshot) = loop {
            match find_latest_incremental_snapshot(hosts.clone()).await {
                Ok(incremental_snapshot) => {
                    debug!("Found latest incremental snapshot: {:?} - will check corresponding full snapshot", incremental_snapshot);

                    let full_snapshot = find_full_snapshot([incremental_snapshot.host.clone()], incremental_snapshot.full_slot).await.unwrap();

                    info!("Found full snapshot: {:?}", full_snapshot);
                    info!("... and incremental snapshot: {:?}", incremental_snapshot);

                    break (full_snapshot, incremental_snapshot);
                }
                Err(e) => {
                    const RETRY_DELAY: Duration = Duration::from_secs(10);
                    info!("Unable to download incremental snapshot: {} - retrying in {:?}", e.to_string(), RETRY_DELAY);
                    sleep(RETRY_DELAY).await;
                }
            }
        };

        info!("{full_snapshot:#?}");
        info!("{incremental_snapshot:#?}");


        let incremental_snapshot_path = download_snapshot(
            incremental_snapshot.host,
            self.cfg.full_snapshot_path.clone(),
            self.cfg.incremental_snapshot_path.clone(),
            (incremental_snapshot.incremental_slot
             , incremental_snapshot.hash),
            SnapshotKind::IncrementalSnapshot(incremental_snapshot.full_slot),
            self.cfg.maximum_full_snapshot_archives_to_retain,
            self.cfg.maximum_incremental_snapshot_archives_to_retain,
        )
            .await
            .await??;

        let full_snapshot_path = download_snapshot(
            full_snapshot.host,
            self.cfg.full_snapshot_path.clone(),
            self.cfg.incremental_snapshot_path.clone(),
            (full_snapshot.slot, full_snapshot.hash),
            SnapshotKind::FullSnapshot,
            self.cfg.maximum_full_snapshot_archives_to_retain,
            self.cfg.maximum_incremental_snapshot_archives_to_retain,
        )
            .await
            .await??;

        info!("incremental_snapshot_path: {:?}", incremental_snapshot_path);
        info!("full_snapshot_path: {:?}", full_snapshot_path);

        Ok(())
    }

    pub async fn load_latest_incremental_snapshot(&self) -> anyhow::Result<IncrementalSnapshot> {
        let latest_snapshot =
            find_latest_incremental_snapshot(self.cfg.hosts.to_vec()).await?;

        if self.cfg.not_before_slot > latest_snapshot.incremental_slot {
            bail!(
                "Latest incremental snapshot is at slot {}, which is older than the requested not_before_slot {}",
                latest_snapshot.incremental_slot,
                self.cfg.not_before_slot
            );
        }

        let path = download_snapshot(
            latest_snapshot.host,
            self.cfg.full_snapshot_path.clone(),
            self.cfg.incremental_snapshot_path.clone(),
            (latest_snapshot.incremental_slot, latest_snapshot.hash),
            SnapshotKind::IncrementalSnapshot(latest_snapshot.full_slot),
            self.cfg.maximum_full_snapshot_archives_to_retain,
            self.cfg.maximum_incremental_snapshot_archives_to_retain,
        )
        .await
        .await??;

        Ok(IncrementalSnapshot {
            path,
            full_slot: latest_snapshot.full_slot,
            incremental_slot: latest_snapshot.incremental_slot,
        })
    }

    async fn ensure_paths_exists(&self) {
        let full_snapshot_path = self.cfg.full_snapshot_path.clone();
        let incremental_snapshot_path = self.cfg.incremental_snapshot_path.clone();

        let _ = tokio::spawn(async move {
            create_dir_all(full_snapshot_path).expect("Unable to create snapshot path");
            create_dir_all(incremental_snapshot_path)
                .expect("Unable to create incremental snapshot path");
        })
        .await;
    }
}

pub(crate) async fn download_snapshot(
    host: HostUrl,
    full_snapshot_archives_dir: PathBuf,
    incremental_snapshot_archives_dir: PathBuf,
    desired_snapshot_hash: (Slot, SnapshotHash),
    snapshot_kind: SnapshotKind,
    maximum_full_snapshot_archives_to_retain: NonZeroUsize,
    maximum_incremental_snapshot_archives_to_retain: NonZeroUsize,
) -> JoinHandle<anyhow::Result<PathBuf>> {
    task::spawn_blocking(move || {
        let full_snapshot_archives_dir = &full_snapshot_archives_dir;
        let incremental_snapshot_archives_dir = &incremental_snapshot_archives_dir;

        snapshot_utils::purge_old_snapshot_archives(
            full_snapshot_archives_dir,
            incremental_snapshot_archives_dir,
            maximum_full_snapshot_archives_to_retain,
            maximum_incremental_snapshot_archives_to_retain,
        );

        let snapshot_archives_remote_dir =
            snapshot_utils::build_snapshot_archives_remote_dir(match snapshot_kind {
                SnapshotKind::FullSnapshot => full_snapshot_archives_dir,
                SnapshotKind::IncrementalSnapshot(_) => incremental_snapshot_archives_dir,
            });
        fs::create_dir_all(&snapshot_archives_remote_dir).unwrap();

        for archive_format in [ArchiveFormat::TarZstd] {
            let destination_path = match snapshot_kind {
                SnapshotKind::FullSnapshot => snapshot_utils::build_full_snapshot_archive_path(
                    &snapshot_archives_remote_dir,
                    desired_snapshot_hash.0,
                    &desired_snapshot_hash.1,
                    archive_format,
                ),
                SnapshotKind::IncrementalSnapshot(base_slot) => {
                    snapshot_utils::build_incremental_snapshot_archive_path(
                        &snapshot_archives_remote_dir,
                        base_slot,
                        desired_snapshot_hash.0,
                        &desired_snapshot_hash.1,
                        archive_format,
                    )
                }
            };

            if destination_path.is_file() {
                return Ok(destination_path);
            }

            match download_file(
                &format!(
                    "{}/{}",
                    host.0,
                    destination_path.file_name().unwrap().to_str().unwrap()
                ),
                &destination_path,
                false,
                &mut None,
            ) {
                Ok(()) => return Ok(destination_path),
                Err(err) => bail!("{}", err),
            }
        }

        bail!(
            "Failed to download a snapshot archive for slot {} from {}",
            desired_snapshot_hash.0,
            host.0
        )
    })
}
