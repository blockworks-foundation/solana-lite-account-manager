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

use std::fs::{create_dir_all, File};
use std::num::NonZeroUsize;
use std::path::{Path, PathBuf};
use std::str::FromStr;
use std::time::Duration;
use std::{env, fs};

use anyhow::bail;
use log::{debug, error, info, trace, warn};
use solana_download_utils::download_file;
use solana_runtime::snapshot_hash::SnapshotHash;
use solana_runtime::snapshot_package::SnapshotKind;
use solana_runtime::snapshot_utils;
use solana_runtime::snapshot_utils::ArchiveFormat;
use solana_sdk::clock::Slot;
use tempfile::{Builder, NamedTempFile, TempPath};
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

#[derive(Debug)]
pub struct SnapshotArchives {
    pub full_snapshot_archive_file: PathBuf,
    pub incremental_snapshot_archive_dir: PathBuf,
}

impl Loader {
    pub fn new(cfg: Config) -> Self {
        Self { cfg }
    }

    pub async fn find_and_load(&self) -> anyhow::Result<SnapshotArchives> {
        let hosts = self.cfg.hosts.to_vec();

        let mut retry_count = 0;
        const RETRY_DELAY: Duration = Duration::from_secs(10);
        const RETRY_COUNT: i32 = 5;
        let (full_snapshot, incremental_snapshot) = 'retry_loop: loop {
            if retry_count > RETRY_COUNT {
                bail!("Exceeded retry count to find and load snapshot");
            }

            match find_latest_incremental_snapshot(hosts.clone()).await {
                Ok(incremental_snapshot) => {
                    if self.cfg.not_before_slot > incremental_snapshot.incremental_slot {
                        let diff = (self.cfg.not_before_slot - incremental_snapshot.incremental_slot) as f64 * 0.4;
                        debug!(
                            "Latest incremental snapshot is at slot {}, which is older than the requested not_before_slot {} - waiting (eta in {:.0}s)",
                            incremental_snapshot.incremental_slot,
                            self.cfg.not_before_slot,
                            diff
                        );
                        retry_count += 1;
                        sleep(RETRY_DELAY).await;
                        continue 'retry_loop;
                    }

                    debug!("Found latest incremental snapshot not_before_slot {}: {:?} - will check corresponding full snapshot",
                        self.cfg.not_before_slot, incremental_snapshot);

                    let full_snapshot = match find_full_snapshot(
                        [incremental_snapshot.host.clone()],
                        incremental_snapshot.full_slot,
                    )
                        .await {
                        Ok(full_snapshot) => full_snapshot,
                        Err(err) => {
                            warn!(
                                "Unable to find full snapshot for incremental snapshot at slot {}: {} - retrying in {:?}",
                                incremental_snapshot.full_slot,
                                err,
                                RETRY_DELAY
                            );
                            retry_count += 1;
                            sleep(RETRY_DELAY).await;
                            continue 'retry_loop;
                        }
                    };

                    info!("Found full snapshot: {:?}", full_snapshot);
                    info!("... and incremental snapshot: {:?}", incremental_snapshot);

                    break 'retry_loop (full_snapshot, incremental_snapshot);
                }
                Err(e) => {
                    info!(
                        "Unable to download incremental snapshot: {} - retrying in {:?}",
                        e.to_string(),
                        RETRY_DELAY
                    );
                    retry_count += 1;
                    sleep(RETRY_DELAY).await;
                }
            }
        };

        let snapshot_dir = PathBuf::from_str("snapshots").unwrap();

        info!("{full_snapshot:#?}");
        info!("{incremental_snapshot:#?}");

        // TODO: download snapshots in parallel
        let incremental_snapshot_outfile = download_snapshot(
            incremental_snapshot.host,
            incremental_snapshot.url_path,
            snapshot_dir.clone(),
            SnapshotKind::IncrementalSnapshot(full_snapshot.slot),
            (
                incremental_snapshot.incremental_slot,
                incremental_snapshot.hash,
            ),
        )
        .await
        .await??;

        let full_snapshot_outfile = download_snapshot(
            full_snapshot.host,
            full_snapshot.url_path,
            snapshot_dir,
            SnapshotKind::FullSnapshot,
            (full_snapshot.slot, full_snapshot.hash),
        )
        .await
        .await??;

        info!("full_snapshot_path: {:?}", full_snapshot_outfile);
        info!(
            "incremental_snapshot_path: {:?}",
            incremental_snapshot_outfile
        );

        Ok(SnapshotArchives {
            full_snapshot_archive_file: full_snapshot_outfile,
            incremental_snapshot_archive_dir: incremental_snapshot_outfile,
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
    uri: String,
    snapshot_dir: PathBuf,
    snapshot_kind: SnapshotKind,
    desired_snapshot_hash: (Slot, SnapshotHash),
) -> JoinHandle<anyhow::Result<PathBuf>> {
    task::spawn_blocking(move || {
        // snapshot_utils::purge_old_snapshot_archives(
        //     full_snapshot_archives_dir,
        //     incremental_snapshot_archives_dir,
        //     maximum_full_snapshot_archives_to_retain,
        //     maximum_incremental_snapshot_archives_to_retain,
        // );

        // let snapshot_archives_remote_dir =
        //     snapshot_utils::build_snapshot_archives_remote_dir(match snapshot_kind {
        //         SnapshotKind::FullSnapshot => full_snapshot_archives_dir,
        //         SnapshotKind::IncrementalSnapshot(_) => incremental_snapshot_archives_dir,
        //     });
        // fs::create_dir_all(&snapshot_archives_remote_dir).unwrap();
        //
        for archive_format in [ArchiveFormat::TarZstd] {
            let destination_path = match snapshot_kind {
                SnapshotKind::FullSnapshot => snapshot_utils::build_full_snapshot_archive_path(
                    &snapshot_dir,
                    desired_snapshot_hash.0,
                    &desired_snapshot_hash.1,
                    archive_format,
                ),
                SnapshotKind::IncrementalSnapshot(base_slot) => {
                    snapshot_utils::build_incremental_snapshot_archive_path(
                        &snapshot_dir,
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

            let url = format!("{}{}", host.0, uri);

            trace!(
                "Downloading snapshot archive for slot {} from {} to {}",
                desired_snapshot_hash.0,
                url,
                destination_path.display()
            );

            match download_file(&url, &destination_path, false, &mut None) {
                Ok(()) => return Ok(destination_path),
                Err(err) => {
                    error!(
                        "Failed to download a snapshot archive for slot {} from {}: {}",
                        desired_snapshot_hash.0, host.0, err
                    );
                    bail!("{}", err)
                }
            }
        }

        bail!(
            "Failed to download a snapshot archive for slot {} from {}",
            desired_snapshot_hash.0,
            host.0
        )
    })
}
