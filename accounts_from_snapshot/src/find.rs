use std::str::FromStr;
use std::time::Duration;

use anyhow::{anyhow, Context};
use log::debug;
use reqwest::redirect::Policy;
use reqwest::Client;
use solana_runtime::snapshot_hash::SnapshotHash;
use solana_sdk::clock::Slot;
use solana_sdk::hash::Hash;
use tokio::task;

use crate::HostUrl;

#[derive(Debug)]
pub struct LatestFullSnapshot {
    pub host: HostUrl,
    pub slot: Slot,
    pub hash: SnapshotHash,
}

#[derive(Debug)]
pub struct LatestIncrementalSnapshot {
    pub host: HostUrl,
    pub full_slot: Slot,
    pub incremental_slot: Slot,
    pub hash: SnapshotHash,
}

pub async fn latest_full_snapshot(
    hosts: impl IntoIterator<Item = HostUrl>,
    not_before_slot: Slot,
) -> anyhow::Result<LatestFullSnapshot> {
    let hosts_and_uris = collect_redirects(hosts, "snapshot.tar.bz2").await?;

    let mut snapshots = Vec::with_capacity(hosts_and_uris.len());

    for (host, uri) in hosts_and_uris {
        if let Some(data) = uri
            .strip_prefix("/snapshot-")
            .and_then(|s| s.strip_suffix(".tar.zst"))
        {
            let parts: Vec<&str> = data.split('-').collect();

            if parts.len() == 2 {
                let full_slot = parts[0].parse::<u64>().unwrap();

                debug!("{} has snapshot of {}", &host, full_slot);
                if full_slot < not_before_slot {
                    continue;
                }

                let hash = SnapshotHash(Hash::from_str(parts[1]).unwrap());
                snapshots.push(LatestFullSnapshot {
                    host: host.clone(),
                    slot: full_slot,
                    hash,
                })
            }
        }
    }

    snapshots
        .into_iter()
        .max_by(|left, right| left.slot.cmp(&right.slot))
        .ok_or_else(|| anyhow!("Unable to find snapshot after {}", not_before_slot))
}

pub async fn latest_incremental_snapshot(
    hosts: impl IntoIterator<Item = HostUrl>,
    not_before_incremental_slot: Slot,
) -> anyhow::Result<LatestIncrementalSnapshot> {
    let hosts_and_uris = collect_redirects(hosts, "incremental-snapshot.tar.bz2").await?;

    let mut snapshots = Vec::with_capacity(hosts_and_uris.len());
    for (host, uri) in hosts_and_uris {
        if let Some(data) = uri
            .strip_prefix("/incremental-snapshot-")
            .and_then(|s| s.strip_suffix(".tar.zst"))
        {
            let parts: Vec<&str> = data.split('-').collect();

            if parts.len() == 3 {
                let full_slot = parts[0].parse::<u64>().unwrap();
                let incremental_slot = parts[1].parse::<u64>().unwrap();

                debug!("{} has incremental snapshot of {}", &host, incremental_slot);
                if incremental_slot < not_before_incremental_slot {
                    continue;
                }

                let hash = SnapshotHash(Hash::from_str(parts[2]).unwrap());
                snapshots.push(LatestIncrementalSnapshot {
                    host: host.clone(),
                    full_slot,
                    incremental_slot,
                    hash,
                })
            }
        }
    }

    snapshots
        .into_iter()
        .max_by(|left, right| left.full_slot.cmp(&right.full_slot))
        .ok_or_else(|| {
            anyhow!(
                "Unable to find snapshot after {}",
                not_before_incremental_slot
            )
        })
}

pub(crate) async fn collect_redirects(
    hosts: impl IntoIterator<Item = HostUrl>,
    path: &str,
) -> anyhow::Result<Vec<(HostUrl, String)>> {
    let client = Client::builder()
        .timeout(Duration::from_secs(5))
        .redirect(Policy::none()) // Disable automatic redirects
        .build()
        .context("Unable to build reqwest client")?;

    let tasks: Vec<_> = hosts
        .into_iter()
        .map(|host| {
            let client = client.clone();
            let path = path.to_string();

            task::spawn(async move {
                let response = client
                    .get(format!("{}/{}", host, path))
                    .send()
                    .await
                    .context("Unable to execute request")?;
                let content = response
                    .bytes()
                    .await
                    .context("Unable to extract byes from response")?;
                let content =
                    String::from_utf8(content.to_vec()).context("Unable to read bytes as utf8")?;
                anyhow::Ok((host, content))
            })
        })
        .collect();

    let mut result = Vec::new();

    for task in tasks {
        if let Ok(Ok((host, response))) = task.await {
            result.push((host.clone(), response));
        }
    }

    anyhow::Ok(result)
}
