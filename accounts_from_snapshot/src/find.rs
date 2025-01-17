use std::time::Duration;

use anyhow::{anyhow, bail, Context};
use log::{debug, trace, warn};
use reqwest::redirect::Policy;
use reqwest::{Client, StatusCode};
use solana_runtime::snapshot_hash::SnapshotHash;
use solana_sdk::clock::Slot;
use tokio::task;

use crate::solana::{
    parse_full_snapshot_archive_filename, parse_incremental_snapshot_archive_filename,
};
use crate::HostUrl;

#[derive(Debug)]
pub struct FullSnapshot {
    pub host: HostUrl,
    // e.g. /snapshot-311178098-9shdweKVo16BKtuoguep81NKQ7GtDDFEKx5kCcRC9WR9.tar.zst
    pub url_path: String,
    pub slot: Slot,
    pub hash: SnapshotHash,
}

#[derive(Debug)]
pub struct IncrementalSnapshot {
    pub host: HostUrl,
    // e.g. /incremental-snapshot-311178098-311179186-9shdweKVo16BKtuoguep81NKQ7GtDDFEKx5kCcRC9WR9.tar.zst
    pub url_path: String,
    // full snapshot slot from which the increment bases off
    pub full_slot: Slot,
    pub incremental_slot: Slot,
    pub hash: SnapshotHash,
}

// find snapshot for exact slot
pub async fn find_full_snapshot(
    hosts: impl IntoIterator<Item = HostUrl>,
    slot: Slot,
) -> anyhow::Result<FullSnapshot> {
    let hosts_and_uris = collect_redirects(hosts, "snapshot.tar.bz2").await?;

    let mut snapshots = Vec::with_capacity(hosts_and_uris.len());

    for (host, uri) in hosts_and_uris {
        if let Ok((full_slot, hash, _archive_format)) =
            parse_full_snapshot_archive_filename(trim_trailing_slash(&uri))
        {
            trace!("checking full snapshot uri part: {}", uri);

            debug!("{} has full snapshot of {}", &host, full_slot);
            if full_slot != slot {
                continue;
            }

            snapshots.push(FullSnapshot {
                host: host.clone(),
                url_path: uri,
                slot: full_slot,
                hash,
            })
        }
    }

    snapshots
        .into_iter()
        .max_by(|left, right| left.slot.cmp(&right.slot))
        .ok_or_else(|| anyhow!("Unable to find full snapshot at slot {}", slot))
}

pub async fn find_latest_incremental_snapshot(
    hosts: impl IntoIterator<Item = HostUrl>,
) -> anyhow::Result<IncrementalSnapshot> {
    let hosts_and_uris = collect_redirects(hosts, "incremental-snapshot.tar.bz2").await?;

    let mut snapshots = Vec::with_capacity(hosts_and_uris.len());
    for (host, uri) in hosts_and_uris {
        trace!("checking incremental snapshot uri part: {}", uri);

        if let Ok((full_slot, incremental_slot, hash, _archive_format)) =
            parse_incremental_snapshot_archive_filename(trim_trailing_slash(&uri))
        {
            debug!(
                "{} has incremental snapshot of {} based on {}",
                &host, incremental_slot, full_slot
            );

            snapshots.push(IncrementalSnapshot {
                host: host.clone(),
                url_path: uri,
                full_slot,
                incremental_slot,
                hash,
            })
        } else {
            warn!("Invalid incremental snapshot uri: {}", uri);
        }
    }

    let best_snapshot = snapshots
        .into_iter()
        .max_by(|left, right| left.incremental_slot.cmp(&right.incremental_slot));

    match best_snapshot {
        Some(snapshot) => {
            debug!("Using incremental snapshot: {:?}", snapshot);
            Ok(snapshot)
        }
        None => Err(anyhow!("Unable to find incremental snapshot from any host")),
    }
}

pub(crate) async fn collect_redirects(
    hosts: impl IntoIterator<Item = HostUrl>,
    path: &str,
) -> anyhow::Result<Vec<(HostUrl, String)>> {
    let client = Client::builder()
        .connect_timeout(Duration::from_secs(15))
        .timeout(Duration::from_secs(15))
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
                if response.status() != StatusCode::SEE_OTHER {
                    // e.g. 429 Too Many Requests
                    bail!(
                        "Unexpected status code for host <{}>: {}",
                        host,
                        response.status().to_string()
                    );
                }
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
        match task.await {
            Ok(Ok((host, response))) => {
                // response contains leading slash: "/incremental-snapshot-307019864-307043916-9r7T1mzsE8Zwha8kp3eyTwCNF63ZPW6SVgqjQMecK8zj.tar.zst"
                result.push((host.clone(), response));
            }
            Ok(Err(e)) => {
                warn!("Error fetching from rpc: {:?}", e);
            }
            Err(join_error) => {
                panic!("Error joining task: {:?}", join_error);
            }
        }
    }

    anyhow::Ok(result)
}

fn trim_trailing_slash(uri: &str) -> &str {
    assert!(uri.starts_with('/'));
    &uri[1..]
}

#[test]
fn test_trim_trailing_slash() {
    assert_eq!(trim_trailing_slash("/abc"), "abc");
    assert_eq!(trim_trailing_slash("/"), "");
}
