use std::str::FromStr;
use std::time::Duration;

use anyhow::{anyhow, bail, Context};
use lazy_static::lazy_static;
use log::{debug, info, trace, warn};
use reqwest::redirect::Policy;
use reqwest::{Client, StatusCode};
use solana_runtime::snapshot_hash::SnapshotHash;
use solana_sdk::clock::Slot;
use solana_sdk::hash::Hash;
use tokio::task;
use regex::Regex;
use once_cell::unsync::Lazy;

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

lazy_static! {
    // 5 captures
    static ref REGEX_INCREMENTAL_SNAPSHOT_RESOURCE: Regex = Regex::from_str(r"/incremental-snapshot-([0-9]+)-([0-9]+)-([0-9a-zA-Z]+)\.tar\.(.+)").unwrap();
}

// find snapshot for exact slot
pub async fn find_full_snapshot(
    hosts: impl IntoIterator<Item = HostUrl>,
    slot: Slot,
) -> anyhow::Result<FullSnapshot> {
    let hosts_and_uris = collect_redirects(hosts, "snapshot.tar.bz2").await?;

    let mut snapshots = Vec::with_capacity(hosts_and_uris.len());

    for (host, uri) in hosts_and_uris {
        // TODOuse REGEX
        if let Some(data) = uri
            .strip_prefix("/snapshot-")
            .and_then(|s| s.strip_suffix(".tar.zst"))
        {
            trace!("checking full snapshot uri part: {}", uri);
            let parts: Vec<&str> = data.split('-').collect();

            if parts.len() == 2 {
                let full_slot = parts[0].parse::<u64>().unwrap();

                debug!("{} has full snapshot of {}", &host, full_slot);
                if full_slot != slot {
                    continue;
                }

                let hash = SnapshotHash(Hash::from_str(parts[1]).unwrap());
                snapshots.push(FullSnapshot {
                    host: host.clone(),
                    url_path: uri,
                    slot: full_slot,
                    hash,
                })
            }
        }
    }

    snapshots
        .into_iter()
        .max_by(|left, right| left.slot.cmp(&right.slot))
        .ok_or_else(|| anyhow!("Unable to find full snapshot at slot {}", slot))
}

// TODO remove
pub async fn __latest_full_snapshot(
    hosts: impl IntoIterator<Item = HostUrl>,
    not_before_slot: Slot,
) -> anyhow::Result<FullSnapshot> {
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
                // if full_slot < not_before_slot {
                //     continue;
                // }

                let hash = SnapshotHash(Hash::from_str(parts[1]).unwrap());
                snapshots.push(FullSnapshot {
                    host: host.clone(),
                    url_path: uri,
                    slot: full_slot,
                    hash,
                })
            }
        }
    }

    snapshots
        .into_iter()
        .max_by(|left, right| left.slot.cmp(&right.slot))
        .ok_or_else(|| anyhow!("Unable to find full snapshot after {}", not_before_slot))
}

pub async fn find_latest_incremental_snapshot(
    hosts: impl IntoIterator<Item = HostUrl>,
) -> anyhow::Result<IncrementalSnapshot> {
    let hosts_and_uris = collect_redirects(hosts, "incremental-snapshot.tar.bz2").await?;

    let mut snapshots = Vec::with_capacity(hosts_and_uris.len());
    for (host, uri) in hosts_and_uris {
        trace!("checking incremental snapshot uri part: {}", uri);
        // e.g. /incremental-snapshot-311178098-311179186-9shdweKVo16BKtuoguep81NKQ7GtDDFEKx5kCcRC9WR9.tar.zst

        if let Some(capture) = REGEX_INCREMENTAL_SNAPSHOT_RESOURCE.captures(&uri) {

            if capture.len() != 5 {
                warn!("Invalid incremental snapshot filename: {:?}", &capture);
                continue;
            } else {
                let full_slot = capture.get(1)
                    .expect("full slot").as_str()
                    .parse::<u64>().unwrap();
                let incremental_slot = capture.get(2)
                    .expect("incremental slot").as_str()
                    .parse::<u64>().unwrap();

                debug!("{} has incremental snapshot of {}", &host, incremental_slot);

                // base58 coded hash
                let hash = SnapshotHash(
                    Hash::from_str(
                        capture.get(3)
                            .expect("hash").as_str()
                    ).unwrap());
                snapshots.push(IncrementalSnapshot {
                    host: host.clone(),
                    url_path: uri,
                    full_slot,
                    incremental_slot,
                    hash,
                })
            }
        } else {
            warn!("Invalid incremental snapshot uri: {}", uri);
        }
    }

    let best_snapshot =
        snapshots.into_iter()
        .max_by(|left, right| left.incremental_slot.cmp(&right.incremental_slot));

    return match best_snapshot {
        Some(snapshot) => {
            debug!("Using incremental snapshot: {:?}", snapshot);
            Ok(snapshot)
        }
        None => {
            Err(anyhow!("Unable to find incremental snapshot from any host"))
        }
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
                    bail!("Unexpected status code for host <{}>: {}", host, response.status().to_string());
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

#[test]
fn test_resource_regex() {
    let str = "/incremental-snapshot-311178098-311181414-BJWKnTQDPKDHuPQPrWdQrix3VEqShnotGRLmGpN9pPuq.tar.zst";
    let capture = REGEX_INCREMENTAL_SNAPSHOT_RESOURCE.captures(str).unwrap();
    assert_eq!(capture.get(1).unwrap().as_str(), "311178098");
    assert_eq!(capture.get(2).unwrap().as_str(), "311181414");
    assert_eq!(capture.get(3).unwrap().as_str(), "BJWKnTQDPKDHuPQPrWdQrix3VEqShnotGRLmGpN9pPuq");
    assert_eq!(capture.get(4).unwrap().as_str(), "zst");
}
