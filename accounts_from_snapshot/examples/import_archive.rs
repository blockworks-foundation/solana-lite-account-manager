use clap::Parser;
use lite_accounts_from_snapshot::import::import_archive;
use log::{debug, info, trace};
use std::path::PathBuf;
use std::str::FromStr;
use std::time::Instant;

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
pub struct Args {
    #[arg(long)]
    pub snapshot_archive_path: String,
}

#[tokio::main]
pub async fn main() {
    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("info")).init();

    let Args {
        snapshot_archive_path,
    } = Args::parse();

    let (mut accounts_rx, _) =
        import_archive(PathBuf::from_str(&snapshot_archive_path).unwrap()).await;

    let mut avg_batchsize_cumulator = 0;
    let mut loop_cnt = 0;
    let mut started_at = Instant::now();
    let mut cnt_append_vecs: u32 = 0;
    let mut batch = Vec::with_capacity(64);
    while let Some(account1) = accounts_rx.recv().await {
        loop_cnt += 1;
        if cnt_append_vecs == 0 {
            started_at = Instant::now();
        }

        batch.clear();
        batch.push(account1);
        'inner_drain_loop: while let Ok(account_more) = accounts_rx.try_recv() {
            batch.push(account_more);
            if batch.len() >= batch.capacity() {
                break 'inner_drain_loop;
            }
        }
        trace!("batch size: {}", batch.len());
        avg_batchsize_cumulator += batch.len();

        for item in batch.drain(..) {
            cnt_append_vecs += 1;
            if cnt_append_vecs % 1_000_000 == 0 {
                info!(
                    "{} append vecs loaded after {:.3}s (speed {:.0}/s)",
                    cnt_append_vecs,
                    started_at.elapsed().as_secs_f64(),
                    cnt_append_vecs as f64 / started_at.elapsed().as_secs_f64()
                );
            }
            debug!("account: {:?}", item);
        }
    }

    // 35
    info!(
        "Average batch size: {}",
        avg_batchsize_cumulator as f64 / loop_cnt as f64
    );
}
