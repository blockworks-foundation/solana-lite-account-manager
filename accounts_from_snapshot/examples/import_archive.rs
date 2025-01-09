use std::path::PathBuf;
use std::str::FromStr;
use std::time::Instant;
use log::{info, trace};
use lite_accounts_from_snapshot::import::import_archive;

#[tokio::main]
pub async fn main() {
    tracing_subscriber::fmt::init();

    let snapshot_file = "/Users/stefan/mango/projects/accountsdb-how-it-works/snapshot-278240833-2WmWBr6Fnq9w6VVtLbhezGZHfH6L7YrPgggRqFg2ouTk.tar.zst";

    let (mut accounts_rx, _) = import_archive(PathBuf::from_str(snapshot_file).unwrap()).await;

    let mut started_at = Instant::now();
    let mut cnt_append_vecs: u32 = 0;
    let mut batch = Vec::with_capacity(64);
    while let Some(account) = accounts_rx.recv().await {
        if cnt_append_vecs == 0 {
            started_at = Instant::now();
        }

        batch.clear();
        batch.push(account);
        'inner_drain_loop: while let Ok(item) = accounts_rx.try_recv() {
            batch.push(item);
            if batch.len() >= batch.capacity() {
                break 'inner_drain_loop;
            }
        }
        trace!("batch size: {}", batch.len());

        for item in batch.drain(..) {
            cnt_append_vecs += 1;
            if cnt_append_vecs % 100_000 == 0 {
                info!("{} append vecs loaded after {:.3}s (speed {:.0}/s)",
                                cnt_append_vecs, started_at.elapsed().as_secs_f64(), cnt_append_vecs as f64 / started_at.elapsed().as_secs_f64());
            }
            // println!("account: {:?}", account);
        }

    }
}
