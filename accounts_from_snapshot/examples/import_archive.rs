use std::path::PathBuf;
use std::str::FromStr;
use std::time::Instant;
use log::info;
use lite_accounts_from_snapshot::import::import_archive;

#[tokio::main]
pub async fn main() {
    tracing_subscriber::fmt::init();

    let snapshot_file = "/Users/stefan/mango/projects/accountsdb-how-it-works/snapshot-278240833-2WmWBr6Fnq9w6VVtLbhezGZHfH6L7YrPgggRqFg2ouTk.tar.zst";



    let (mut accounts_rx, _) = import_archive(PathBuf::from_str(snapshot_file).unwrap()).await;

    let started_at = Instant::now();
    let mut cnt_append_vecs: u32 = 0;
    while let Some(account) = accounts_rx.recv().await {
        cnt_append_vecs += 1;
        if cnt_append_vecs % 100_000 == 0 {
            info!("{} append vecs loaded after {:.3}s (speed {:.0}/s)",
                            cnt_append_vecs, started_at.elapsed().as_secs_f64(), cnt_append_vecs as f64 / started_at.elapsed().as_secs_f64());
        }
        // println!("account: {:?}", account);
    }
}
