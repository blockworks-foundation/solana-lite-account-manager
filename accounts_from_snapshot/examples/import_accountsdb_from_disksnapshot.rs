use std::path::PathBuf;
use std::str::FromStr;
use std::sync::Arc;
use std::time::Instant;

use clap::Parser;
use log::info;

use lite_account_manager_common::account_store_interface::AccountStorageInterface;
use lite_account_storage::accountsdb::AccountsDb;
use lite_accounts_from_snapshot::import::import_archive;

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
pub struct Args {
    #[arg(long)]
    pub snapshot_archive_path: String,
}

#[tokio::main]
pub async fn main() {
    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("info")).init();
    solana_logger::setup_with_default("info");

    let Args {
        snapshot_archive_path,
    } = Args::parse();

    let db = Arc::new(AccountsDb::new());

    info!("Start importing accounts from full snapshot");
    let started_at = Instant::now();
    let (mut accounts_rx, _) =
        import_archive(PathBuf::from_str(&snapshot_archive_path).unwrap()).await;
    while let Some(account) = accounts_rx.recv().await {
        db.initialize_or_update_account(account)
    }

    info!(
        "Importing accounts from full snapshot took {:?}",
        started_at.elapsed()
    );
}
