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
    /// path to snapshot archive file (e.g. snapshot-312734832-8rTnxYEstpNFavGV5syBXJ1SaLphFFCHYpPXdCaEP4dC.tar.zst)
    #[arg(long)]
    pub snapshot_archive_path: String,

    /// where to build the accounts db; directory must exist
    #[arg(long)]
    pub accounts_path: String,

    /// where to overflow the accounts index; directory must exist
    #[arg(long)]
    pub account_index_path: String,
}

#[tokio::main]
pub async fn main() {
    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("info")).init();
    solana_logger::setup_with_default("info");

    let Args {
        snapshot_archive_path,
        accounts_path,
        account_index_path,
    } = Args::parse();

    let accounts_path = PathBuf::from_str(&accounts_path).unwrap();
    let account_index_path = PathBuf::from_str(&account_index_path).unwrap();

    assert!(accounts_path.is_dir(), "accounts_path must exist and must be a directory");
    assert!(account_index_path.is_dir(), "account_index_path must exist and must be a directory");


    let db = Arc::new(
        AccountsDb::new_with_account_paths(
            vec![accounts_path],
            vec![account_index_path],
        ));

    info!("Start importing accounts from full snapshot...");
    let started_at = Instant::now();
    let mut processed_accounts = 0;
    let (mut accounts_rx, _) =
        import_archive(PathBuf::from_str(&snapshot_archive_path).unwrap()).await;
    while let Some(account) = accounts_rx.recv().await {
        let slot = account.updated_slot;
        db.initialize_or_update_account(account);
        processed_accounts += 1;


        if processed_accounts % 100_000 == 0 {
            info!("Flushing at {} processed accounts", processed_accounts);
            db.force_flush(slot);
        }
    }

    info!(
        "Importing accounts from full snapshot took {:?}",
        started_at.elapsed()
    );
}
