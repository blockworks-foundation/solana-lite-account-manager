use std::path::PathBuf;
use std::str::FromStr;
use std::sync::Arc;
use std::time::{Duration, Instant};

use clap::Parser;
use lite_account_manager_common::account_data::AccountData;
use log::{debug, info, trace, warn};
use solana_accounts_db::storable_accounts::{AccountForStorage, StorableAccounts};
use solana_sdk::clock::Slot;

use lite_account_manager_common::account_store_interface::AccountStorageInterface;
use lite_account_storage::accountsdb::AccountsDb;
use lite_accounts_from_snapshot::debouncer_instant;
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

    assert!(
        accounts_path.is_dir(),
        "accounts_path must exist and must be a directory"
    );
    assert!(
        account_index_path.is_dir(),
        "account_index_path must exist and must be a directory"
    );

    let db = Arc::new(AccountsDb::new_with_account_paths(
        vec![accounts_path],
        vec![account_index_path],
    ));

    let raydium_program_id =
        solana_sdk::pubkey::Pubkey::from_str("675kPX9MHTjS2zt1qfr1NYHuzeLXfQM9H24wFSUt1Mp8")
            .unwrap();

    info!(
        "Start importing accounts from full snapshot filtering program {}...",
        raydium_program_id
    );
    let started_at = Instant::now();
    let flush_debouncer = debouncer_instant::Debouncer::new(Duration::from_millis(100));
    let mut processed_accounts = 0;
    let (mut accounts_rx, _) =
        import_archive(PathBuf::from_str(&snapshot_archive_path).unwrap()).await;
    let mut batch = Vec::with_capacity(1024);
    let mut last_slot = 0;
    let mut highest_slot = 0;
    'accounts_loop: while let Some(account) = accounts_rx.recv().await {
        // if account.account.owner != raydium_program_id {
        //     continue 'accounts_loop;
        // }

        processed_accounts += 1;

        let slot = account.updated_slot;
        let slot_changed = {
            let changed = slot != last_slot;
            last_slot = slot;
            changed
        };

        let highest_slot_changed = {
            if slot > highest_slot {
                highest_slot = slot;
                true
            } else {
                false
            }
        };

        if highest_slot_changed {
            info!("Freezing slot {}", slot);
            db.freeze_slot(slot);
        }

        if !slot_changed {
            batch.push(account);

            if batch.len() >= 1024 {
                trace!("Flushing full batch of {} accounts", batch.len());
                db.initialize_or_update_accounts(slot, &batch);
                batch.clear();
            }
        } else {
            trace!("Flushing batch on slot change of {} accounts", batch.len());
            db.initialize_or_update_accounts(slot, &batch);
            batch.clear();
            batch.push(account);
        }

        if processed_accounts % 100_000 == 0 {
            info!("Processed {} accounts so far", processed_accounts);
        }

        if flush_debouncer.can_fire() {
            info!("Flushing accounts cache");
            // this is done by background job in validators
            // db.flush_accounts_cache_if_needed(slot);
            db.force_flush(slot)
        }
    }

    // TODO we lose the unflushed data
    warn!("NOT Flushing last batch of {} accounts", batch.len());

    info!(
        "Importing accounts from full snapshot took {:?}",
        started_at.elapsed()
    );
}
