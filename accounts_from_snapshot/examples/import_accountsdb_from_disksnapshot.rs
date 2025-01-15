use std::collections::HashSet;
use std::path::PathBuf;
use std::str::FromStr;
use std::sync::Arc;
use std::time::{Duration, Instant};

use clap::Parser;
use itertools::Itertools;
use lite_account_manager_common::account_data::AccountData;
use log::{debug, info, trace, warn};
use solana_accounts_db::storable_accounts::{AccountForStorage, StorableAccounts};
use solana_sdk::clock::Slot;
use solana_sdk::pubkey::Pubkey;

use lite_account_manager_common::account_store_interface::{AccountLoadingError, AccountStorageInterface};
use lite_account_manager_common::commitment::Commitment;
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

    start_accountsdb_read_task(db.clone());

    let included_program_ids = [
        "srmqPvymJeFKQ4zGQed1GFppgkRHL9kaELCbyksJtPX",
        "opnb2LAfJYbRMAHHvqjCwQxanZn7ReEHp1k81EohpZb",
        "4MangoMjqJ2firMokCjjGgoK8d4MXcrgL7XJaL3w6fVg",
        "PhoeNiXZ8ByJGLkxNfZRnkUfjvmuYqLR89jjFHGqdXY",
        "DjVE6JNiYqPL2QXyCUUh8rNjHrbz9hXHNYt99MQ59qw1",
        "9W959DqEETiGZocYWCQPaJ6sBmUzgfxXfqGeTEdp3aQP",
        "whirLbMiicVdio4qvUfM5KAg6Ct8VwpYzGff3uctyCc",
        "82yxjeMsvaURa4MbZZ7WZZHfobirZYkH1zF8fmeGtyaQ",
        "CAMMCzo5YL8w4VFF8KVHrK22GGUsp5VTaW7grrKgrWqK",
        "675kPX9MHTjS2zt1qfr1NYHuzeLXfQM9H24wFSUt1Mp8",
        "LBUZKhRxPF3XUpBCjp4YzTKgLccjZhTSDM9YuVaPwxo",
    ];
    // raydium 675kPX9MHTjS2zt1qfr1NYHuzeLXfQM9H24wFSUt1Mp8

    let whitelist: HashSet<Pubkey> = included_program_ids
        .iter()
        .map(|program_id| Pubkey::from_str(program_id).unwrap())
        .collect();

    info!(
        "Start importing accounts from full snapshot filtering {} programs ...",
        whitelist.len()
    );
    let mut some_account_ids_to_play_with = vec![];

    let started_at = Instant::now();
    let flush_debouncer = debouncer_instant::Debouncer::new(Duration::from_millis(100));
    let mut processed_accounts = 0;
    let (mut accounts_rx, _) =
        import_archive(PathBuf::from_str(&snapshot_archive_path).unwrap()).await;
    let mut batch = Vec::with_capacity(1024);
    let mut last_slot = 0;
    let mut highest_slot = 0;
    'accounts_loop: while let Some(account_data) = accounts_rx.recv().await {
        if !whitelist.contains(&account_data.account.owner) {
            continue 'accounts_loop;
        }

        if processed_accounts % 100 == 0 {
            if some_account_ids_to_play_with.len() < 20 {
                some_account_ids_to_play_with.push(account_data.pubkey);
            }
        }

        processed_accounts += 1;

        let slot = account_data.updated_slot;
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
            batch.push(account_data);

            if batch.len() >= 1024 {
                trace!("Flushing full batch of {} accounts", batch.len());
                db.initialize_or_update_accounts(slot, &batch);
                batch.clear();
            }
        } else {
            trace!("Flushing batch on slot change of {} accounts", batch.len());
            db.initialize_or_update_accounts(slot, &batch);
            batch.clear();
            batch.push(account_data);
        }

        if processed_accounts % 100_000 == 0 {
            info!("Processed {} accounts so far", processed_accounts);

            for pk in &some_account_ids_to_play_with {
                let result = db.get_account(pk.clone(), Commitment::Processed); // TODO check commitment level
                match result {
                    Ok(Some(found)) => {
                        info!("-> found account {:?} with size {}", found.pubkey, found.account.data.len());
                    }
                    Err(err) => {
                        warn!("-> error loading account {:?}: {:?}", pk, err);
                    }
                    Ok(None) => {
                        warn!("-> eccount {:?} not found", pk);
                    }
                }

            }

        }

        if flush_debouncer.can_fire() {
            // interval is defined in accounts_background_service: INTERVAL_MS=100
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

fn start_accountsdb_read_task(db: Arc<AccountsDb>) {
    tokio::spawn(async move {

        let db = db.clone();

        let mut tick = tokio::time::interval(Duration::from_secs(1));
        loop {

            let pk = Pubkey::from_str("88hUW6qs3QGv6cfoejkq7zgeWAE5BhYPJ2CjWnXzoBtN").unwrap();

            let result = db.get_account(pk.clone(), Commitment::Finalized);
            match result {
                Ok(Some(found)) => {
                    info!("-> found account {:?} with size {}", found.pubkey, found.account.data.len());
                }
                Err(err) => {
                    warn!("-> error loading account {:?}: {:?}", pk, err);
                }
                Ok(None) => {
                    warn!("-> eccount {:?} not found", pk);
                }
            }


            tick.tick().await;
        }

    });

}
