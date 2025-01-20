use std::sync::Arc;

use lite_account_manager_common::{
    account_data::{Account, AccountData, Data},
    account_store_interface::AccountStorageInterface,
    commitment::Commitment,
    slot_info::SlotInfo,
};
use lite_token_account_storage::{
    inmemory_token_account_storage::InmemoryTokenAccountStorage,
    inmemory_token_storage::TokenProgramAccountsStorage,
};
use solana_sdk::pubkey::Pubkey;
use utils::{
    create_mint_account_data, create_token_account_data, parse_account_data_to_mint_params,
    parse_account_data_to_token_params, MintCreationParams, TokenAccountParams,
};

mod utils;

#[test]
pub fn test_saving_and_loading_token_account() {
    tracing_subscriber::fmt::init();
    let inmemory_token_storage = Arc::new(InmemoryTokenAccountStorage::default());
    let token_store = TokenProgramAccountsStorage::new(inmemory_token_storage);

    let mint: Pubkey = Pubkey::new_unique();
    let mint_creation_params = MintCreationParams::create_default(100);
    let mint_account = create_mint_account_data(mint, mint_creation_params, 1, 1);

    let owner = Pubkey::new_unique();
    let token_account_pk = Pubkey::new_unique();
    let token_account_params = TokenAccountParams::create_default(owner, mint, 50);
    let token_account_data =
        create_token_account_data(token_account_pk, token_account_params, 2, 2);

    token_store.initialize_or_update_account(mint_account);
    token_store.initialize_or_update_account(token_account_data);

    assert_eq!(
        parse_account_data_to_token_params(
            token_store
                .get_account(token_account_pk, Commitment::Confirmed)
                .unwrap()
                .unwrap()
        ),
        token_account_params
    );
    assert_eq!(
        parse_account_data_to_token_params(
            token_store
                .get_account(token_account_pk, Commitment::Processed)
                .unwrap()
                .unwrap()
        ),
        token_account_params
    );
    assert_eq!(
        parse_account_data_to_token_params(
            token_store
                .get_account(token_account_pk, Commitment::Finalized)
                .unwrap()
                .unwrap()
        ),
        token_account_params
    );

    assert_eq!(
        parse_account_data_to_mint_params(
            token_store
                .get_account(mint, Commitment::Confirmed)
                .unwrap()
                .unwrap()
        ),
        mint_creation_params
    );
    assert_eq!(
        parse_account_data_to_mint_params(
            token_store
                .get_account(mint, Commitment::Processed)
                .unwrap()
                .unwrap()
        ),
        mint_creation_params
    );
    assert_eq!(
        parse_account_data_to_mint_params(
            token_store
                .get_account(mint, Commitment::Finalized)
                .unwrap()
                .unwrap()
        ),
        mint_creation_params
    );

    let mut rng = rand::thread_rng();
    let token_account_params_2 = TokenAccountParams::create_random(&mut rng, owner, mint, 50);
    let token_account_params_3 = TokenAccountParams::create_random(&mut rng, owner, mint, 100);
    let token_account_data_2 =
        create_token_account_data(token_account_pk, token_account_params_2, 3, 3);
    let account_data_3 = create_token_account_data(token_account_pk, token_account_params_3, 4, 4);
    token_store.update_account(token_account_data_2.clone(), Commitment::Processed);

    token_store.update_account(account_data_3.clone(), Commitment::Processed);

    token_store.process_slot_data(
        SlotInfo {
            slot: 3,
            parent: 2,
            root: 0,
        },
        Commitment::Processed,
    );

    token_store.process_slot_data(
        SlotInfo {
            slot: 4,
            parent: 3,
            root: 0,
        },
        Commitment::Processed,
    );

    token_store.process_slot_data(
        SlotInfo {
            slot: 3,
            parent: 2,
            root: 0,
        },
        Commitment::Confirmed,
    );

    assert_eq!(
        parse_account_data_to_token_params(
            token_store
                .get_account(token_account_pk, Commitment::Processed)
                .unwrap()
                .unwrap()
        ),
        token_account_params_3
    );
    assert_eq!(
        parse_account_data_to_token_params(
            token_store
                .get_account(token_account_pk, Commitment::Confirmed)
                .unwrap()
                .unwrap()
        ),
        token_account_params_2
    );
    assert_eq!(
        parse_account_data_to_token_params(
            token_store
                .get_account(token_account_pk, Commitment::Finalized)
                .unwrap()
                .unwrap()
        ),
        token_account_params
    );

    assert_eq!(
        parse_account_data_to_mint_params(
            token_store
                .get_account(mint, Commitment::Confirmed)
                .unwrap()
                .unwrap()
        ),
        mint_creation_params
    );
    assert_eq!(
        parse_account_data_to_mint_params(
            token_store
                .get_account(mint, Commitment::Processed)
                .unwrap()
                .unwrap()
        ),
        mint_creation_params
    );
    assert_eq!(
        parse_account_data_to_mint_params(
            token_store
                .get_account(mint, Commitment::Finalized)
                .unwrap()
                .unwrap()
        ),
        mint_creation_params
    );

    token_store.process_slot_data(
        SlotInfo {
            slot: 4,
            parent: 3,
            root: 0,
        },
        Commitment::Confirmed,
    );

    token_store.process_slot_data(
        SlotInfo {
            slot: 3,
            parent: 2,
            root: 0,
        },
        Commitment::Finalized,
    );

    let mint_2 = MintCreationParams::create_random(&mut rng, 2000);

    let mint_account_2 = create_mint_account_data(mint, mint_2, 5, 5);
    token_store.update_account(mint_account_2.clone(), Commitment::Processed);

    assert_eq!(
        parse_account_data_to_token_params(
            token_store
                .get_account(token_account_pk, Commitment::Processed)
                .unwrap()
                .unwrap()
        ),
        token_account_params_3
    );
    assert_eq!(
        parse_account_data_to_token_params(
            token_store
                .get_account(token_account_pk, Commitment::Confirmed)
                .unwrap()
                .unwrap()
        ),
        token_account_params_3
    );
    assert_eq!(
        parse_account_data_to_token_params(
            token_store
                .get_account(token_account_pk, Commitment::Finalized)
                .unwrap()
                .unwrap()
        ),
        token_account_params_2
    );

    assert_eq!(
        parse_account_data_to_mint_params(
            token_store
                .get_account(mint, Commitment::Confirmed)
                .unwrap()
                .unwrap()
        ),
        mint_creation_params
    );
    assert_eq!(
        parse_account_data_to_mint_params(
            token_store
                .get_account(mint, Commitment::Processed)
                .unwrap()
                .unwrap()
        ),
        mint_2
    );
    assert_eq!(
        parse_account_data_to_mint_params(
            token_store
                .get_account(mint, Commitment::Finalized)
                .unwrap()
                .unwrap()
        ),
        mint_creation_params
    );

    // deletion
    let deleted_account = AccountData {
        pubkey: token_account_pk,
        account: Arc::new(Account {
            lamports: 0,
            data: Data::Uncompressed(vec![]),
            owner: Pubkey::default(),
            executable: false,
            rent_epoch: u64::MAX,
        }),
        updated_slot: 6,
        write_version: 6,
    };

    token_store.update_account(deleted_account.clone(), Commitment::Processed);

    assert_eq!(
        token_store
            .get_account(token_account_pk, Commitment::Processed)
            .unwrap(),
        None
    );
    assert_eq!(
        parse_account_data_to_token_params(
            token_store
                .get_account(token_account_pk, Commitment::Confirmed)
                .unwrap()
                .unwrap()
        ),
        token_account_params_3
    );
    assert_eq!(
        parse_account_data_to_token_params(
            token_store
                .get_account(token_account_pk, Commitment::Finalized)
                .unwrap()
                .unwrap()
        ),
        token_account_params_2
    );

    token_store.process_slot_data(
        SlotInfo {
            slot: 6,
            parent: 5,
            root: 0,
        },
        Commitment::Confirmed,
    );

    token_store.process_slot_data(
        SlotInfo {
            slot: 5,
            parent: 4,
            root: 0,
        },
        Commitment::Finalized,
    );

    assert_eq!(
        token_store
            .get_account(token_account_pk, Commitment::Processed)
            .unwrap(),
        None
    );
    assert_eq!(
        token_store
            .get_account(token_account_pk, Commitment::Confirmed)
            .unwrap(),
        None
    );

    assert_eq!(
        parse_account_data_to_token_params(
            token_store
                .get_account(token_account_pk, Commitment::Finalized)
                .unwrap()
                .unwrap()
        ),
        token_account_params_3
    );

    token_store.process_slot_data(
        SlotInfo {
            slot: 6,
            parent: 5,
            root: 0,
        },
        Commitment::Finalized,
    );

    assert_eq!(
        token_store
            .get_account(token_account_pk, Commitment::Processed)
            .unwrap(),
        None
    );
    assert_eq!(
        token_store
            .get_account(token_account_pk, Commitment::Confirmed)
            .unwrap(),
        None
    );

    assert_eq!(
        token_store
            .get_account(token_account_pk, Commitment::Finalized)
            .unwrap(),
        None
    );
}
