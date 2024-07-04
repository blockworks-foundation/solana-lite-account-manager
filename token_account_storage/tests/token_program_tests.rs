use std::sync::Arc;

use lite_account_manager_common::{
    account_data::{Account, AccountData},
    account_store_interface::AccountStorageInterface,
    commitment::Commitment,
    slot_info::SlotInfo,
};
use lite_token_account_storage::inmemory_token_storage::InMemoryTokenStorage;
use solana_sdk::pubkey::Pubkey;

mod utils;

#[tokio::test]
pub async fn test_saving_and_loading_token_account() {
    tracing_subscriber::fmt::init();
    let token_store = InMemoryTokenStorage::new();

    let mint: Pubkey = Pubkey::new_unique();
    let mint_creation_params = utils::MintCreationParams::create_default(100);
    let mint_account = utils::create_mint_account_data(mint, mint_creation_params, 1, 1);

    let owner = Pubkey::new_unique();
    let token_account_pk = Pubkey::new_unique();
    let token_account_params = utils::TokenAccountParams::create_default(owner, mint, 50);
    let token_account_data =
        utils::create_token_account_data(token_account_pk, token_account_params, 2, 2);

    token_store.initilize_or_update_account(mint_account).await;
    token_store
        .initilize_or_update_account(token_account_data)
        .await;

    assert_eq!(
        utils::parse_account_data_to_token_params(
            token_store
                .get_account(
                    token_account_pk,
                    lite_account_manager_common::commitment::Commitment::Confirmed
                )
                .await
                .unwrap()
                .unwrap()
        ),
        token_account_params
    );
    assert_eq!(
        utils::parse_account_data_to_token_params(
            token_store
                .get_account(
                    token_account_pk,
                    lite_account_manager_common::commitment::Commitment::Processed
                )
                .await
                .unwrap()
                .unwrap()
        ),
        token_account_params
    );
    assert_eq!(
        utils::parse_account_data_to_token_params(
            token_store
                .get_account(
                    token_account_pk,
                    lite_account_manager_common::commitment::Commitment::Finalized
                )
                .await
                .unwrap()
                .unwrap()
        ),
        token_account_params
    );

    assert_eq!(
        utils::parse_account_data_to_mint_params(
            token_store
                .get_account(
                    mint,
                    lite_account_manager_common::commitment::Commitment::Confirmed
                )
                .await
                .unwrap()
                .unwrap()
        ),
        mint_creation_params
    );
    assert_eq!(
        utils::parse_account_data_to_mint_params(
            token_store
                .get_account(
                    mint,
                    lite_account_manager_common::commitment::Commitment::Processed
                )
                .await
                .unwrap()
                .unwrap()
        ),
        mint_creation_params
    );
    assert_eq!(
        utils::parse_account_data_to_mint_params(
            token_store
                .get_account(
                    mint,
                    lite_account_manager_common::commitment::Commitment::Finalized
                )
                .await
                .unwrap()
                .unwrap()
        ),
        mint_creation_params
    );

    let mut rng = rand::thread_rng();
    let token_account_params_2 =
        utils::TokenAccountParams::create_random(&mut rng, owner, mint, 50);
    let token_account_params_3 =
        utils::TokenAccountParams::create_random(&mut rng, owner, mint, 100);
    let token_account_data_2 =
        utils::create_token_account_data(token_account_pk, token_account_params_2, 3, 3);
    let account_data_3 =
        utils::create_token_account_data(token_account_pk, token_account_params_3, 4, 4);
    token_store
        .update_account(
            token_account_data_2.clone(),
            lite_account_manager_common::commitment::Commitment::Processed,
        )
        .await;

    token_store
        .update_account(
            account_data_3.clone(),
            lite_account_manager_common::commitment::Commitment::Processed,
        )
        .await;

    assert_eq!(
        token_store
            .process_slot_data(
                SlotInfo {
                    slot: 3,
                    parent: 2,
                    root: 0,
                },
                Commitment::Processed
            )
            .await,
        vec![]
    );

    assert_eq!(
        token_store
            .process_slot_data(
                SlotInfo {
                    slot: 4,
                    parent: 3,
                    root: 0,
                },
                Commitment::Processed
            )
            .await,
        vec![]
    );

    assert_eq!(
        token_store
            .process_slot_data(
                SlotInfo {
                    slot: 3,
                    parent: 2,
                    root: 0,
                },
                Commitment::Confirmed
            )
            .await,
        vec![token_account_data_2.clone()]
    );

    assert_eq!(
        utils::parse_account_data_to_token_params(
            token_store
                .get_account(
                    token_account_pk,
                    lite_account_manager_common::commitment::Commitment::Processed
                )
                .await
                .unwrap()
                .unwrap()
        ),
        token_account_params_3
    );
    assert_eq!(
        utils::parse_account_data_to_token_params(
            token_store
                .get_account(
                    token_account_pk,
                    lite_account_manager_common::commitment::Commitment::Confirmed
                )
                .await
                .unwrap()
                .unwrap()
        ),
        token_account_params_2
    );
    assert_eq!(
        utils::parse_account_data_to_token_params(
            token_store
                .get_account(
                    token_account_pk,
                    lite_account_manager_common::commitment::Commitment::Finalized
                )
                .await
                .unwrap()
                .unwrap()
        ),
        token_account_params
    );

    assert_eq!(
        utils::parse_account_data_to_mint_params(
            token_store
                .get_account(
                    mint,
                    lite_account_manager_common::commitment::Commitment::Confirmed
                )
                .await
                .unwrap()
                .unwrap()
        ),
        mint_creation_params
    );
    assert_eq!(
        utils::parse_account_data_to_mint_params(
            token_store
                .get_account(
                    mint,
                    lite_account_manager_common::commitment::Commitment::Processed
                )
                .await
                .unwrap()
                .unwrap()
        ),
        mint_creation_params
    );
    assert_eq!(
        utils::parse_account_data_to_mint_params(
            token_store
                .get_account(
                    mint,
                    lite_account_manager_common::commitment::Commitment::Finalized
                )
                .await
                .unwrap()
                .unwrap()
        ),
        mint_creation_params
    );

    assert_eq!(
        token_store
            .process_slot_data(
                SlotInfo {
                    slot: 4,
                    parent: 3,
                    root: 0,
                },
                Commitment::Confirmed
            )
            .await,
        vec![account_data_3.clone()]
    );

    assert_eq!(
        token_store
            .process_slot_data(
                SlotInfo {
                    slot: 3,
                    parent: 2,
                    root: 0,
                },
                Commitment::Finalized
            )
            .await,
        vec![token_account_data_2.clone()]
    );

    let mint_2 = utils::MintCreationParams::create_random(&mut rng, 2000);

    let mint_account_2 = utils::create_mint_account_data(mint, mint_2, 5, 5);
    token_store
        .update_account(
            mint_account_2.clone(),
            lite_account_manager_common::commitment::Commitment::Processed,
        )
        .await;

    assert_eq!(
        utils::parse_account_data_to_token_params(
            token_store
                .get_account(
                    token_account_pk,
                    lite_account_manager_common::commitment::Commitment::Processed
                )
                .await
                .unwrap()
                .unwrap()
        ),
        token_account_params_3
    );
    assert_eq!(
        utils::parse_account_data_to_token_params(
            token_store
                .get_account(
                    token_account_pk,
                    lite_account_manager_common::commitment::Commitment::Confirmed
                )
                .await
                .unwrap()
                .unwrap()
        ),
        token_account_params_3
    );
    assert_eq!(
        utils::parse_account_data_to_token_params(
            token_store
                .get_account(
                    token_account_pk,
                    lite_account_manager_common::commitment::Commitment::Finalized
                )
                .await
                .unwrap()
                .unwrap()
        ),
        token_account_params_2
    );

    assert_eq!(
        utils::parse_account_data_to_mint_params(
            token_store
                .get_account(
                    mint,
                    lite_account_manager_common::commitment::Commitment::Confirmed
                )
                .await
                .unwrap()
                .unwrap()
        ),
        mint_creation_params
    );
    assert_eq!(
        utils::parse_account_data_to_mint_params(
            token_store
                .get_account(
                    mint,
                    lite_account_manager_common::commitment::Commitment::Processed
                )
                .await
                .unwrap()
                .unwrap()
        ),
        mint_2
    );
    assert_eq!(
        utils::parse_account_data_to_mint_params(
            token_store
                .get_account(
                    mint,
                    lite_account_manager_common::commitment::Commitment::Finalized
                )
                .await
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
            data: lite_account_manager_common::account_data::Data::Uncompressed(vec![]),
            owner: Pubkey::default(),
            executable: false,
            rent_epoch: u64::MAX,
            data_length: 0,
        }),
        updated_slot: 6,
        write_version: 6,
    };

    token_store
        .update_account(deleted_account.clone(), Commitment::Processed)
        .await;

    assert_eq!(
        token_store
            .get_account(
                token_account_pk,
                lite_account_manager_common::commitment::Commitment::Processed
            )
            .await
            .unwrap(),
        None
    );
    assert_eq!(
        utils::parse_account_data_to_token_params(
            token_store
                .get_account(
                    token_account_pk,
                    lite_account_manager_common::commitment::Commitment::Confirmed
                )
                .await
                .unwrap()
                .unwrap()
        ),
        token_account_params_3
    );
    assert_eq!(
        utils::parse_account_data_to_token_params(
            token_store
                .get_account(
                    token_account_pk,
                    lite_account_manager_common::commitment::Commitment::Finalized
                )
                .await
                .unwrap()
                .unwrap()
        ),
        token_account_params_2
    );

    assert_eq!(
        token_store
            .process_slot_data(
                SlotInfo {
                    slot: 6,
                    parent: 5,
                    root: 0,
                },
                Commitment::Confirmed
            )
            .await,
        vec![]
    );

    let accounts_updated = token_store
        .process_slot_data(
            SlotInfo {
                slot: 5,
                parent: 4,
                root: 0,
            },
            Commitment::Finalized,
        )
        .await;

    assert!(
        accounts_updated == vec![mint_account_2.clone(), account_data_3.clone()]
            || accounts_updated == vec![account_data_3.clone(), mint_account_2.clone()]
    );

    assert_eq!(
        token_store
            .get_account(
                token_account_pk,
                lite_account_manager_common::commitment::Commitment::Processed
            )
            .await
            .unwrap(),
        None
    );
    assert_eq!(
        token_store
            .get_account(
                token_account_pk,
                lite_account_manager_common::commitment::Commitment::Confirmed
            )
            .await
            .unwrap(),
        None
    );

    assert_eq!(
        utils::parse_account_data_to_token_params(
            token_store
                .get_account(
                    token_account_pk,
                    lite_account_manager_common::commitment::Commitment::Finalized
                )
                .await
                .unwrap()
                .unwrap()
        ),
        token_account_params_3
    );

    assert_eq!(
        token_store
            .process_slot_data(
                SlotInfo {
                    slot: 6,
                    parent: 5,
                    root: 0,
                },
                Commitment::Finalized
            )
            .await,
        vec![]
    );

    assert_eq!(
        token_store
            .get_account(
                token_account_pk,
                lite_account_manager_common::commitment::Commitment::Processed
            )
            .await
            .unwrap(),
        None
    );
    assert_eq!(
        token_store
            .get_account(
                token_account_pk,
                lite_account_manager_common::commitment::Commitment::Confirmed
            )
            .await
            .unwrap(),
        None
    );

    assert_eq!(
        token_store
            .get_account(
                token_account_pk,
                lite_account_manager_common::commitment::Commitment::Finalized
            )
            .await
            .unwrap(),
        None
    );
}
