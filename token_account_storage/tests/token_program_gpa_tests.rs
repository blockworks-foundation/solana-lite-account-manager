use lite_token_account_storage::inmemory_token_storage::InMemoryTokenStorage;
use solana_sdk::pubkey::Pubkey;
mod utils;

use lite_account_manager_common::{
    account_filter::{AccountFilterType, MemcmpFilter},
    account_store_interface::AccountStorageInterface,
    commitment::Commitment,
};

#[tokio::test]
pub async fn test_gpa_token_account() {
    let token_store = InMemoryTokenStorage::new();

    let mint_1: Pubkey = Pubkey::new_unique();
    let mint_creation_params = utils::MintCreationParams::create_default(100);
    let mint_account_1 = utils::create_mint_account_data(mint_1, mint_creation_params, 0, 0);

    let mint_2: Pubkey = Pubkey::new_unique();
    let mint_creation_params = utils::MintCreationParams::create_default(100);
    let mint_account_2 = utils::create_mint_account_data(mint_2, mint_creation_params, 0, 0);

    let owner_1 = Pubkey::new_unique();
    let owner_2 = Pubkey::new_unique();
    let owner_3 = Pubkey::new_unique();

    let token_acc_1_1 = Pubkey::new_unique();
    let token_acc_1_2 = Pubkey::new_unique();
    let token_acc_1_3 = Pubkey::new_unique();
    let token_acc_1_1_params = utils::TokenAccountParams::create_default(owner_1, mint_1, 100);
    let token_acc_1_2_params = utils::TokenAccountParams::create_default(owner_1, mint_2, 200);
    let token_acc_1_3_params = utils::TokenAccountParams::create_default(owner_1, mint_2, 300);
    let token_acc_1_1_account =
        utils::create_token_account_data(token_acc_1_1, token_acc_1_1_params, 0, 0);
    let token_acc_1_2_account =
        utils::create_token_account_data(token_acc_1_2, token_acc_1_2_params, 0, 0);
    let token_acc_1_3_account =
        utils::create_token_account_data(token_acc_1_3, token_acc_1_3_params, 0, 0);
    token_store
        .initilize_or_update_account(token_acc_1_1_account.clone())
        .await;
    token_store
        .initilize_or_update_account(token_acc_1_2_account.clone())
        .await;
    token_store
        .initilize_or_update_account(token_acc_1_3_account.clone())
        .await;

    let token_acc_2_1 = Pubkey::new_unique();
    let token_acc_2_2 = Pubkey::new_unique();
    let token_acc_2_1_params = utils::TokenAccountParams::create_default(owner_2, mint_1, 400);
    let token_acc_2_2_params = utils::TokenAccountParams::create_default(owner_2, mint_1, 500);
    let token_acc_2_1_account =
        utils::create_token_account_data(token_acc_2_1, token_acc_2_1_params, 0, 0);
    let token_acc_2_2_account =
        utils::create_token_account_data(token_acc_2_2, token_acc_2_2_params, 0, 0);
    token_store
        .initilize_or_update_account(token_acc_2_1_account.clone())
        .await;
    token_store
        .initilize_or_update_account(token_acc_2_2_account.clone())
        .await;

    let token_acc_3_1 = Pubkey::new_unique();
    let token_acc_3_2 = Pubkey::new_unique();
    let token_acc_3_3 = Pubkey::new_unique();
    let token_acc_3_4 = Pubkey::new_unique();
    let token_acc_3_5 = Pubkey::new_unique();
    let token_acc_3_1_params = utils::TokenAccountParams::create_default(owner_3, mint_1, 600);
    let token_acc_3_2_params = utils::TokenAccountParams::create_default(owner_3, mint_1, 700);
    let token_acc_3_3_params = utils::TokenAccountParams::create_default(owner_3, mint_2, 800);
    let token_acc_3_4_params = utils::TokenAccountParams::create_default(owner_3, mint_2, 900);
    let token_acc_3_5_params = utils::TokenAccountParams::create_default(owner_3, mint_1, 1000);
    let token_acc_3_1_account =
        utils::create_token_account_data(token_acc_3_1, token_acc_3_1_params, 0, 0);
    let token_acc_3_2_account =
        utils::create_token_account_data(token_acc_3_2, token_acc_3_2_params, 0, 0);
    let token_acc_3_3_account =
        utils::create_token_account_data(token_acc_3_3, token_acc_3_3_params, 0, 0);
    let token_acc_3_4_account =
        utils::create_token_account_data(token_acc_3_4, token_acc_3_4_params, 0, 0);
    let token_acc_3_5_account =
        utils::create_token_account_data(token_acc_3_5, token_acc_3_5_params, 0, 0);
    token_store
        .initilize_or_update_account(token_acc_3_1_account.clone())
        .await;
    token_store
        .initilize_or_update_account(token_acc_3_2_account.clone())
        .await;
    token_store
        .initilize_or_update_account(token_acc_3_3_account.clone())
        .await;
    token_store
        .initilize_or_update_account(token_acc_3_4_account.clone())
        .await;
    token_store
        .initilize_or_update_account(token_acc_3_5_account.clone())
        .await;

    token_store
        .initilize_or_update_account(mint_account_1.clone())
        .await;
    token_store
        .initilize_or_update_account(mint_account_2.clone())
        .await;

    let token_acc_owner_1 = token_store
        .get_program_accounts(
            spl_token::id(),
            Some(vec![
                AccountFilterType::Datasize(165),
                AccountFilterType::Memcmp(MemcmpFilter {
                    offset: 32,
                    data: lite_account_manager_common::account_filter::MemcmpFilterData::Bytes(
                        owner_1.to_bytes().to_vec(),
                    ),
                }),
            ]),
            Commitment::Processed,
        )
        .await
        .unwrap();
    assert_eq!(token_acc_owner_1.len(), 3);
    assert!(token_acc_owner_1.contains(&token_acc_1_1_account));
    assert!(token_acc_owner_1.contains(&token_acc_1_2_account));
    assert!(token_acc_owner_1.contains(&token_acc_1_3_account));

    let token_acc_owner_2 = token_store
        .get_program_accounts(
            spl_token::id(),
            Some(vec![
                AccountFilterType::Datasize(165),
                AccountFilterType::Memcmp(MemcmpFilter {
                    offset: 32,
                    data: lite_account_manager_common::account_filter::MemcmpFilterData::Bytes(
                        owner_2.to_bytes().to_vec(),
                    ),
                }),
            ]),
            Commitment::Processed,
        )
        .await
        .unwrap();
    assert_eq!(token_acc_owner_2.len(), 2);
    assert!(token_acc_owner_2.contains(&token_acc_2_1_account));
    assert!(token_acc_owner_2.contains(&token_acc_2_2_account));

    let token_acc_owner_3 = token_store
        .get_program_accounts(
            spl_token::id(),
            Some(vec![
                AccountFilterType::Datasize(165),
                AccountFilterType::Memcmp(MemcmpFilter {
                    offset: 32,
                    data: lite_account_manager_common::account_filter::MemcmpFilterData::Bytes(
                        owner_3.to_bytes().to_vec(),
                    ),
                }),
            ]),
            Commitment::Processed,
        )
        .await
        .unwrap();
    assert_eq!(token_acc_owner_3.len(), 5);
    assert!(token_acc_owner_3.contains(&token_acc_3_1_account));
    assert!(token_acc_owner_3.contains(&token_acc_3_2_account));
    assert!(token_acc_owner_3.contains(&token_acc_3_3_account));
    assert!(token_acc_owner_3.contains(&token_acc_3_4_account));
    assert!(token_acc_owner_3.contains(&token_acc_3_5_account));

    let token_acc_owner_3_mint_1 = token_store
        .get_program_accounts(
            spl_token::id(),
            Some(vec![
                AccountFilterType::Datasize(165),
                AccountFilterType::Memcmp(MemcmpFilter {
                    offset: 32,
                    data: lite_account_manager_common::account_filter::MemcmpFilterData::Bytes(
                        owner_3.to_bytes().to_vec(),
                    ),
                }),
                AccountFilterType::Memcmp(MemcmpFilter {
                    offset: 0,
                    data: lite_account_manager_common::account_filter::MemcmpFilterData::Bytes(
                        mint_1.to_bytes().to_vec(),
                    ),
                }),
            ]),
            Commitment::Processed,
        )
        .await
        .unwrap();

    assert_eq!(token_acc_owner_3_mint_1.len(), 3);
    assert!(token_acc_owner_3_mint_1.contains(&token_acc_3_1_account));
    assert!(token_acc_owner_3_mint_1.contains(&token_acc_3_2_account));
    assert!(token_acc_owner_3_mint_1.contains(&token_acc_3_5_account));

    let token_accounts_mint_1 = token_store
        .get_program_accounts(
            spl_token::id(),
            Some(vec![
                AccountFilterType::Datasize(165),
                AccountFilterType::Memcmp(MemcmpFilter {
                    offset: 0,
                    data: lite_account_manager_common::account_filter::MemcmpFilterData::Bytes(
                        mint_1.to_bytes().to_vec(),
                    ),
                }),
            ]),
            Commitment::Confirmed,
        )
        .await
        .unwrap();
    assert_eq!(token_accounts_mint_1.len(), 6);
    assert!(token_accounts_mint_1.contains(&token_acc_1_1_account));
    assert!(token_accounts_mint_1.contains(&token_acc_2_1_account));
    assert!(token_accounts_mint_1.contains(&token_acc_2_2_account));
    assert!(token_accounts_mint_1.contains(&token_acc_3_1_account));
    assert!(token_accounts_mint_1.contains(&token_acc_3_2_account));
    assert!(token_accounts_mint_1.contains(&token_acc_3_5_account));

    let token_accounts_mint_2 = token_store
        .get_program_accounts(
            spl_token::id(),
            Some(vec![
                AccountFilterType::Datasize(165),
                AccountFilterType::Memcmp(MemcmpFilter {
                    offset: 0,
                    data: lite_account_manager_common::account_filter::MemcmpFilterData::Bytes(
                        mint_2.to_bytes().to_vec(),
                    ),
                }),
            ]),
            Commitment::Confirmed,
        )
        .await
        .unwrap();
    assert_eq!(token_accounts_mint_2.len(), 4);
    assert!(token_accounts_mint_2.contains(&token_acc_1_2_account));
    assert!(token_accounts_mint_2.contains(&token_acc_1_3_account));
    assert!(token_accounts_mint_2.contains(&token_acc_3_3_account));
    assert!(token_accounts_mint_2.contains(&token_acc_3_4_account));

    let mint_gpa = token_store
        .get_program_accounts(
            spl_token::id(),
            Some(vec![AccountFilterType::Datasize(82)]),
            Commitment::Processed,
        )
        .await
        .unwrap();
    assert_eq!(mint_gpa.len(), 2);
    assert!(mint_gpa.contains(&mint_account_1));
    assert!(mint_gpa.contains(&mint_account_2));
}
