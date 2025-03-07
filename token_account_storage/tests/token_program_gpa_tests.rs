use std::sync::Arc;

use lite_token_account_storage::{
    inmemory_token_account_storage::InmemoryTokenAccountStorage,
    inmemory_token_storage::TokenProgramAccountsStorage,
};
use solana_sdk::pubkey::Pubkey;
mod utils;

use lite_account_manager_common::{
    account_filter::{AccountFilterType, MemcmpFilter, MemcmpFilterData},
    account_store_interface::AccountStorageInterface,
    commitment::Commitment,
};
use utils::{
    create_mint_account_data, create_token_account_data, MintCreationParams, TokenAccountParams,
};

#[test]
pub fn test_gpa_token_account() {
    let inmemory_token_storage = Arc::new(InmemoryTokenAccountStorage::default());
    let token_store = TokenProgramAccountsStorage::new(inmemory_token_storage);

    let mint_1: Pubkey = Pubkey::new_unique();
    let mint_creation_params = MintCreationParams::create_default(100);
    let mint_account_1 = create_mint_account_data(mint_1, mint_creation_params, 0, 0);

    let mint_2: Pubkey = Pubkey::new_unique();
    let mint_creation_params = MintCreationParams::create_default(100);
    let mint_account_2 = create_mint_account_data(mint_2, mint_creation_params, 0, 0);

    let owner_1 = Pubkey::new_unique();
    let owner_2 = Pubkey::new_unique();
    let owner_3 = Pubkey::new_unique();

    let token_acc_1_1 = Pubkey::new_unique();
    let token_acc_1_2 = Pubkey::new_unique();
    let token_acc_1_3 = Pubkey::new_unique();
    let token_acc_1_1_params = TokenAccountParams::create_default(owner_1, mint_1, 100);
    let token_acc_1_2_params = TokenAccountParams::create_default(owner_1, mint_2, 200);
    let token_acc_1_3_params = TokenAccountParams::create_default(owner_1, mint_2, 300);
    let token_acc_1_1_account =
        create_token_account_data(token_acc_1_1, token_acc_1_1_params, 0, 0);
    let token_acc_1_2_account =
        create_token_account_data(token_acc_1_2, token_acc_1_2_params, 0, 0);
    let token_acc_1_3_account =
        create_token_account_data(token_acc_1_3, token_acc_1_3_params, 0, 0);
    token_store.initialize_or_update_account(token_acc_1_1_account.clone());
    token_store.initialize_or_update_account(token_acc_1_2_account.clone());
    token_store.initialize_or_update_account(token_acc_1_3_account.clone());

    let token_acc_2_1 = Pubkey::new_unique();
    let token_acc_2_2 = Pubkey::new_unique();
    let token_acc_2_1_params = TokenAccountParams::create_default(owner_2, mint_1, 400);
    let token_acc_2_2_params = TokenAccountParams::create_default(owner_2, mint_1, 500);
    let token_acc_2_1_account =
        create_token_account_data(token_acc_2_1, token_acc_2_1_params, 0, 0);
    let token_acc_2_2_account =
        create_token_account_data(token_acc_2_2, token_acc_2_2_params, 0, 0);
    token_store.initialize_or_update_account(token_acc_2_1_account.clone());
    token_store.initialize_or_update_account(token_acc_2_2_account.clone());

    let token_acc_3_1 = Pubkey::new_unique();
    let token_acc_3_2 = Pubkey::new_unique();
    let token_acc_3_3 = Pubkey::new_unique();
    let token_acc_3_4 = Pubkey::new_unique();
    let token_acc_3_5 = Pubkey::new_unique();
    let token_acc_3_1_params = TokenAccountParams::create_default(owner_3, mint_1, 600);
    let token_acc_3_2_params = TokenAccountParams::create_default(owner_3, mint_1, 700);
    let token_acc_3_3_params = TokenAccountParams::create_default(owner_3, mint_2, 800);
    let token_acc_3_4_params = TokenAccountParams::create_default(owner_3, mint_2, 900);
    let token_acc_3_5_params = TokenAccountParams::create_default(owner_3, mint_1, 1000);
    let token_acc_3_1_account =
        create_token_account_data(token_acc_3_1, token_acc_3_1_params, 0, 0);
    let token_acc_3_2_account =
        create_token_account_data(token_acc_3_2, token_acc_3_2_params, 0, 0);
    let token_acc_3_3_account =
        create_token_account_data(token_acc_3_3, token_acc_3_3_params, 0, 0);
    let token_acc_3_4_account =
        create_token_account_data(token_acc_3_4, token_acc_3_4_params, 0, 0);
    let token_acc_3_5_account =
        create_token_account_data(token_acc_3_5, token_acc_3_5_params, 0, 0);
    token_store.initialize_or_update_account(token_acc_3_1_account.clone());
    token_store.initialize_or_update_account(token_acc_3_2_account.clone());
    token_store.initialize_or_update_account(token_acc_3_3_account.clone());
    token_store.initialize_or_update_account(token_acc_3_4_account.clone());
    token_store.initialize_or_update_account(token_acc_3_5_account.clone());

    token_store.initialize_or_update_account(mint_account_1.clone());
    token_store.initialize_or_update_account(mint_account_2.clone());

    let token_acc_owner_1 = token_store
        .get_program_accounts(
            spl_token::id(),
            Some(vec![
                AccountFilterType::DataSize(165),
                AccountFilterType::Memcmp(MemcmpFilter {
                    offset: 32,
                    data: MemcmpFilterData::Bytes(owner_1.to_bytes().to_vec()),
                }),
            ]),
            Commitment::Processed,
        )
        .unwrap();
    assert_eq!(token_acc_owner_1.len(), 3);
    assert!(token_acc_owner_1.contains(&token_acc_1_1_account));
    assert!(token_acc_owner_1.contains(&token_acc_1_2_account));
    assert!(token_acc_owner_1.contains(&token_acc_1_3_account));

    let token_acc_owner_2 = token_store
        .get_program_accounts(
            spl_token::id(),
            Some(vec![
                AccountFilterType::DataSize(165),
                AccountFilterType::Memcmp(MemcmpFilter {
                    offset: 32,
                    data: MemcmpFilterData::Bytes(owner_2.to_bytes().to_vec()),
                }),
            ]),
            Commitment::Processed,
        )
        .unwrap();
    assert_eq!(token_acc_owner_2.len(), 2);
    assert!(token_acc_owner_2.contains(&token_acc_2_1_account));
    assert!(token_acc_owner_2.contains(&token_acc_2_2_account));

    let token_acc_owner_3 = token_store
        .get_program_accounts(
            spl_token::id(),
            Some(vec![
                AccountFilterType::DataSize(165),
                AccountFilterType::Memcmp(MemcmpFilter {
                    offset: 32,
                    data: MemcmpFilterData::Bytes(owner_3.to_bytes().to_vec()),
                }),
            ]),
            Commitment::Processed,
        )
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
                AccountFilterType::DataSize(165),
                AccountFilterType::Memcmp(MemcmpFilter {
                    offset: 32,
                    data: MemcmpFilterData::Bytes(owner_3.to_bytes().to_vec()),
                }),
                AccountFilterType::Memcmp(MemcmpFilter {
                    offset: 0,
                    data: MemcmpFilterData::Bytes(mint_1.to_bytes().to_vec()),
                }),
            ]),
            Commitment::Processed,
        )
        .unwrap();

    assert_eq!(token_acc_owner_3_mint_1.len(), 3);
    assert!(token_acc_owner_3_mint_1.contains(&token_acc_3_1_account));
    assert!(token_acc_owner_3_mint_1.contains(&token_acc_3_2_account));
    assert!(token_acc_owner_3_mint_1.contains(&token_acc_3_5_account));

    let token_accounts_mint_1 = token_store
        .get_program_accounts(
            spl_token::id(),
            Some(vec![
                AccountFilterType::DataSize(165),
                AccountFilterType::Memcmp(MemcmpFilter {
                    offset: 0,
                    data: MemcmpFilterData::Bytes(mint_1.to_bytes().to_vec()),
                }),
            ]),
            Commitment::Confirmed,
        )
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
                AccountFilterType::DataSize(165),
                AccountFilterType::Memcmp(MemcmpFilter {
                    offset: 0,
                    data: MemcmpFilterData::Bytes(mint_2.to_bytes().to_vec()),
                }),
            ]),
            Commitment::Confirmed,
        )
        .unwrap();
    assert_eq!(token_accounts_mint_2.len(), 4);
    assert!(token_accounts_mint_2.contains(&token_acc_1_2_account));
    assert!(token_accounts_mint_2.contains(&token_acc_1_3_account));
    assert!(token_accounts_mint_2.contains(&token_acc_3_3_account));
    assert!(token_accounts_mint_2.contains(&token_acc_3_4_account));

    let mint_gpa = token_store
        .get_program_accounts(
            spl_token::id(),
            Some(vec![AccountFilterType::DataSize(82)]),
            Commitment::Processed,
        )
        .unwrap();
    assert_eq!(mint_gpa.len(), 2);
    assert!(mint_gpa.contains(&mint_account_1));
    assert!(mint_gpa.contains(&mint_account_2));
}
