use lite_account_manager_common::account_store_interface::AccountStorageInterface;
use lite_token_account_storage::inmemory_token_storage::InMemoryTokenStorage;
use solana_sdk::pubkey::Pubkey;

mod utils;

#[tokio::test]
pub async fn test_saving_and_loading_token_account() {
    let token_store = InMemoryTokenStorage::new();

    let mint = Pubkey::new_unique();
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
}
