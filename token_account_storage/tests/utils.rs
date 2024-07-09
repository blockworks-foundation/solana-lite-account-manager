use lite_account_manager_common::account_data::{Account, AccountData};
use lite_token_account_storage::account_types::TokenProgramAccountState;
use rand::{rngs::ThreadRng, Rng};
use solana_sdk::{clock::Slot, program_option::COption, program_pack::Pack, pubkey::Pubkey};
use std::sync::Arc;

#[derive(PartialEq, Eq, Clone, Copy, Debug)]
pub struct TokenAccountParams {
    owner: Pubkey,
    mint: Pubkey,
    amount: u64,
    delegate: Option<(Pubkey, u64)>,
    is_native: Option<u64>,
    close_authority: Option<Pubkey>,
    state: TokenProgramAccountState,
}

impl TokenAccountParams {
    pub fn create_default(owner: Pubkey, mint: Pubkey, amount: u64) -> TokenAccountParams {
        TokenAccountParams {
            owner,
            mint,
            amount,
            delegate: None,
            is_native: None,
            close_authority: None,
            state: TokenProgramAccountState::Initialized,
        }
    }

    #[allow(dead_code)]
    pub fn create_random(
        rng: &mut ThreadRng,
        owner: Pubkey,
        mint: Pubkey,
        amount: u64,
    ) -> TokenAccountParams {
        let delegate_supply: u64 = rng.gen();
        let state = rng.gen::<u8>() % 2;
        TokenAccountParams {
            owner,
            mint,
            amount,
            delegate: Some((Pubkey::new_unique(), delegate_supply)),
            is_native: rng.gen(),
            close_authority: Some(Pubkey::new_unique()),
            state: match state {
                0 => TokenProgramAccountState::Initialized,
                1 => TokenProgramAccountState::Frozen,
                _ => unreachable!(),
            },
        }
    }
}

#[derive(PartialEq, Eq, Clone, Copy, Debug)]
pub struct MintCreationParams {
    mint_authority: Option<Pubkey>,
    supply: u64,
    decimals: u8,
    is_initialized: bool,
    freeze_authority: Option<Pubkey>,
}

impl MintCreationParams {
    pub fn create_default(supply: u64) -> MintCreationParams {
        MintCreationParams {
            mint_authority: None,
            supply,
            decimals: 6,
            is_initialized: true,
            freeze_authority: None,
        }
    }

    #[allow(dead_code)]
    pub fn create_random(rng: &mut ThreadRng, supply: u64) -> MintCreationParams {
        MintCreationParams {
            mint_authority: Some(Pubkey::new_unique()),
            supply,
            decimals: rng.gen(),
            is_initialized: true,
            freeze_authority: Some(Pubkey::new_unique()),
        }
    }
}

pub fn create_token_account(token_creation_params: TokenAccountParams) -> Vec<u8> {
    let token_account = spl_token::state::Account {
        mint: token_creation_params.mint,
        owner: token_creation_params.owner,
        amount: token_creation_params.amount,
        delegate: token_creation_params
            .delegate
            .map(|(pubkey, _)| pubkey)
            .into(),
        delegated_amount: token_creation_params
            .delegate
            .map(|(_, amount)| amount)
            .unwrap_or_default(),
        state: token_creation_params.state.into_spl_state(),
        is_native: token_creation_params.is_native.into(),
        close_authority: token_creation_params.close_authority.into(),
    };
    let mut data = vec![0; 165];
    spl_token::state::Account::pack(token_account, &mut data).unwrap();
    data
}

pub fn create_token_account_data(
    pubkey: Pubkey,
    token_creation_params: TokenAccountParams,
    slot: Slot,
    write_version: u64,
) -> AccountData {
    AccountData {
        pubkey,
        account: Arc::new(Account {
            lamports: 2039280,
            data: lite_account_manager_common::account_data::Data::Uncompressed(
                create_token_account(token_creation_params),
            ),
            owner: spl_token::id(),
            executable: false,
            rent_epoch: u64::MAX,
            data_length: 165,
        }),
        updated_slot: slot,
        write_version,
    }
}

#[allow(dead_code)]
fn coption_to_option<T>(coption: COption<T>) -> Option<T> {
    match coption {
        COption::Some(t) => Some(t),
        COption::None => None,
    }
}

#[allow(dead_code)]
pub fn parse_account_data_to_token_params(account_data: AccountData) -> TokenAccountParams {
    let token_account =
        spl_token::state::Account::unpack(&account_data.account.data.data()).unwrap();
    let delegate = match token_account.delegate {
        solana_sdk::program_option::COption::Some(delegate) => {
            Some((delegate, token_account.delegated_amount))
        }
        solana_sdk::program_option::COption::None => Option::None::<_>,
    };
    TokenAccountParams {
        owner: token_account.owner,
        mint: token_account.mint,
        amount: token_account.amount,
        delegate,
        is_native: coption_to_option(token_account.is_native),
        close_authority: coption_to_option(token_account.close_authority),
        state: token_account.state.into(),
    }
}

pub fn create_mint(mint_creation_parameters: MintCreationParams) -> Vec<u8> {
    let mint_account = spl_token::state::Mint {
        mint_authority: mint_creation_parameters.mint_authority.into(),
        supply: mint_creation_parameters.supply,
        decimals: mint_creation_parameters.decimals,
        is_initialized: mint_creation_parameters.is_initialized,
        freeze_authority: mint_creation_parameters.freeze_authority.into(),
    };
    let mut data = vec![0; 82];
    spl_token::state::Mint::pack(mint_account, &mut data).unwrap();
    data
}

pub fn create_mint_account_data(
    pubkey: Pubkey,
    mint_creation_params: MintCreationParams,
    slot: Slot,
    write_version: u64,
) -> AccountData {
    AccountData {
        pubkey,
        account: Arc::new(Account {
            lamports: 1000,
            data: lite_account_manager_common::account_data::Data::Uncompressed(create_mint(
                mint_creation_params,
            )),
            owner: spl_token::id(),
            executable: false,
            rent_epoch: u64::MAX,
            data_length: 82,
        }),
        updated_slot: slot,
        write_version,
    }
}

#[allow(dead_code)]
pub fn parse_account_data_to_mint_params(account_data: AccountData) -> MintCreationParams {
    let mint = spl_token::state::Mint::unpack(&account_data.account.data.data()).unwrap();
    MintCreationParams {
        mint_authority: coption_to_option(mint.mint_authority),
        supply: mint.supply,
        decimals: mint.decimals,
        is_initialized: mint.is_initialized,
        freeze_authority: coption_to_option(mint.freeze_authority),
    }
}
