use crate::account_types::{MintAccount, MultiSig, TokenAccount, TokenProgramAccountState};
use anyhow::bail;
use itertools::Itertools;
use lite_account_manager_common::account_data::AccountData;
use solana_sdk::{program_pack::Pack, pubkey::Pubkey};

pub enum TokenProgramAccountType {
    TokenAccount(TokenAccount, Pubkey),
    Mint(MintAccount),
    MultiSig(MultiSig, Pubkey),
}

pub fn get_token_program_account_type(
    account_data: &AccountData,
) -> anyhow::Result<TokenProgramAccountType> {
    if account_data.account.owner == spl_token_2022::ID {
        let data = account_data.account.data.data();
        let (type_account, additional_data) = if account_data.account.data_length == 82 {
            //mint
            (0, false)
        } else if account_data.account.data_length == 165 {
            // token account
            (1, false)
        } else if account_data.account.data_length == 355 {
            // multisig
            (2, false)
        } else if account_data.account.data_length > 165 {
            // extended token account
            if data[165] == 1 {
                //mint
                (0, false)
            } else if data[165] == 2 {
                // token account
                (1, false)
            } else {
                bail!("unknown token account")
            }
        } else {
            bail!("unknown token account")
        };

        match type_account {
            0 => {
                //mint
                let mint = spl_token_2022::state::Mint::unpack(&account_data.account.data.data())?;
                Ok(TokenProgramAccountType::Mint(MintAccount {
                    program: crate::account_types::Program::Token2022Program,
                    pubkey: account_data.pubkey,
                    supply: mint.supply,
                    decimals: mint.decimals,
                    is_initialized: mint.is_initialized,
                    mint_authority: mint.mint_authority.into(),
                    freeze_authority: mint.freeze_authority.into(),
                    additional_data: additional_data.then(|| data[165..].to_vec()),
                }))
            }
            1 => {
                let token_account =
                    spl_token_2022::state::Account::unpack(&account_data.account.data.data())?;
                Ok(TokenProgramAccountType::TokenAccount(
                    TokenAccount {
                        program: crate::account_types::Program::Token2022Program,
                        pubkey: account_data.pubkey,
                        mint: 0, // mint should be set inmemory account
                        owner: token_account.owner,
                        amount: token_account.amount,
                        state: match token_account.state {
                            spl_token_2022::state::AccountState::Uninitialized => {
                                TokenProgramAccountState::Uninitialized
                            }
                            spl_token_2022::state::AccountState::Initialized => {
                                TokenProgramAccountState::Initialized
                            }
                            spl_token_2022::state::AccountState::Frozen => {
                                TokenProgramAccountState::Frozen
                            }
                        },
                        delegate: token_account
                            .delegate
                            .map(|x| (x, token_account.delegated_amount))
                            .into(),
                        is_native: token_account.is_native.into(),
                        close_authority: token_account.close_authority.into(),
                        additional_data: additional_data.then(|| data[165..].to_vec()),
                    },
                    token_account.mint,
                ))
            }
            2 => {
                let multi_sig =
                    spl_token_2022::state::Multisig::unpack(&account_data.account.data.data())?;
                Ok(TokenProgramAccountType::MultiSig(
                    MultiSig {
                        program: crate::account_types::Program::Token2022Program,
                        m: multi_sig.m,
                        n: multi_sig.n,
                        signers: multi_sig
                            .signers
                            .iter()
                            .filter(|x| **x != Pubkey::default())
                            .copied()
                            .collect_vec(),
                    },
                    account_data.pubkey,
                ))
            }
            _ => unreachable!(),
        }
    } else if account_data.account.owner == spl_token::ID {
        if account_data.account.data_length == 82 {
            // mint
            let mint = spl_token::state::Mint::unpack(&account_data.account.data.data())?;
            Ok(TokenProgramAccountType::Mint(MintAccount {
                program: crate::account_types::Program::TokenProgram,
                pubkey: account_data.pubkey,
                supply: mint.supply,
                decimals: mint.decimals,
                is_initialized: mint.is_initialized,
                mint_authority: mint.mint_authority.into(),
                freeze_authority: mint.freeze_authority.into(),
                additional_data: None,
            }))
        } else if account_data.account.data_length == 165 {
            //token account
            let token_account =
                spl_token::state::Account::unpack(&account_data.account.data.data())?;
            Ok(TokenProgramAccountType::TokenAccount(
                TokenAccount {
                    program: crate::account_types::Program::TokenProgram,
                    pubkey: account_data.pubkey,
                    mint: 0, // mint should be set inmemory account
                    owner: token_account.owner,
                    amount: token_account.amount,
                    state: match token_account.state {
                        spl_token::state::AccountState::Uninitialized => {
                            TokenProgramAccountState::Uninitialized
                        }
                        spl_token::state::AccountState::Initialized => {
                            TokenProgramAccountState::Initialized
                        }
                        spl_token::state::AccountState::Frozen => TokenProgramAccountState::Frozen,
                    },
                    delegate: token_account
                        .delegate
                        .map(|x| (x, token_account.delegated_amount))
                        .into(),
                    is_native: token_account.is_native.into(),
                    close_authority: token_account.close_authority.into(),
                    additional_data: None,
                },
                token_account.mint,
            ))
        } else {
            // multisig
            assert_eq!(account_data.account.data_length, 355);
            let multi_sig = spl_token::state::Multisig::unpack(&account_data.account.data.data())?;
            Ok(TokenProgramAccountType::MultiSig(
                MultiSig {
                    program: crate::account_types::Program::TokenProgram,
                    m: multi_sig.m,
                    n: multi_sig.n,
                    signers: multi_sig
                        .signers
                        .iter()
                        .filter(|x| **x != Pubkey::default())
                        .copied()
                        .collect_vec(),
                },
                account_data.pubkey,
            ))
        }
    } else {
        bail!("Account does not belong to token program");
    }
}
