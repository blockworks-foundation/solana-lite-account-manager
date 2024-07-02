use std::sync::{atomic::AtomicU64, Arc};

use crate::account_types::{
    MintAccount, MintData, MultiSig, Program, TokenAccount, TokenProgramAccountState,
};
use anyhow::bail;
use dashmap::DashMap;
use itertools::Itertools;
use lite_account_manager_common::account_data::{Account, AccountData};
use solana_sdk::{program_option::COption, program_pack::Pack, pubkey::Pubkey};
use spl_token::instruction::MAX_SIGNERS;

pub enum TokenProgramAccountType {
    TokenAccount(TokenAccount),
    Mint(MintAccount),
    MultiSig(MultiSig, Pubkey),
}

pub fn get_or_create_mint_index(
    mint: Pubkey,
    mint_index_by_pubkey: &Arc<DashMap<Pubkey, u64>>,
    mint_index_count: &Arc<AtomicU64>,
) -> u64 {
    match mint_index_by_pubkey.entry(mint) {
        dashmap::mapref::entry::Entry::Occupied(occ) => *occ.get(),
        dashmap::mapref::entry::Entry::Vacant(vac) => {
            let index = mint_index_count.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            vac.insert(index);
            index
        }
    }
}

pub fn get_token_program_account_type(
    account_data: &AccountData,
    mint_index_by_pubkey: &Arc<DashMap<Pubkey, u64>>,
    mint_index_count: &Arc<AtomicU64>,
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
                    lamports: account_data.account.lamports,
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
                let mint_index = get_or_create_mint_index(
                    token_account.mint,
                    mint_index_by_pubkey,
                    mint_index_count,
                );
                Ok(TokenProgramAccountType::TokenAccount(TokenAccount {
                    program: crate::account_types::Program::Token2022Program,
                    lamports: account_data.account.lamports,
                    pubkey: account_data.pubkey,
                    mint: mint_index, // mint should be set inmemory account
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
                }))
            }
            2 => {
                let multi_sig =
                    spl_token_2022::state::Multisig::unpack(&account_data.account.data.data())?;
                Ok(TokenProgramAccountType::MultiSig(
                    MultiSig {
                        program: crate::account_types::Program::Token2022Program,
                        lamports: account_data.account.lamports,
                        is_initialized: multi_sig.is_initialized,
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
                lamports: account_data.account.lamports,
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
            let mint_index = get_or_create_mint_index(
                token_account.mint,
                mint_index_by_pubkey,
                mint_index_count,
            );
            Ok(TokenProgramAccountType::TokenAccount(TokenAccount {
                program: crate::account_types::Program::TokenProgram,
                pubkey: account_data.pubkey,
                lamports: account_data.account.lamports,
                mint: mint_index, // mint should be set inmemory account
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
            }))
        } else {
            // multisig
            assert_eq!(account_data.account.data_length, 355);
            let multi_sig = spl_token::state::Multisig::unpack(&account_data.account.data.data())?;
            Ok(TokenProgramAccountType::MultiSig(
                MultiSig {
                    program: crate::account_types::Program::TokenProgram,
                    lamports: account_data.account.lamports,
                    is_initialized: multi_sig.is_initialized,
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

pub fn token_account_to_solana_account(
    token_account: &TokenAccount,
    updated_slot: u64,
    write_version: u64,
    mints_by_index: &Arc<DashMap<u64, MintData>>,
) -> AccountData {
    let (delegate, delegated_amount) = token_account.delegate.unwrap_or_default();
    let mint = mints_by_index.get(&token_account.mint).unwrap();
    let data = match token_account.program {
        Program::TokenProgram => {
            let t_acc = spl_token::state::Account {
                mint: mint.pubkey,
                owner: token_account.owner,
                amount: token_account.amount,
                delegate: if token_account.delegate.is_some() {
                    COption::Some(delegate)
                } else {
                    COption::None
                },
                state: match token_account.state {
                    TokenProgramAccountState::Uninitialized => {
                        spl_token::state::AccountState::Uninitialized
                    }
                    TokenProgramAccountState::Initialized => {
                        spl_token::state::AccountState::Initialized
                    }
                    TokenProgramAccountState::Frozen => spl_token::state::AccountState::Frozen,
                },
                is_native: token_account.is_native.into(),
                delegated_amount,
                close_authority: token_account.close_authority.into(),
            };
            let mut data = vec![0; 165];
            t_acc.pack_into_slice(&mut data);
            data
        }
        Program::Token2022Program => {
            let t_acc = spl_token_2022::state::Account {
                mint: mint.pubkey,
                owner: token_account.owner,
                amount: token_account.amount,
                delegate: if token_account.delegate.is_some() {
                    COption::Some(delegate)
                } else {
                    COption::None
                },
                state: match token_account.state {
                    TokenProgramAccountState::Uninitialized => {
                        spl_token_2022::state::AccountState::Uninitialized
                    }
                    TokenProgramAccountState::Initialized => {
                        spl_token_2022::state::AccountState::Initialized
                    }
                    TokenProgramAccountState::Frozen => spl_token_2022::state::AccountState::Frozen,
                },
                is_native: token_account.is_native.into(),
                delegated_amount,
                close_authority: token_account.close_authority.into(),
            };
            let mut data = vec![0; 165];
            t_acc.pack_into_slice(&mut data);
            if let Some(additional_data) = &token_account.additional_data {
                // append additional data
                data.extend_from_slice(additional_data);
            }
            data
        }
    };
    let data_length = data.len() as u64;
    let account = Arc::new(Account {
        lamports: token_account.lamports,
        data: lite_account_manager_common::account_data::Data::Uncompressed(data),
        owner: match token_account.program {
            Program::TokenProgram => spl_token::id(),
            Program::Token2022Program => spl_token_2022::id(),
        },
        executable: false,
        rent_epoch: u64::MAX,
        data_length,
    });
    AccountData {
        pubkey: token_account.pubkey,
        account,
        updated_slot,
        write_version,
    }
}

pub fn token_mint_to_solana_account(
    mint_account: &MintAccount,
    updated_slot: u64,
    write_version: u64,
) -> AccountData {
    let data = match mint_account.program {
        Program::TokenProgram => {
            let m_acc = spl_token::state::Mint {
                mint_authority: mint_account.mint_authority.into(),
                supply: mint_account.supply,
                decimals: mint_account.decimals,
                is_initialized: mint_account.is_initialized,
                freeze_authority: mint_account.freeze_authority.into(),
            };
            let mut data = vec![0; 82];
            m_acc.pack_into_slice(&mut data);
            data
        }
        Program::Token2022Program => {
            let m_acc = spl_token_2022::state::Mint {
                mint_authority: mint_account.mint_authority.into(),
                supply: mint_account.supply,
                decimals: mint_account.decimals,
                is_initialized: mint_account.is_initialized,
                freeze_authority: mint_account.freeze_authority.into(),
            };
            let size = if mint_account.additional_data.is_some() {
                // additional data will be extended from 165th byte
                165
            } else {
                82
            };
            let mut data = vec![0; size];
            m_acc.pack_into_slice(&mut data);
            if let Some(additional_data) = &mint_account.additional_data {
                data.extend_from_slice(additional_data);
            }
            data
        }
    };

    let data_length = data.len() as u64;
    let account = Arc::new(Account {
        lamports: mint_account.lamports,
        data: lite_account_manager_common::account_data::Data::Uncompressed(data),
        owner: match mint_account.program {
            Program::TokenProgram => spl_token::id(),
            Program::Token2022Program => spl_token_2022::id(),
        },
        executable: false,
        rent_epoch: u64::MAX,
        data_length,
    });
    AccountData {
        pubkey: mint_account.pubkey,
        account,
        updated_slot,
        write_version,
    }
}

pub fn token_multisig_to_solana_account(
    multsig: &MultiSig,
    pubkey: Pubkey,
    updated_slot: u64,
    write_version: u64,
) -> AccountData {
    let data = match multsig.program {
        Program::TokenProgram => {
            let mut signers = [Pubkey::default(); MAX_SIGNERS];
            signers[..multsig.signers.len()].copy_from_slice(&multsig.signers);
            let m_acc = spl_token::state::Multisig {
                m: multsig.m,
                n: multsig.n,
                is_initialized: multsig.is_initialized,
                signers,
            };
            let mut data = vec![0; 355];
            m_acc.pack_into_slice(&mut data);
            data
        }
        Program::Token2022Program => {
            let mut signers = [Pubkey::default(); spl_token_2022::instruction::MAX_SIGNERS];
            signers[..multsig.signers.len()].copy_from_slice(&multsig.signers);
            let m_acc = spl_token_2022::state::Multisig {
                m: multsig.m,
                n: multsig.n,
                is_initialized: multsig.is_initialized,
                signers,
            };
            let mut data = vec![0; 355];
            m_acc.pack_into_slice(&mut data);
            data
        }
    };
    let data_length = data.len() as u64;
    let account = Arc::new(Account {
        lamports: multsig.lamports,
        data: lite_account_manager_common::account_data::Data::Uncompressed(data),
        owner: match multsig.program {
            Program::TokenProgram => spl_token::id(),
            Program::Token2022Program => spl_token_2022::id(),
        },
        executable: false,
        rent_epoch: u64::MAX,
        data_length,
    });
    AccountData {
        pubkey,
        account,
        updated_slot,
        write_version,
    }
}

pub fn token_program_account_to_solana_account(
    token_program_account: &TokenProgramAccountType,
    updated_slot: u64,
    write_version: u64,
    mints_by_index: &Arc<DashMap<u64, MintData>>,
) -> AccountData {
    match token_program_account {
        TokenProgramAccountType::TokenAccount(tok_acc) => {
            token_account_to_solana_account(tok_acc, updated_slot, write_version, mints_by_index)
        }
        TokenProgramAccountType::Mint(mint_account) => {
            token_mint_to_solana_account(mint_account, updated_slot, write_version)
        }
        TokenProgramAccountType::MultiSig(multisig, pubkey) => {
            token_multisig_to_solana_account(multisig, *pubkey, updated_slot, write_version)
        }
    }
}
