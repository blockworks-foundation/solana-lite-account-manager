use std::{
    cell::OnceCell,
    sync::{atomic::AtomicU32, Arc},
    vec,
};

use crate::account_types::{
    MintAccount, MintIndex, MultiSig, Program, TokenAccount, TokenProgramAccountState,
};
use anyhow::{bail, Context};
use dashmap::DashMap;
use itertools::Itertools;
use lite_account_manager_common::{
    account_data::{Account, AccountData},
    account_filter::{AccountFilterType, MemcmpFilter, MemcmpFilterData},
};
use solana_sdk::{
    program_option::COption,
    program_pack::Pack,
    pubkey::{Pubkey, PUBKEY_BYTES},
    rent::Rent,
};
use spl_token::instruction::MAX_SIGNERS;

#[derive(Clone)]
pub enum TokenProgramAccountType {
    TokenAccount(TokenAccount),
    Mint(MintAccount),
    MultiSig(MultiSig, Pubkey),
    Deleted(Pubkey),
}

impl TokenProgramAccountType {
    pub fn pubkey(&self) -> Pubkey {
        match self {
            TokenProgramAccountType::TokenAccount(acc) => acc.pubkey,
            TokenProgramAccountType::Mint(m) => m.pubkey,
            TokenProgramAccountType::MultiSig(_, pk) => *pk,
            TokenProgramAccountType::Deleted(pk) => *pk,
        }
    }
}

pub fn get_or_create_mint_index(
    mint: Pubkey,
    mint_index_by_pubkey: &Arc<DashMap<Pubkey, MintIndex>>,
    mint_index_count: &Arc<AtomicU32>,
) -> MintIndex {
    match mint_index_by_pubkey.entry(mint) {
        dashmap::mapref::entry::Entry::Occupied(occ) => *occ.get(),
        dashmap::mapref::entry::Entry::Vacant(vac) => {
            let index = mint_index_count.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            vac.insert(index);
            index
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum SplTokenProgram {
    TokenProgram,
    Token2022Program,
}

impl TryFrom<&Pubkey> for SplTokenProgram {
    type Error = ();

    fn try_from(pubkey: &Pubkey) -> Result<Self, Self::Error> {
        match *pubkey {
            spl_token::ID => Ok(SplTokenProgram::TokenProgram),
            spl_token_2022::ID => Ok(SplTokenProgram::Token2022Program),
            _ => Err(()),
        }
    }
}

// this enum covers old and new (2022) accounts
#[derive(Debug, Clone, PartialEq, Eq)]
enum AccountType {
    Mint,
    TokenAccount,
    Multisig,
}

fn get_spl_account_type(
    token_program: SplTokenProgram,
    account_data_len: usize,
    byte_after_account_len: Option<u8>,
) -> anyhow::Result<(AccountType, bool)> {
    match token_program {
        SplTokenProgram::Token2022Program => {
            if account_data_len == spl_token_2022::state::Mint::LEN {
                Ok((AccountType::Mint, false))
            } else if account_data_len == spl_token_2022::state::Account::LEN {
                Ok((AccountType::TokenAccount, false))
            } else if account_data_len == spl_token_2022::state::Multisig::LEN {
                Ok((AccountType::Multisig, false))
            } else if account_data_len > spl_token_2022::state::Account::LEN {
                if let Some(byte_after_account_len) = byte_after_account_len {
                    let account_type = byte_after_account_len;
                    // extended token account
                    if account_type == spl_token_2022::extension::AccountType::Mint as u8 {
                        Ok((AccountType::Mint, true))
                    } else if account_type == spl_token_2022::extension::AccountType::Account as u8
                    {
                        Ok((AccountType::TokenAccount, true))
                    } else if account_type
                        == spl_token_2022::extension::AccountType::Uninitialized as u8
                    {
                        bail!("uninitialized account")
                    } else {
                        bail!("unknown token account type: {}", account_type)
                    }
                } else {
                    bail!("token account is longer than account len but type byte is missing!")
                }
            } else {
                bail!("unknown token account")
            }
        }
        SplTokenProgram::TokenProgram => {
            if account_data_len == spl_token::state::Mint::LEN {
                Ok((AccountType::Mint, false))
            } else if account_data_len == spl_token::state::Account::LEN {
                Ok((AccountType::TokenAccount, false))
            } else if account_data_len == spl_token::state::Multisig::LEN {
                Ok((AccountType::Multisig, false))
            } else {
                bail!("unknown token account")
            }
        }
    }
}

pub fn get_token_program_account_type(
    account_data: &AccountData,
    mint_index_by_pubkey: &Arc<DashMap<Pubkey, MintIndex>>,
    mint_index_count: &Arc<AtomicU32>,
) -> anyhow::Result<TokenProgramAccountType> {
    if account_data.account.lamports == 0 {
        // account owner changed or is deleted
        return Ok(TokenProgramAccountType::Deleted(account_data.pubkey));
    }

    let token_program: SplTokenProgram = match (&account_data.account.owner).try_into() {
        Ok(token_program) => token_program,
        Err(_) => return Ok(TokenProgramAccountType::Deleted(account_data.pubkey)),
    };

    let packed_data = account_data.account.data.data();

    let byte_after_account_len = if token_program == SplTokenProgram::Token2022Program
        && packed_data.len() > spl_token_2022::state::Account::LEN
    {
        packed_data
            .get(spl_token_2022::state::Account::LEN)
            .copied()
    } else {
        None
    };

    let (account_type, has_additional_data) = get_spl_account_type(
        token_program.clone(),
        packed_data.len(),
        byte_after_account_len,
    )?;

    match token_program {
        SplTokenProgram::Token2022Program => {
            match account_type {
                AccountType::Mint => {
                    //mint
                    let mint = spl_token_2022::state::Mint::unpack_unchecked(
                        &packed_data[..spl_token_2022::state::Mint::LEN],
                    )
                    .context("token2022 mint unpack")?;
                    Ok(TokenProgramAccountType::Mint(MintAccount {
                        program: crate::account_types::Program::Token2022Program,
                        pubkey: account_data.pubkey,
                        lamports: account_data.account.lamports,
                        supply: mint.supply,
                        decimals: mint.decimals,
                        is_initialized: mint.is_initialized,
                        mint_authority: mint.mint_authority.into(),
                        freeze_authority: mint.freeze_authority.into(),
                        additional_data: has_additional_data.then(|| packed_data[165..].to_vec()),
                    }))
                }
                AccountType::TokenAccount => {
                    let token_account = spl_token_2022::state::Account::unpack_unchecked(
                        &packed_data[..spl_token_2022::state::Account::LEN],
                    )
                    .context("token2022 account unpack")?;
                    let mint_index = get_or_create_mint_index(
                        token_account.mint,
                        mint_index_by_pubkey,
                        mint_index_count,
                    );
                    Ok(TokenProgramAccountType::TokenAccount(TokenAccount {
                        program: crate::account_types::Program::Token2022Program,
                        is_deleted: account_data.account.lamports == 0,
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
                        additional_data: has_additional_data.then(|| packed_data[165..].to_vec()),
                    }))
                }
                AccountType::Multisig => {
                    let multi_sig = spl_token_2022::state::Multisig::unpack_unchecked(&packed_data)
                        .context("token2022 multisig unpack")?;
                    Ok(TokenProgramAccountType::MultiSig(
                        MultiSig {
                            program: crate::account_types::Program::Token2022Program,
                            pubkey: account_data.pubkey,
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
            }
        }
        SplTokenProgram::TokenProgram => {
            match account_type {
                AccountType::Mint => {
                    let mint =
                        spl_token::state::Mint::unpack_unchecked(&account_data.account.data.data())
                            .context("token mint unpack")?;
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
                }
                AccountType::TokenAccount => {
                    let token_account = spl_token::state::Account::unpack_unchecked(
                        &account_data.account.data.data(),
                    )
                    .context("token account unpack")?;
                    let mint_index = get_or_create_mint_index(
                        token_account.mint,
                        mint_index_by_pubkey,
                        mint_index_count,
                    );
                    Ok(TokenProgramAccountType::TokenAccount(TokenAccount {
                        program: crate::account_types::Program::TokenProgram,
                        pubkey: account_data.pubkey,
                        is_deleted: account_data.account.lamports == 0,
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
                            spl_token::state::AccountState::Frozen => {
                                TokenProgramAccountState::Frozen
                            }
                        },
                        delegate: token_account
                            .delegate
                            .map(|x| (x, token_account.delegated_amount))
                            .into(),
                        is_native: token_account.is_native.into(),
                        close_authority: token_account.close_authority.into(),
                        additional_data: None,
                    }))
                }
                AccountType::Multisig => {
                    let multi_sig = spl_token::state::Multisig::unpack_unchecked(
                        &account_data.account.data.data(),
                    )
                    .context("token multisig unpack")?;
                    Ok(TokenProgramAccountType::MultiSig(
                        MultiSig {
                            program: crate::account_types::Program::TokenProgram,
                            pubkey: account_data.pubkey,
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
            }
        }
    }
}

pub fn token_account_to_solana_account(
    token_account: &TokenAccount,
    updated_slot: u64,
    write_version: u64,
    mints_by_index: &Arc<DashMap<MintIndex, MintAccount>>,
) -> Option<AccountData> {
    if token_account.is_deleted {
        return None;
    }
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
    let rent = Rent::default();
    let account = Arc::new(Account {
        lamports: if token_account.is_deleted {
            0
        } else {
            rent.minimum_balance(data_length as usize)
        },
        data: lite_account_manager_common::account_data::Data::Uncompressed(data),
        owner: match token_account.program {
            Program::TokenProgram => spl_token::id(),
            Program::Token2022Program => spl_token_2022::id(),
        },
        executable: false,
        rent_epoch: u64::MAX,
    });
    Some(AccountData {
        pubkey: token_account.pubkey,
        account,
        updated_slot,
        write_version,
    })
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

    let account = Arc::new(Account {
        lamports: mint_account.lamports,
        data: lite_account_manager_common::account_data::Data::Uncompressed(data),
        owner: match mint_account.program {
            Program::TokenProgram => spl_token::id(),
            Program::Token2022Program => spl_token_2022::id(),
        },
        executable: false,
        rent_epoch: u64::MAX,
    });
    AccountData {
        pubkey: mint_account.pubkey,
        account,
        updated_slot,
        write_version,
    }
}

pub fn token_multisig_to_solana_account(
    multisig: &MultiSig,
    pubkey: Pubkey,
    updated_slot: u64,
    write_version: u64,
) -> AccountData {
    let data = match multisig.program {
        Program::TokenProgram => {
            let mut signers = [Pubkey::default(); MAX_SIGNERS];
            signers[..multisig.signers.len()].copy_from_slice(&multisig.signers);
            let m_acc = spl_token::state::Multisig {
                m: multisig.m,
                n: multisig.n,
                is_initialized: multisig.is_initialized,
                signers,
            };
            let mut data = vec![0; 355];
            m_acc.pack_into_slice(&mut data);
            data
        }
        Program::Token2022Program => {
            let mut signers = [Pubkey::default(); spl_token_2022::instruction::MAX_SIGNERS];
            signers[..multisig.signers.len()].copy_from_slice(&multisig.signers);
            let m_acc = spl_token_2022::state::Multisig {
                m: multisig.m,
                n: multisig.n,
                is_initialized: multisig.is_initialized,
                signers,
            };
            let mut data = vec![0; 355];
            m_acc.pack_into_slice(&mut data);
            data
        }
    };
    let account = Arc::new(Account {
        lamports: multisig.lamports,
        data: lite_account_manager_common::account_data::Data::Uncompressed(data),
        owner: match multisig.program {
            Program::TokenProgram => spl_token::id(),
            Program::Token2022Program => spl_token_2022::id(),
        },
        executable: false,
        rent_epoch: u64::MAX,
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
    mints_by_index: &Arc<DashMap<MintIndex, MintAccount>>,
) -> Option<AccountData> {
    match token_program_account {
        TokenProgramAccountType::TokenAccount(tok_acc) => {
            token_account_to_solana_account(tok_acc, updated_slot, write_version, mints_by_index)
        }
        TokenProgramAccountType::Mint(mint_account) => Some(token_mint_to_solana_account(
            mint_account,
            updated_slot,
            write_version,
        )),
        TokenProgramAccountType::MultiSig(multisig, pubkey) => Some(
            token_multisig_to_solana_account(multisig, *pubkey, updated_slot, write_version),
        ),
        TokenProgramAccountType::Deleted(_) => None,
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TokenProgramAccountFilter {
    AccountFilter(AccountFilter),
    MintFilter,
    MultisigFilter,
    NoFilter,
    FilterError,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AccountFilter {
    pub owner: Option<Pubkey>,
    pub mint: Option<Pubkey>,
}

pub fn get_token_program_account_filter(
    program_id: &Pubkey,
    filters: &[AccountFilterType],
) -> TokenProgramAccountFilter {
    let token_program: SplTokenProgram = match program_id.try_into() {
        Ok(token_program) => token_program,
        Err(_) => return TokenProgramAccountFilter::NoFilter,
    };

    let mut data_size_filter: OnceCell<u64> = OnceCell::new();
    let mut memcmp_filters: Vec<(&u64, &[u8])> = vec![];

    for filter in filters {
        match filter {
            AccountFilterType::DataSize(size) => match data_size_filter.set(*size) {
                Ok(_) => {}
                Err(_) => {
                    log::error!("Multiple data size filters provided");
                    return TokenProgramAccountFilter::FilterError;
                }
            },
            AccountFilterType::Memcmp(MemcmpFilter {
                offset,
                data: MemcmpFilterData::Bytes(bytes),
            }) => {
                memcmp_filters.push((offset, bytes));
            }
            _ => {}
        }
    }

    let byte_after_account_len = (token_program == SplTokenProgram::Token2022Program)
        .then(|| {
            memcmp_filters
                .iter()
                .find(|(offset, _)| **offset == spl_token_2022::state::Account::LEN as u64)
                .and_then(|(_, bytes)| bytes.get(0).copied())
        })
        .flatten();

    let account_type = get_spl_account_type(
        token_program.clone(),
        data_size_filter.take().unwrap_or_default() as usize,
        byte_after_account_len,
    );

    let account_type = match account_type {
        Ok((account_type, _)) => account_type,
        Err(e) => {
            log::warn!("Error getting account type: {:?}", e);
            return TokenProgramAccountFilter::NoFilter;
        }
    };

    match account_type {
        AccountType::TokenAccount => {
            const SPL_TOKEN_ACCOUNT_MINT_OFFSET: u64 = 0;
            const SPL_TOKEN_ACCOUNT_OWNER_OFFSET: u64 = 32;

            let mint_filter = memcmp_filters
                .iter()
                .filter(|(offset, _)| *offset == &SPL_TOKEN_ACCOUNT_MINT_OFFSET)
                .at_most_one()
                .map_err(|_| {
                    log::error!("Multiple filters provided for token account mint");
                    TokenProgramAccountFilter::FilterError
                })
                .and_then(|filter| {
                    filter.map_or(Ok(None), |(_, bytes)| {
                        if bytes.len() != PUBKEY_BYTES {
                            log::error!(
                                "Incorrect num bytes ({:?}) provided for token account mint filter",
                                bytes.len()
                            );
                            return Err(TokenProgramAccountFilter::FilterError);
                        }

                        Ok(Pubkey::try_from(*bytes).ok())
                    })
                });

            let owner_filter =  memcmp_filters
                .iter()
                .filter(|(offset, _)| *offset == &SPL_TOKEN_ACCOUNT_OWNER_OFFSET)
                .at_most_one()
                .map_err(|_| {
                    log::error!("Multiple filters provided for token account owner");
                    TokenProgramAccountFilter::FilterError
                })
                .and_then(|filter| {
                    filter.map_or(Ok(None), |(_, bytes)| {
                        if bytes.len() != PUBKEY_BYTES {
                            log::error!(
                                "Incorrect num bytes ({:?}) provided for token account owner filter",
                                bytes.len()
                            );
                            return Err(TokenProgramAccountFilter::FilterError);
                        }

                        Ok(Pubkey::try_from(*bytes).ok())
                    })
                });

            owner_filter
                .and_then(|owner_filter| {
                    mint_filter.map(|mint_filter| {
                        TokenProgramAccountFilter::AccountFilter(AccountFilter {
                            owner: owner_filter,
                            mint: mint_filter,
                        })
                    })
                })
                .unwrap_or_else(|res| res)
        }
        AccountType::Mint => TokenProgramAccountFilter::MintFilter,
        AccountType::Multisig => TokenProgramAccountFilter::MultisigFilter,
    }
}

#[cfg(test)]
mod test {
    mod get_token_program_account_filter {
        use lite_account_manager_common::account_filter::{
            AccountFilterType, MemcmpFilter, MemcmpFilterData,
        };
        use solana_sdk::program_pack::Pack;

        use crate::token_program_utils::{
            get_token_program_account_filter, AccountFilter, TokenProgramAccountFilter,
        };

        #[test]
        fn test_filter_for_non_token_program() {
            let filters = vec![AccountFilterType::DataSize(
                spl_token_2022::state::Mint::LEN as u64,
            )];
            let filter =
                get_token_program_account_filter(&solana_sdk::pubkey::new_rand(), &filters);

            assert_eq!(filter, TokenProgramAccountFilter::NoFilter);
        }

        #[test]
        fn test_filter_for_multiple_data_sizes() {
            let filters = vec![
                AccountFilterType::DataSize(spl_token_2022::state::Mint::LEN as u64),
                AccountFilterType::DataSize(spl_token_2022::state::Mint::LEN as u64),
            ];
            let filter = get_token_program_account_filter(&spl_token_2022::ID, &filters);

            assert_eq!(filter, TokenProgramAccountFilter::FilterError);
        }

        #[test]
        fn test_overlapping_token_account_or_owner_mint_filters() {
            // multiple filters for mint
            let filters = vec![
                AccountFilterType::DataSize(spl_token::state::Account::LEN as u64),
                AccountFilterType::Memcmp(MemcmpFilter {
                    offset: 0,
                    data: MemcmpFilterData::Bytes(
                        solana_sdk::pubkey::new_rand().to_bytes().to_vec(),
                    ),
                }),
                AccountFilterType::Memcmp(MemcmpFilter {
                    offset: 0,
                    data: MemcmpFilterData::Bytes(
                        solana_sdk::pubkey::new_rand().to_bytes().to_vec(),
                    ),
                }),
            ];

            let filter = get_token_program_account_filter(&spl_token::ID, &filters);

            assert_eq!(filter, TokenProgramAccountFilter::FilterError);

            // incorrect mint filter data size
            let filters = vec![
                AccountFilterType::DataSize(spl_token::state::Account::LEN as u64),
                AccountFilterType::Memcmp(MemcmpFilter {
                    offset: 0,
                    data: MemcmpFilterData::Bytes(
                        solana_sdk::pubkey::new_rand().to_bytes()[..10].to_vec(),
                    ),
                }),
            ];

            let filter = get_token_program_account_filter(&spl_token::ID, &filters);

            assert_eq!(filter, TokenProgramAccountFilter::FilterError);

            // multiple filters for owner
            let filters = vec![
                AccountFilterType::DataSize(spl_token::state::Account::LEN as u64),
                AccountFilterType::Memcmp(MemcmpFilter {
                    offset: 32,
                    data: MemcmpFilterData::Bytes(
                        solana_sdk::pubkey::new_rand().to_bytes().to_vec(),
                    ),
                }),
                AccountFilterType::Memcmp(MemcmpFilter {
                    offset: 32,
                    data: MemcmpFilterData::Bytes(
                        solana_sdk::pubkey::new_rand().to_bytes().to_vec(),
                    ),
                }),
            ];

            let filter = get_token_program_account_filter(&spl_token::ID, &filters);

            assert_eq!(filter, TokenProgramAccountFilter::FilterError);

            // incorrect owner filter data size
            let filters = vec![
                AccountFilterType::DataSize(spl_token::state::Account::LEN as u64),
                AccountFilterType::Memcmp(MemcmpFilter {
                    offset: 32,
                    data: MemcmpFilterData::Bytes(
                        solana_sdk::pubkey::new_rand().to_bytes()[..10].to_vec(),
                    ),
                }),
            ];

            let filter = get_token_program_account_filter(&spl_token::ID, &filters);

            assert_eq!(filter, TokenProgramAccountFilter::FilterError);
        }

        #[test]
        fn test_filter_without_data_size() {
            let filters = vec![];
            let filter =
                get_token_program_account_filter(&solana_sdk::pubkey::new_rand(), &filters);

            assert_eq!(filter, TokenProgramAccountFilter::NoFilter);
        }

        #[test]
        fn test_filter_for_mint() {
            let filters = vec![AccountFilterType::DataSize(
                spl_token::state::Mint::LEN as u64,
            )];
            let filter = get_token_program_account_filter(&spl_token::ID, &filters);

            assert_eq!(filter, TokenProgramAccountFilter::MintFilter);

            let filters = vec![AccountFilterType::DataSize(
                spl_token_2022::state::Mint::LEN as u64,
            )];
            let filter = get_token_program_account_filter(&spl_token_2022::ID, &filters);

            assert_eq!(filter, TokenProgramAccountFilter::MintFilter);
        }

        #[test]
        fn test_filter_for_multisig() {
            let filters = vec![AccountFilterType::DataSize(
                spl_token::state::Multisig::LEN as u64,
            )];
            let filter = get_token_program_account_filter(&spl_token::ID, &filters);

            assert_eq!(filter, TokenProgramAccountFilter::MultisigFilter);

            let filters = vec![AccountFilterType::DataSize(
                spl_token_2022::state::Multisig::LEN as u64,
            )];
            let filter = get_token_program_account_filter(&spl_token_2022::ID, &filters);

            assert_eq!(filter, TokenProgramAccountFilter::MultisigFilter);
        }

        #[test]
        fn test_filter_for_token_account() {
            let data_size = spl_token::state::Account::LEN as u64;

            // filter only by token accounts
            let filters = vec![AccountFilterType::DataSize(data_size)];
            let filter = get_token_program_account_filter(&spl_token::ID, &filters);

            assert_eq!(
                filter,
                TokenProgramAccountFilter::AccountFilter(AccountFilter {
                    owner: None,
                    mint: None,
                })
            );

            // filter by token accounts && mint
            let mint_pubkey = solana_sdk::pubkey::new_rand();
            let filters = vec![
                AccountFilterType::DataSize(data_size),
                AccountFilterType::Memcmp(MemcmpFilter {
                    offset: 0,
                    data: MemcmpFilterData::Bytes(mint_pubkey.to_bytes().to_vec()),
                }),
            ];
            let filter = get_token_program_account_filter(&spl_token::ID, &filters);

            assert_eq!(
                filter,
                TokenProgramAccountFilter::AccountFilter(AccountFilter {
                    owner: None,
                    mint: mint_pubkey.into(),
                })
            );

            // filter by token accounts && owner
            let owner_pubkey = solana_sdk::pubkey::new_rand();
            let filters = vec![
                AccountFilterType::DataSize(data_size),
                AccountFilterType::Memcmp(MemcmpFilter {
                    offset: 32,
                    data: MemcmpFilterData::Bytes(owner_pubkey.to_bytes().to_vec()),
                }),
            ];
            let filter = get_token_program_account_filter(&spl_token::ID, &filters);

            assert_eq!(
                filter,
                TokenProgramAccountFilter::AccountFilter(AccountFilter {
                    owner: owner_pubkey.into(),
                    mint: None,
                })
            );

            // filter by token accounts && mint && owner
            let mint_pubkey = solana_sdk::pubkey::new_rand();
            let owner_pubkey = solana_sdk::pubkey::new_rand();
            let filters = vec![
                AccountFilterType::DataSize(spl_token::state::Account::LEN as u64),
                AccountFilterType::Memcmp(MemcmpFilter {
                    offset: 0,
                    data: MemcmpFilterData::Bytes(mint_pubkey.to_bytes().to_vec()),
                }),
                AccountFilterType::Memcmp(MemcmpFilter {
                    offset: 32,
                    data: MemcmpFilterData::Bytes(owner_pubkey.to_bytes().to_vec()),
                }),
            ];
            let filter = get_token_program_account_filter(&spl_token::ID, &filters);

            assert_eq!(
                filter,
                TokenProgramAccountFilter::AccountFilter(AccountFilter {
                    owner: owner_pubkey.into(),
                    mint: mint_pubkey.into(),
                })
            );
        }

        #[test]
        fn test_filter_for_token_account_2022_without_extensions() {
            let data_size = spl_token_2022::state::Account::LEN as u64;
            // filter only by token accounts
            let filters = vec![AccountFilterType::DataSize(data_size)];
            let filter = get_token_program_account_filter(&spl_token_2022::ID, &filters);

            assert_eq!(
                filter,
                TokenProgramAccountFilter::AccountFilter(AccountFilter {
                    owner: None,
                    mint: None,
                })
            );

            // filter by token accounts && mint
            let mint_pubkey = solana_sdk::pubkey::new_rand();
            let filters = vec![
                AccountFilterType::DataSize(data_size),
                AccountFilterType::Memcmp(MemcmpFilter {
                    offset: 0,
                    data: MemcmpFilterData::Bytes(mint_pubkey.to_bytes().to_vec()),
                }),
            ];
            let filter = get_token_program_account_filter(&spl_token_2022::ID, &filters);

            assert_eq!(
                filter,
                TokenProgramAccountFilter::AccountFilter(AccountFilter {
                    owner: None,
                    mint: mint_pubkey.into(),
                })
            );

            // filter by token accounts && owner
            let owner_pubkey = solana_sdk::pubkey::new_rand();
            let filters = vec![
                AccountFilterType::DataSize(data_size),
                AccountFilterType::Memcmp(MemcmpFilter {
                    offset: 32,
                    data: MemcmpFilterData::Bytes(owner_pubkey.to_bytes().to_vec()),
                }),
            ];
            let filter = get_token_program_account_filter(&spl_token_2022::ID, &filters);

            assert_eq!(
                filter,
                TokenProgramAccountFilter::AccountFilter(AccountFilter {
                    owner: owner_pubkey.into(),
                    mint: None,
                })
            );

            // filter by token accounts && mint && owner
            let mint_pubkey = solana_sdk::pubkey::new_rand();
            let owner_pubkey = solana_sdk::pubkey::new_rand();
            let filters = vec![
                AccountFilterType::DataSize(data_size),
                AccountFilterType::Memcmp(MemcmpFilter {
                    offset: 0,
                    data: MemcmpFilterData::Bytes(mint_pubkey.to_bytes().to_vec()),
                }),
                AccountFilterType::Memcmp(MemcmpFilter {
                    offset: 32,
                    data: MemcmpFilterData::Bytes(owner_pubkey.to_bytes().to_vec()),
                }),
            ];
            let filter = get_token_program_account_filter(&spl_token_2022::ID, &filters);

            assert_eq!(
                filter,
                TokenProgramAccountFilter::AccountFilter(AccountFilter {
                    owner: owner_pubkey.into(),
                    mint: mint_pubkey.into(),
                })
            );
        }

        #[test]
        fn test_filter_for_token_account_2022_with_extensions() {
            let data_size = spl_token_2022::state::Account::LEN as u64 + 1;

            // filter by token accounts with extensions without specifying the type
            let filters = vec![AccountFilterType::DataSize(data_size)];
            let filter = get_token_program_account_filter(&spl_token_2022::ID, &filters);

            assert_eq!(filter, TokenProgramAccountFilter::NoFilter);

            // filter by token accounts with extensions specifying the type
            let filters = vec![
                AccountFilterType::DataSize(data_size),
                AccountFilterType::Memcmp(MemcmpFilter {
                    offset: spl_token_2022::state::Account::LEN as u64,
                    data: MemcmpFilterData::Bytes(vec![
                        spl_token_2022::extension::AccountType::Account as u8,
                    ]),
                }),
            ];
            let filter = get_token_program_account_filter(&spl_token_2022::ID, &filters);

            assert_eq!(
                filter,
                TokenProgramAccountFilter::AccountFilter(AccountFilter {
                    owner: None,
                    mint: None,
                })
            );

            // filter by token accounts with extensions && mint
            let mint_pubkey = solana_sdk::pubkey::new_rand();
            let filters = vec![
                AccountFilterType::DataSize(data_size),
                AccountFilterType::Memcmp(MemcmpFilter {
                    offset: spl_token_2022::state::Account::LEN as u64,
                    data: MemcmpFilterData::Bytes(vec![
                        spl_token_2022::extension::AccountType::Account as u8,
                    ]),
                }),
                AccountFilterType::Memcmp(MemcmpFilter {
                    offset: 0,
                    data: MemcmpFilterData::Bytes(mint_pubkey.to_bytes().to_vec()),
                }),
            ];
            let filter = get_token_program_account_filter(&spl_token_2022::ID, &filters);

            assert_eq!(
                filter,
                TokenProgramAccountFilter::AccountFilter(AccountFilter {
                    owner: None,
                    mint: mint_pubkey.into(),
                })
            );

            // filter by token accounts with extensions && owner
            let owner_pubkey = solana_sdk::pubkey::new_rand();
            let filters = vec![
                AccountFilterType::DataSize(data_size),
                AccountFilterType::Memcmp(MemcmpFilter {
                    offset: spl_token_2022::state::Account::LEN as u64,
                    data: MemcmpFilterData::Bytes(vec![
                        spl_token_2022::extension::AccountType::Account as u8,
                    ]),
                }),
                AccountFilterType::Memcmp(MemcmpFilter {
                    offset: 32,
                    data: MemcmpFilterData::Bytes(owner_pubkey.to_bytes().to_vec()),
                }),
            ];
            let filter = get_token_program_account_filter(&spl_token_2022::ID, &filters);

            assert_eq!(
                filter,
                TokenProgramAccountFilter::AccountFilter(AccountFilter {
                    owner: owner_pubkey.into(),
                    mint: None,
                })
            );

            // filter by token accounts && mint && owner
            let mint_pubkey = solana_sdk::pubkey::new_rand();
            let owner_pubkey = solana_sdk::pubkey::new_rand();
            let filters = vec![
                AccountFilterType::DataSize(data_size),
                AccountFilterType::Memcmp(MemcmpFilter {
                    offset: spl_token_2022::state::Account::LEN as u64,
                    data: MemcmpFilterData::Bytes(vec![
                        spl_token_2022::extension::AccountType::Account as u8,
                    ]),
                }),
                AccountFilterType::Memcmp(MemcmpFilter {
                    offset: 0,
                    data: MemcmpFilterData::Bytes(mint_pubkey.to_bytes().to_vec()),
                }),
                AccountFilterType::Memcmp(MemcmpFilter {
                    offset: 32,
                    data: MemcmpFilterData::Bytes(owner_pubkey.to_bytes().to_vec()),
                }),
            ];
            let filter = get_token_program_account_filter(&spl_token_2022::ID, &filters);

            assert_eq!(
                filter,
                TokenProgramAccountFilter::AccountFilter(AccountFilter {
                    owner: owner_pubkey.into(),
                    mint: mint_pubkey.into(),
                })
            );
        }
    }
}
