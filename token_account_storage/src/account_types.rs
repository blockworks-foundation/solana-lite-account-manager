use serde::{Deserialize, Serialize};
use solana_sdk::pubkey::Pubkey;

pub type MintIndex = u32;
pub type TokenAccountIndex = u32;

#[derive(Debug, Serialize, Deserialize, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
#[repr(u8)]
pub enum Program {
    TokenProgram,
    Token2022Program,
}

#[derive(Debug, Serialize, Deserialize, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
#[repr(u8)]
pub enum TokenProgramAccountState {
    Uninitialized,
    Initialized,
    Frozen,
}

impl TokenProgramAccountState {
    pub fn into_spl_state(&self) -> spl_token::state::AccountState {
        match self {
            TokenProgramAccountState::Uninitialized => {
                spl_token::state::AccountState::Uninitialized
            }
            TokenProgramAccountState::Initialized => spl_token::state::AccountState::Initialized,
            TokenProgramAccountState::Frozen => spl_token::state::AccountState::Frozen,
        }
    }

    pub fn into_spl_2022_state(&self) -> spl_token_2022::state::AccountState {
        match self {
            TokenProgramAccountState::Uninitialized => {
                spl_token_2022::state::AccountState::Uninitialized
            }
            TokenProgramAccountState::Initialized => {
                spl_token_2022::state::AccountState::Initialized
            }
            TokenProgramAccountState::Frozen => spl_token_2022::state::AccountState::Frozen,
        }
    }
}

impl From<spl_token::state::AccountState> for TokenProgramAccountState {
    fn from(value: spl_token::state::AccountState) -> Self {
        match value {
            spl_token::state::AccountState::Uninitialized => Self::Uninitialized,
            spl_token::state::AccountState::Initialized => Self::Initialized,
            spl_token::state::AccountState::Frozen => Self::Frozen,
        }
    }
}

impl From<spl_token_2022::state::AccountState> for TokenProgramAccountState {
    fn from(value: spl_token_2022::state::AccountState) -> Self {
        match value {
            spl_token_2022::state::AccountState::Uninitialized => Self::Uninitialized,
            spl_token_2022::state::AccountState::Initialized => Self::Initialized,
            spl_token_2022::state::AccountState::Frozen => Self::Frozen,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct TokenAccount {
    pub program: Program,                 // 1 byte, offset : 0,
    pub is_deleted: bool,                 // 1, offset : 1
    pub pubkey: Pubkey,                   // 32, offset: 2
    pub owner: Pubkey,                    // 32, offset : 34
    pub mint: MintIndex,                  // 4, offset : 66
    pub amount: u64,                      // 8, offset : 70
    pub state: TokenProgramAccountState,  // 1
    pub delegate: Option<(Pubkey, u64)>,  // 1
    pub is_native: Option<u64>,           // 1
    pub close_authority: Option<Pubkey>,  // 1
    pub additional_data: Option<Vec<u8>>, // 0
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct MintAccount {
    pub program: Program,
    pub pubkey: Pubkey,
    pub lamports: u64,
    pub supply: u64,
    pub decimals: u8,
    pub is_initialized: bool,
    pub mint_authority: Option<Pubkey>,
    pub freeze_authority: Option<Pubkey>,
    pub additional_data: Option<Vec<u8>>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct MultiSig {
    pub program: Program,
    pub pubkey: Pubkey,
    pub lamports: u64,
    pub m: u8,
    pub n: u8,
    pub is_initialized: bool,
    pub signers: Vec<Pubkey>,
}

const INITIALIZED_STATE_BITS: u8 = 0b00000000;
const FROZEN_STATE_BITS: u8 = 0b00000001;
const UNITITIALIZED_STATE_BITS: u8 = 0b00000010;

// serialized adds 8 bytes to store the size of resulting array, so using custom function instead
impl TokenAccount {
    pub fn to_bytes(&self) -> Vec<u8> {
        // save space in serialization, first two bits LSB is reserved for program / 2
        // next two LSBs are reserved for state / 2
        // next bit for has delegate / 1
        // then next for has is_native / 1
        // then next of has close authority / 1
        // and last for has additional data / 1
        let mut program_bits = 0u8;
        if self.program == Program::Token2022Program {
            program_bits |= 0x1; // set lsb to 1
        }
        match self.state {
            TokenProgramAccountState::Uninitialized => {
                program_bits |= UNITITIALIZED_STATE_BITS << 2
            }
            TokenProgramAccountState::Frozen => program_bits |= FROZEN_STATE_BITS << 2,
            TokenProgramAccountState::Initialized => program_bits |= INITIALIZED_STATE_BITS << 2,
        }
        let has_delegate = self.delegate.is_some();
        if has_delegate {
            program_bits |= 0x1 << 4;
        }

        let has_native = self.is_native.is_some();
        if has_native {
            program_bits |= 0x1 << 5;
        }

        let has_close_authority = self.close_authority.is_some();
        if has_close_authority {
            program_bits |= 0x1 << 6;
        }

        let has_additional_data = self.additional_data.is_some();
        if has_additional_data {
            program_bits |= 0x1 << 7;
        }

        let mut bytes = Vec::<u8>::with_capacity(85);
        bytes.push(program_bits);
        bytes.push(if self.is_deleted { 1 } else { 0 });
        bytes.extend_from_slice(&self.pubkey.to_bytes());
        bytes.extend_from_slice(&self.owner.to_bytes());
        bytes.extend_from_slice(&self.mint.to_le_bytes());
        bytes.extend_from_slice(&self.amount.to_le_bytes());
        if has_delegate {
            let (delegate, amount) = self.delegate.unwrap();
            bytes.extend_from_slice(&delegate.to_bytes());
            bytes.extend_from_slice(&amount.to_le_bytes());
        }

        if has_native {
            bytes.extend_from_slice(&self.is_native.unwrap().to_le_bytes());
        }

        if has_close_authority {
            bytes.extend_from_slice(&self.close_authority.unwrap().to_bytes());
        }

        if has_additional_data {
            let additional_data = self.additional_data.as_ref().unwrap();
            let size = additional_data.len() as u32; // accounts are 10MBs max will not extend u32 space
            bytes.extend_from_slice(&size.to_le_bytes());
            bytes.extend_from_slice(additional_data);
        }
        bytes
    }

    pub fn from_bytes(v: &[u8]) -> TokenAccount {
        let header: [u8; 78] = v[0..78].try_into().unwrap();
        let mut others = &v[78..];
        let (bit_flags, is_deleted, pubkey, owner, mint_index, amount) =
            arrayref::array_refs![&header, 1, 1, 32, 32, 4, 8];
        let bit_flags = bit_flags[0];
        let program = match bit_flags & 0x1 {
            0 => Program::TokenProgram,
            1 => Program::Token2022Program,
            _ => unreachable!(),
        };
        let state = match (bit_flags >> 2) & 0x3 {
            INITIALIZED_STATE_BITS => TokenProgramAccountState::Initialized,
            FROZEN_STATE_BITS => TokenProgramAccountState::Frozen,
            UNITITIALIZED_STATE_BITS => TokenProgramAccountState::Uninitialized,
            _ => unreachable!(),
        };
        let has_delegate = (bit_flags >> 4) & 0x1 == 0x1;
        let has_native = (bit_flags >> 5) & 0x1 == 0x1;
        let has_close_authority = (bit_flags >> 6) & 0x1 == 0x1;
        let has_additional_data = (bit_flags >> 7) & 0x1 == 0x1;

        let delegate = if has_delegate {
            let delegate_bytes: &[u8; 32 + 8] = others[..32 + 8].try_into().unwrap();
            others = &others[32 + 8..];
            let delegate_pk = Pubkey::new_from_array(*arrayref::array_ref![delegate_bytes, 0, 32]);
            let amount = u64::from_le_bytes(*arrayref::array_ref![delegate_bytes, 32, 8]);
            Some((delegate_pk, amount))
        } else {
            None
        };

        let is_native = if has_native {
            let native_bytes = *arrayref::array_ref![others, 0, 8];
            others = &others[8..];
            Some(u64::from_le_bytes(native_bytes))
        } else {
            None
        };

        let close_authority = if has_close_authority {
            let native_bytes = *arrayref::array_ref![others, 0, 32];
            others = &others[32..];
            Some(Pubkey::new_from_array(native_bytes))
        } else {
            None
        };

        let additional_data = if has_additional_data {
            let native_bytes = *arrayref::array_ref![others, 0, 4];
            Some(others[4..4 + u32::from_le_bytes(native_bytes) as usize].to_vec())
        } else {
            None
        };

        TokenAccount {
            program,
            is_deleted: match is_deleted[0] {
                0 => false,
                1 => true,
                _ => unreachable!(),
            },
            pubkey: Pubkey::new_from_array(*pubkey),
            owner: Pubkey::new_from_array(*owner),
            mint: u32::from_le_bytes(*mint_index),
            amount: u64::from_le_bytes(*amount),
            state,
            delegate,
            is_native,
            close_authority,
            additional_data,
        }
    }

    pub fn get_pubkey_from_binary(binary: &[u8]) -> Pubkey {
        let pubkey_array: [u8; 32] = binary[2..34].try_into().unwrap();
        Pubkey::new_from_array(pubkey_array)
    }

    pub fn get_owner_from_binary(binary: &[u8]) -> Pubkey {
        let pubkey_array: [u8; 32] = binary[34..66].try_into().unwrap();
        Pubkey::new_from_array(pubkey_array)
    }

    pub fn get_mint_index_from_binary(binary: &[u8]) -> u32 {
        let bytes: [u8; 4] = binary[66..66 + 4].try_into().unwrap();
        u32::from_le_bytes(bytes)
    }
}

#[cfg(test)]
mod test {
    use itertools::Itertools;
    use rand::{thread_rng, Rng};
    use solana_sdk::pubkey::Pubkey;

    use super::{Program, TokenAccount, TokenProgramAccountState};

    #[test]
    pub fn test_account_serialization() {
        let acc = TokenAccount {
            program: super::Program::TokenProgram,
            is_deleted: false,
            pubkey: Pubkey::new_unique(),
            mint: 0,
            owner: Pubkey::new_unique(),
            amount: 10,
            state: TokenProgramAccountState::Initialized,
            delegate: None,
            is_native: None,
            close_authority: None,
            additional_data: None,
        };
        assert_eq!(acc.to_bytes().len(), 78);
    }

    #[test]
    pub fn fuzzy_test_token_account_serialization() {
        tracing_subscriber::fmt::init();
        let mut rng = thread_rng();
        for _ in 0..5_000 {
            let acc = TokenAccount {
                program: if rng.gen::<bool>() {
                    Program::TokenProgram
                } else {
                    Program::Token2022Program
                },
                is_deleted: rng.gen::<bool>(),
                pubkey: Pubkey::new_unique(),
                mint: rng.gen(),
                owner: Pubkey::new_unique(),
                amount: rng.gen(),
                state: TokenProgramAccountState::Initialized,
                delegate: if rng.gen::<bool>() {
                    Some((Pubkey::new_unique(), rng.gen()))
                } else {
                    None
                },
                is_native: if rng.gen::<bool>() {
                    Some(rng.gen())
                } else {
                    None
                },
                close_authority: if rng.gen::<bool>() {
                    Some(Pubkey::new_unique())
                } else {
                    None
                },
                additional_data: if rng.gen_bool(0.1) {
                    let len = rng.gen::<usize>() % 1_000_000;
                    let data = (0..len).map(|_| rng.gen::<u8>()).collect_vec();
                    Some(data)
                } else {
                    None
                },
            };
            let ser = acc.to_bytes();

            assert_eq!(TokenAccount::get_pubkey_from_binary(&ser), acc.pubkey);
            assert_eq!(TokenAccount::get_owner_from_binary(&ser), acc.owner);
            assert_eq!(TokenAccount::get_mint_index_from_binary(&ser), acc.mint);
            let deser = TokenAccount::from_bytes(&ser);
            assert_eq!(deser, acc);
        }
    }
}
