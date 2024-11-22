use std::sync::{mpsc::Receiver, Arc};

use serde::{Deserialize, Serialize};
use solana_rpc_client_api::filter::RpcFilterType;
use solana_sdk::{
    account::Account as SolanaAccount, commitment_config::CommitmentConfig, pubkey::Pubkey,
    slot_history::Slot,
};
// 64 MB
const MAX_ACCOUNT_SIZE: usize = 64 * 1024 * 1024;

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub enum CompressionMethod {
    None,
    Lz4(i32),
    Zstd(i32),
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub enum Data {
    Uncompressed(Vec<u8>),
    Lz4 { binary: Vec<u8>, len: usize },
    Zstd { binary: Vec<u8>, len: usize },
}

impl Data {
    pub fn new(data: &[u8], compression_method: CompressionMethod) -> Self {
        match compression_method {
            CompressionMethod::None => Data::Uncompressed(data.to_vec()),
            CompressionMethod::Lz4(level) => {
                let len = data.len();
                let binary = lz4::block::compress(
                    data,
                    Some(lz4::block::CompressionMode::FAST(level)),
                    true,
                )
                .unwrap();
                Data::Lz4 { binary, len }
            }
            CompressionMethod::Zstd(level) => {
                let len = data.len();
                let binary = zstd::bulk::compress(data, level).unwrap();
                Data::Zstd { binary, len }
            }
        }
    }

    pub fn len(&self) -> usize {
        match self {
            Data::Uncompressed(d) => d.len(),
            Data::Lz4 { len, .. } => *len,
            Data::Zstd { len, .. } => *len,
        }
    }

    pub fn is_empty(&self) -> bool {
        match self {
            Data::Uncompressed(d) => d.is_empty(),
            Data::Lz4 { len, .. } => *len == 0,
            Data::Zstd { len, .. } => *len == 0,
        }
    }

    pub fn data(&self) -> Vec<u8> {
        match self {
            Data::Uncompressed(d) => d.clone(),
            Data::Lz4 { binary, len } => match lz4::block::decompress(binary, None) {
                Ok(res) => res,
                Err(e) => {
                    log::error!(
                        "error {e:?} decompressing data of size {}, data_size_set: {}",
                        binary.len(),
                        len
                    );
                    panic!()
                }
            },
            Data::Zstd { binary, .. } => zstd::bulk::decompress(binary, MAX_ACCOUNT_SIZE).unwrap(),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct Account {
    /// lamports in the account
    pub lamports: u64,
    /// data held in this account
    pub data: Data,
    /// the program that owns this account. If executable, the program that loads this account.
    pub owner: Pubkey,
    /// this account's data contains a loaded program (and is now read-only)
    pub executable: bool,
    /// the epoch at which this account will next owe rent
    pub rent_epoch: u64,
}

impl Account {
    pub fn from_solana_account(
        account: SolanaAccount,
        compression_method: CompressionMethod,
    ) -> Self {
        Self {
            lamports: account.lamports,
            data: Data::new(&account.data, compression_method),
            owner: account.owner,
            executable: account.executable,
            rent_epoch: account.rent_epoch,
        }
    }

    pub fn to_solana_account(&self) -> SolanaAccount {
        SolanaAccount {
            lamports: self.lamports,
            data: self.data.data(),
            owner: self.owner,
            executable: self.executable,
            rent_epoch: self.rent_epoch,
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct AccountData {
    pub pubkey: Pubkey,
    pub account: Arc<Account>,
    pub updated_slot: Slot,
    pub write_version: u64,
}

impl AccountData {
    pub fn allows(&self, filter: &RpcFilterType) -> bool {
        match filter {
            RpcFilterType::DataSize(size) => self.account.data.len() as u64 == *size,
            RpcFilterType::Memcmp(compare) => compare.bytes_match(&self.account.data.data()),
            RpcFilterType::TokenAccountState => {
                // todo
                false
            }
        }
    }
}

impl PartialEq for AccountData {
    fn eq(&self, other: &Self) -> bool {
        self.pubkey == other.pubkey
            && *self.account == *other.account
            && self.updated_slot == other.updated_slot
    }
}

#[derive(Clone, Debug)]
pub struct AccountNotificationMessage {
    pub data: AccountData,
    pub commitment: CommitmentConfig,
}

pub type AccountStream = Receiver<AccountNotificationMessage>;
