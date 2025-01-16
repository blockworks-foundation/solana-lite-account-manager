use std::collections::HashMap;
use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;

use base64::Engine;
use itertools::Itertools;
use jsonrpsee::server::ServerBuilder;
use jsonrpsee::{core::RpcResult, proc_macros::rpc};
use lite_account_manager_common::account_filter::AccountFilterType as AmAccountFilterType;
use lite_account_manager_common::account_store_interface::{
    AccountLoadingError, AccountStorageInterface, Mint, ProgramAccountStorageInterface,
    TokenProgramAccountData, TokenProgramType,
};
use lite_account_manager_common::{account_data::AccountData, commitment::Commitment};
use solana_account_decoder::parse_account_data::{AccountAdditionalDataV2, SplTokenAdditionalData};
use solana_account_decoder::{UiAccount, UiAccountEncoding};
use solana_rpc_client_api::client_error::reqwest::Method;
use solana_rpc_client_api::{
    config::{RpcAccountInfoConfig, RpcProgramAccountsConfig},
    response::{OptionalContext, Response as RpcResponse, RpcKeyedAccount, RpcResponseContext},
};
use solana_sdk::{commitment_config::CommitmentConfig, pubkey::Pubkey};
use tokio::task::JoinHandle;
use tower_http::cors::{Any, CorsLayer};

#[rpc(server)]
pub trait TestRpc {
    #[method(name = "getProgramAccounts")]
    async fn get_program_accounts(
        &self,
        program_id_str: String,
        config: Option<RpcProgramAccountsConfig>,
    ) -> RpcResult<OptionalContext<Vec<RpcKeyedAccount>>>;

    #[method(name = "getSnapshot")]
    async fn get_snapshot(&self, program_id_str: String) -> RpcResult<String>;

    #[method(name = "getAccountInfo")]
    async fn get_account_info(
        &self,
        pubkey_str: String,
        config: Option<RpcAccountInfoConfig>,
    ) -> RpcResult<RpcResponse<Option<UiAccount>>>;
}

pub struct RpcServerImpl {
    storage: Arc<dyn AccountStorageInterface>,
    program_account_storage: Option<Arc<dyn ProgramAccountStorageInterface>>,
}

impl RpcServerImpl {
    pub fn new(
        storage: Arc<dyn AccountStorageInterface>,
        program_account_storage: Option<Arc<dyn ProgramAccountStorageInterface>>,
    ) -> Self {
        Self {
            storage,
            program_account_storage,
        }
    }

    pub async fn start_serving(
        rpc_impl: RpcServerImpl,
        port: u16,
    ) -> anyhow::Result<JoinHandle<()>> {
        let http_addr = format!("[::]:{port}");
        let cors = CorsLayer::new()
            .max_age(Duration::from_secs(86400))
            // Allow `POST` when accessing the resource
            .allow_methods([Method::POST, Method::GET, Method::OPTIONS])
            // Allow requests from any origin
            .allow_origin(Any)
            .allow_headers(Any);

        let middleware = tower::ServiceBuilder::new().layer(cors);

        let http_server_handle = ServerBuilder::default()
            .set_middleware(middleware)
            .max_connections(10)
            .max_request_body_size(1024 * 1024) // 16 MB
            .max_response_body_size(512 * 1024 * 1024) // 512 MBs
            .http_only()
            .build(http_addr.clone())
            .await?
            .start(rpc_impl.into_rpc());

        let jh = tokio::spawn(async move {
            log::info!("HTTP Server started at {http_addr:?}");
            http_server_handle.stopped().await;
            log::error!("QUIC GEYSER PLUGIN HTTP SERVER STOPPED");
        });
        Ok(jh)
    }
}

#[jsonrpsee::core::async_trait]
impl TestRpcServer for RpcServerImpl {
    async fn get_program_accounts(
        &self,
        program_id_str: String,
        config: Option<RpcProgramAccountsConfig>,
    ) -> RpcResult<OptionalContext<Vec<RpcKeyedAccount>>> {
        let program_type: TokenProgramType = Pubkey::from_str(&program_id_str)
            .map_err(|_| ())
            .and_then(|pubkey| TokenProgramType::try_from(&pubkey))
            .map_err(|_| jsonrpsee::types::error::ErrorCode::InvalidParams)?;

        let with_context = config
            .as_ref()
            .map(|value| value.with_context.unwrap_or_default())
            .unwrap_or_default();

        let commitment: CommitmentConfig = config
            .as_ref()
            .and_then(|x| x.account_config.commitment)
            .unwrap_or_default();

        let account_filters = config
            .as_ref()
            .and_then(|x| x.filters.as_ref())
            .map(|filters| filters.iter().map(AmAccountFilterType::from).collect_vec());
        let commitment = Commitment::from(commitment);

        let (program_accounts, token_account_mints) = self
            .program_account_storage
            .as_ref()
            .map(|storage| {
                storage.get_program_accounts_with_mints(
                    program_type.clone(),
                    account_filters.clone(),
                    commitment,
                )
            })
            .unwrap_or_else(|| {
                self.storage
                    .get_program_accounts(*program_type.as_ref(), account_filters, commitment)
                    .map(|accounts| {
                        (
                            accounts
                                .into_iter()
                                .map(TokenProgramAccountData::OtherAccount)
                                .collect_vec(),
                            HashMap::with_capacity(0),
                        )
                    })
            })
            .map_err(|e| {
                log::error!("get_program_accounts: {}", e);
                account_loading_error_to_jsonrpsee_error(e)
            })?;
        log::debug!(
            "get_program_accounts: found {} accounts",
            program_accounts.len()
        );
        let min_context_slot = config
            .as_ref()
            .and_then(|c| {
                if c.with_context.unwrap_or_default() {
                    c.account_config.min_context_slot
                } else {
                    None
                }
            })
            .unwrap_or_default();

        let slot = program_accounts
            .iter()
            .map(|program_account| program_account.updated_slot)
            .max()
            .unwrap_or_default();

        let request_account_config = config.map(|c| c.account_config);

        let rpc_keyed_accounts = program_accounts
            .iter()
            .filter(|account_data| account_data.updated_slot >= min_context_slot)
            .map(|account_data| RpcKeyedAccount {
                pubkey: account_data.pubkey.to_string(),
                account: convert_account_data_to_ui_account(
                    account_data,
                    request_account_config.clone(),
                    convert_token_program_account_data_to_additional_data(
                        account_data,
                        &token_account_mints,
                    ),
                ),
            })
            .collect_vec();

        if with_context {
            Ok(OptionalContext::Context(RpcResponse {
                context: RpcResponseContext {
                    slot,
                    api_version: None,
                },
                value: rpc_keyed_accounts,
            }))
        } else {
            Ok(OptionalContext::NoContext(rpc_keyed_accounts))
        }
    }

    async fn get_snapshot(&self, program_id_str: String) -> RpcResult<String> {
        let program_id = Pubkey::from_str(program_id_str.as_str())
            .map_err(|_| jsonrpsee::types::error::ErrorCode::InvalidParams)?;
        let res = self
            .storage
            .create_snapshot(program_id)
            .map_err(|_| jsonrpsee::types::error::ErrorCode::InternalError)?;
        Ok(base64::engine::general_purpose::STANDARD.encode(res))
    }

    async fn get_account_info(
        &self,
        pubkey_str: String,
        config: Option<RpcAccountInfoConfig>,
    ) -> RpcResult<RpcResponse<Option<UiAccount>>> {
        let account_pk = Pubkey::from_str(pubkey_str.as_str())
            .map_err(|_| jsonrpsee::types::error::ErrorCode::InvalidParams)?;
        let commitment = config
            .clone()
            .and_then(|x| x.commitment)
            .unwrap_or_default();

        let get_account_result = self
            .program_account_storage
            .as_ref()
            .map(|storage| storage.get_account_with_mint(account_pk, Commitment::from(commitment)))
            .unwrap_or_else(|| {
                self.storage
                    .get_account(account_pk, Commitment::from(commitment))
                    .map(|account| {
                        account.map(|account_data| {
                            (TokenProgramAccountData::OtherAccount(account_data), None)
                        })
                    })
            })
            .map_err(|e| {
                log::error!("get_account_info: {}", e);
                account_loading_error_to_jsonrpsee_error(e)
            })?;

        Ok(get_account_result
            .map(|(account_data, token_account_mint)| RpcResponse {
                context: RpcResponseContext {
                    slot: account_data.updated_slot,
                    api_version: None,
                },
                value: Some(convert_account_data_to_ui_account(
                    &account_data,
                    config,
                    convert_mint_to_additional_data(token_account_mint.as_ref()),
                )),
            })
            .ok_or_else(|| RpcResponse {
                context: RpcResponseContext {
                    slot: 0,
                    api_version: None,
                },
                value: None,
            })
            .unwrap_or_else(|res| res))
    }
}

fn convert_account_data_to_ui_account(
    account_data: &AccountData,
    config: Option<RpcAccountInfoConfig>,
    additional_data: Option<AccountAdditionalDataV2>,
) -> UiAccount {
    let encoding = config
        .as_ref()
        .and_then(|cfg| cfg.encoding)
        .unwrap_or(UiAccountEncoding::Base64);
    let data_slice = config.as_ref().map(|c| c.data_slice).unwrap_or_default();
    UiAccount::encode(
        &account_data.pubkey,
        &account_data.account.to_solana_account(),
        encoding,
        additional_data,
        data_slice,
    )
}

fn convert_token_program_account_data_to_additional_data(
    token_program_account_data: &TokenProgramAccountData,
    token_account_mints: &HashMap<Pubkey, Mint>,
) -> Option<AccountAdditionalDataV2> {
    if let TokenProgramAccountData::TokenAccount(token_account) = token_program_account_data {
        convert_mint_to_additional_data(token_account_mints.get(&token_account.mint_pubkey))
    } else {
        None
    }
}

fn convert_mint_to_additional_data(
    token_account_mint: Option<&Mint>,
) -> Option<AccountAdditionalDataV2> {
    token_account_mint.as_ref().map(|mint| {
        let decimals = match mint {
            Mint::TokenMint(mint) => mint.decimals,
            Mint::Token2022Mint(mint) => mint.decimals,
        };
        AccountAdditionalDataV2 {
            spl_token_additional_data: Some(SplTokenAdditionalData {
                decimals,
                ..Default::default()
            }),
        }
    })
}

fn account_loading_error_to_jsonrpsee_error(
    e: AccountLoadingError,
) -> jsonrpsee::types::error::ErrorCode {
    match e {
        AccountLoadingError::AccountNotFound => jsonrpsee::types::error::ErrorCode::InvalidParams,
        AccountLoadingError::ShouldContainAnAccountFilter => {
            jsonrpsee::types::error::ErrorCode::InvalidParams
        }
        AccountLoadingError::WrongIndex => jsonrpsee::types::error::ErrorCode::InvalidParams,
        AccountLoadingError::TokenAccountsCannotUseThisFilter => {
            jsonrpsee::types::error::ErrorCode::InvalidParams
        }
        _ => jsonrpsee::types::error::ErrorCode::InternalError,
    }
}
