use anyhow::{bail, Context, Result};
use base64::{prelude::BASE64_STANDARD as base64, Engine};
use log::debug;
use reqwest::{Client, StatusCode, Url};
use serde_json::json;
use solana_account_decoder::{UiAccount, UiAccountData, UiAccountEncoding};
use solana_rpc_client_api::response::Response as RpcResponse;
use spl_token_2022::{solana_program::program_pack::Pack, state::Account};

pub async fn bench_get_account_info_request(
    client: &Client,
    url: &Url,
    account_pk: &str,
) -> Result<(StatusCode, usize)> {
    debug!("getAccountInfo request for account_pk={}", account_pk);

    let req_body: serde_json::Value = json!({
        "jsonrpc": "2.0",
        "id": 1,
        "method": "getAccountInfo",
        "params": [
          account_pk,
          {
            "encoding": "base64"
          }
        ]
    });
    let response = client.post(url.clone()).json(&req_body).send().await?;
    let response_status = response.status();
    let response_bytes = response.bytes().await?;
    let response_length = response_bytes.len();

    if let StatusCode::OK = response_status {
        debug!("response OK for account_pk={}", account_pk);

        let response_json: serde_json::Value = serde_json::from_slice(&response_bytes)
            .with_context(|| {
                format!(
                    "Couldn't parse response bytes to JSON for account_pk={}",
                    account_pk
                )
            })?;
        let result_value = response_json
            .get("result")
            .with_context(|| {
                format!(
                    "Response object property 'result' is nullish for account_pk={}",
                    account_pk
                )
            })?
            .clone();
        let get_account_info_response: RpcResponse<Option<UiAccount>> =
            serde_json::from_value(result_value).with_context(|| {
                format!(
                    "Couldn't parse result value to RpcResponse for account_pk={}",
                    account_pk
                )
            })?;
        let account_data = get_account_info_response
            .value
            .with_context(|| format!("'value' property is nullish for account_pk={}", account_pk))?
            .data;
        match account_data {
            UiAccountData::Binary(data, UiAccountEncoding::Base64) => {
                let decoded_data = base64.decode(data).with_context(|| {
                    format!("Couldn't decode base64 data for pubkey {}", account_pk)
                })?;
                Account::unpack(&decoded_data).with_context(|| {
                    format!(
                        "Couldn't unpack token account data for account_pk={}",
                        account_pk
                    )
                })?;
                debug!(
                    "successfully deserialized account data for account_pk={}",
                    account_pk
                );
            }
            UiAccountData::Binary(_data, encoding) => {
                bail!(
                    "Unexpected binary account data encoding for pubkey: encoding={:?}, account_pk={}",
                    encoding,
                    account_pk
                );
            }
            UiAccountData::Json(_) => {
                bail!(
                    "Unexpected account data encoding for pubkey: encoding=Json, account_pk={}",
                    account_pk
                );
            }
            _ => unreachable!(),
        }
    }
    Ok((response_status, response_length))
}
