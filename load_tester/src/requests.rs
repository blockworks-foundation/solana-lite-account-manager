use anyhow::{Error, Result};
use reqwest::{Client, Response, Url};
use serde_json::json;

pub async fn bench_get_account_info_request(
    client: &Client,
    url: &Url,
    pubkey: &str,
) -> Result<Response> {
    let req_body = json!({
        "jsonrpc": "2.0",
        "id": 1,
        "method": "getAccountInfo",
        "params": [
          pubkey,
          {
            "encoding": "base64"
          }
        ]
    });
    client
        .post(url.clone())
        .json(&req_body)
        .send()
        .await
        .map_err(Error::new)
}
