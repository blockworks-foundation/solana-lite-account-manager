use std::fs::read_to_string;

use crate::cli::{LoadTestRequestCommand, LoadTesterCli};

#[derive(Debug, Clone)]
pub enum LoadTestEndpointParams {
    GetAccountInfo(Vec<String>),
}

impl LoadTestEndpointParams {
    pub fn get_params_from_cli_args(cli_args: &LoadTesterCli) -> LoadTestEndpointParams {
        match &cli_args.load_test_request {
            LoadTestRequestCommand::GetAccountInfoArgs {
                account_pk,
                input_file,
            } => {
                let account_pks = match (account_pk, input_file) {
                    (Some(account_pk), None) => vec![account_pk.clone()],
                    (None, Some(input_file)) => read_to_string(input_file)
                        .unwrap_or_else(|e| {
                            panic!("failed to read file: file={}, error={}", input_file, e)
                        })
                        .trim()
                        .lines()
                        .map(|s| s.to_string())
                        .collect(),
                    _ => unreachable!(),
                };
                LoadTestEndpointParams::GetAccountInfo(account_pks)
            }
        }
    }
}
