use std::fs::read_to_string;

use crate::cli::{LoadTestRequestCommand, LoadTesterCli};

#[derive(Debug, Clone)]
pub enum LoadTestEndpointParams {
    GetAccountInfo(Vec<String>),
}

impl LoadTestEndpointParams {
    pub fn get_params_from_cli_args(cli_args: &LoadTesterCli) -> LoadTestEndpointParams {
        match &cli_args.load_test_request {
            LoadTestRequestCommand::GetAccountInfoArgs { pk, input_file } => {
                let pks = match (pk, input_file) {
                    (Some(pk), None) => vec![pk.clone()],
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
                LoadTestEndpointParams::GetAccountInfo(pks)
            }
        }
    }
}
