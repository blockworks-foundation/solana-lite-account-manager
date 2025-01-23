use anyhow::Result;
use async_trait::async_trait;
use clap::Parser;
use cli::LoadTesterCli;
use load_test_endpoint_params::LoadTestEndpointParams;
use rand::{rngs::StdRng, Rng, SeedableRng};
use requests::bench_get_account_info_request;
use reqwest::Client;
use rlt::{BenchSuite, IterInfo, IterReport};
use tokio::time::Instant;

mod cli;
mod load_test_endpoint_params;
mod requests;

#[derive(Clone)]
pub struct LoadTester {
    cli_opts: LoadTesterCli,
    load_test_endpoint_params: LoadTestEndpointParams,
}

impl LoadTester {
    fn new(cli_opts: LoadTesterCli, load_test_endpoint_params: LoadTestEndpointParams) -> Self {
        Self {
            cli_opts,
            load_test_endpoint_params,
        }
    }
}

pub struct State {
    client: Client,
    prng: StdRng,
}

impl State {
    fn new() -> Self {
        Self {
            client: Client::new(),
            prng: StdRng::from_entropy(),
        }
    }
}

#[async_trait]
impl BenchSuite for LoadTester {
    type WorkerState = State;

    async fn state(&self, _: u32) -> Result<Self::WorkerState> {
        Ok(State::new())
    }

    async fn bench(&mut self, state: &mut Self::WorkerState, _: &IterInfo) -> Result<IterReport> {
        let started_at = Instant::now();

        let (response_status, response_length) = match &self.load_test_endpoint_params {
            LoadTestEndpointParams::GetAccountInfo(account_pks) => {
                let index = state.prng.gen::<usize>() % account_pks.len();
                let account_pk = account_pks[index].as_str();

                bench_get_account_info_request(&state.client, &self.cli_opts.rpc_url, account_pk)
                    .await?
            }
        };

        let duration = started_at.elapsed();
        Ok(IterReport {
            duration,
            status: response_status.into(),
            bytes: response_length as u64,
            items: 1,
        })
    }
}

#[tokio::main(flavor = "multi_thread")]
async fn main() -> Result<()> {
    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("info")).init();

    let cli_opts = LoadTesterCli::parse();
    let load_test_endpoint_params = LoadTestEndpointParams::get_params_from_cli_args(&cli_opts);
    rlt::cli::run(
        cli_opts.bench_opts,
        LoadTester::new(cli_opts, load_test_endpoint_params),
    )
    .await
}
