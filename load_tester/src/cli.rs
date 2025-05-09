use clap::{Parser, Subcommand};
use reqwest::Url;

#[derive(Parser, Clone)]
pub struct LoadTesterCli {
    /// Target RPC server URL
    pub rpc_url: Url,

    #[command(subcommand)]
    pub load_test_request: LoadTestRequestCommand,

    #[command(flatten)]
    pub bench_opts: rlt::cli::BenchCli,
}

#[derive(Debug, Clone, Subcommand)]
pub enum LoadTestRequestCommand {
    /// Run benchmark for the getAccountInfo request
    #[command(name = "get-account-info")]
    #[group(id = "get-account-info", required = true, multiple = false)]
    GetAccountInfoArgs {
        /// The public key of the account
        #[arg(short, long)]
        account_pk: Option<String>,

        /// The file to read the public keys from
        #[arg(short, long)]
        input_file: Option<String>,
    },
}
