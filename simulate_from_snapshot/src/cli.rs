use clap::{Args as ClapArgs, Parser};

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
pub struct Args {
    #[arg(short = 's', long)]
    pub snapshot_archive_path: String,

    #[command(flatten)]
    pub geyser_sources: GeyserSources,

    /// Address to bind the RPC server to
    #[arg(long, default_value = "[::]:10700")]
    pub rpc_bind_addr: String,

    /// Address to bind the Prometheus Exporter endpoint to
    #[arg(long)]
    pub prometheus_bind_addr: Option<String>,
}

#[derive(ClapArgs, Debug)]
#[group(id = "geyser-sources", required = false, multiple = false)]
pub struct GeyserSources {
    /// Address of the QUIC service to connect to
    #[arg(long)]
    pub quic_url: Option<String>,

    /// Address of the GRPC service to connect to. The GRPC token may be provided via the GRPC_X_TOKEN env var.
    #[arg(long)]
    pub grpc_addr: Option<String>,
}
