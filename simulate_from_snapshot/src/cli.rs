use clap::Parser;

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
pub struct Args {
    #[arg(short = 's', long)]
    pub snapshot_archive_path: String,

    #[arg(short = 'u', long)]
    pub quic_url: Option<String>,
}
