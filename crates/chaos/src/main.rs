mod commands;

use clap::{Parser, Subcommand};

#[derive(Parser)]
#[command(name = "chaos", about = "Chaos-testing CLI for a running Quorum cluster")]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Kill a node's OS process outright (requires its PID)
    Kill {
        #[arg(long)]
        pid: u32,
    },
    /// Block a node's Raft TCP port, simulating a network partition
    Partition {
        #[arg(long)]
        node: u32,
    },
    /// Remove a previously-added partition, restoring connectivity
    Heal {
        #[arg(long)]
        node: u32,
    },
    /// Print role/term/commit_index/log_len for every known node
    Status,
}

fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();
    match cli.command {
        Commands::Kill { pid } => commands::kill_node(pid),
        Commands::Partition { node } => commands::partition_node(node),
        Commands::Heal { node } => commands::heal_node(node),
        Commands::Status => commands::status(),
    }
}