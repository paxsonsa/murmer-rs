//! `murmer` — node identity and zero-trust allowlist management CLI.
//!
//! This tool operates purely on files; it does not talk to a running node. A
//! node watches its allowlist file and hot-reloads changes within ~1s, so:
//!
//! ```text
//! murmer id --key node.key                  # print this node's endpoint id
//! murmer allow add <endpoint-id> --file allow.txt
//! murmer allow rm  <endpoint-id> --file allow.txt
//! murmer allow list --file allow.txt
//! ```
//!
//! To add a node X to a running cluster: run `murmer id` on X to get its key,
//! then `murmer allow add <X-key>` against each existing node's allowlist file
//! (or a shared file). No restarts needed.

use std::path::PathBuf;
use std::process::ExitCode;

use clap::{Parser, Subcommand};
use iroh::EndpointId;
use murmer::cluster::{allowlist, identity_key};

const DEFAULT_KEY_PATH: &str = "murmer-node.key";
const DEFAULT_ALLOWLIST_PATH: &str = "murmer-allowlist.txt";

#[derive(Parser)]
#[command(name = "murmer", about = "murmer node identity & allowlist management")]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Print this node's endpoint id (generating the key file if absent).
    Id {
        /// Path to the node secret key file.
        #[arg(long, default_value = DEFAULT_KEY_PATH)]
        key: PathBuf,
    },
    /// Manage the zero-trust allowlist file.
    Allow {
        #[command(subcommand)]
        action: AllowAction,
    },
}

#[derive(Subcommand)]
enum AllowAction {
    /// Add an endpoint id to the allowlist.
    Add {
        endpoint_id: String,
        #[arg(long, default_value = DEFAULT_ALLOWLIST_PATH)]
        file: PathBuf,
    },
    /// Remove an endpoint id from the allowlist.
    Rm {
        endpoint_id: String,
        #[arg(long, default_value = DEFAULT_ALLOWLIST_PATH)]
        file: PathBuf,
    },
    /// List the endpoint ids in the allowlist.
    List {
        #[arg(long, default_value = DEFAULT_ALLOWLIST_PATH)]
        file: PathBuf,
    },
}

fn main() -> ExitCode {
    let cli = Cli::parse();
    match run(cli) {
        Ok(()) => ExitCode::SUCCESS,
        Err(e) => {
            eprintln!("error: {e}");
            ExitCode::FAILURE
        }
    }
}

fn run(cli: Cli) -> Result<(), String> {
    match cli.command {
        Command::Id { key } => {
            let secret = identity_key::load_or_generate(&key).map_err(|e| e.to_string())?;
            println!("{}", secret.public());
            Ok(())
        }
        Command::Allow { action } => match action {
            AllowAction::Add { endpoint_id, file } => {
                let id = parse_id(&endpoint_id)?;
                if allowlist::add_to_file(&file, id).map_err(|e| e.to_string())? {
                    println!("added {id} to {}", file.display());
                } else {
                    println!("{id} already present in {}", file.display());
                }
                Ok(())
            }
            AllowAction::Rm { endpoint_id, file } => {
                let id = parse_id(&endpoint_id)?;
                if allowlist::remove_from_file(&file, &id).map_err(|e| e.to_string())? {
                    println!("removed {id} from {}", file.display());
                } else {
                    println!("{id} was not present in {}", file.display());
                }
                Ok(())
            }
            AllowAction::List { file } => {
                let mut ids: Vec<String> = allowlist::load_file(&file)
                    .map_err(|e| e.to_string())?
                    .iter()
                    .map(|id| id.to_string())
                    .collect();
                ids.sort();
                if ids.is_empty() {
                    eprintln!("(allowlist {} is empty)", file.display());
                }
                for id in ids {
                    println!("{id}");
                }
                Ok(())
            }
        },
    }
}

fn parse_id(s: &str) -> Result<EndpointId, String> {
    s.parse::<EndpointId>()
        .map_err(|e| format!("invalid endpoint id {s:?}: {e}"))
}
