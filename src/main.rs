mod cli;
mod iters;
mod senders;

use clap::Parser;
use cli::{Cli, Protocol, SignalType};
use iters::{LogsIter, TracesIter};
use senders::Sender;
use tokio::task::JoinSet;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let cli = Cli::parse_from(wild::args());

    let sender = match cli.protocol {
        Protocol::Grpc => Sender::new_grpc(&cli.url).await?,
        Protocol::Http => Sender::new_http(&cli.url)?,
    };

    let mut tasks = JoinSet::new();

    match cli.signal_type {
        SignalType::Logs => {
            for path in cli.files {
                for logs in LogsIter::new(path)? {
                    tasks.spawn(sender.send_logs(logs?));
                }
            }
        }
        SignalType::Traces => {
            for path in cli.files {
                for traces in TracesIter::new(path)? {
                    tasks.spawn(sender.send_traces(traces?));
                }
            }
        }
    }

    tasks
        .join_all()
        .await
        .into_iter()
        .collect::<anyhow::Result<()>>()?;

    Ok(())
}
