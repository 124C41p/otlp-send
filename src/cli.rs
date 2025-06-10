use std::path::PathBuf;

use clap::{Parser, ValueEnum};

/// This tool reads logs and traces from zstd-compressed protobuf-encoded files
/// written by the OpenTelemetry File Exporter, and sends those to any OTLP-compatible server.
#[derive(Parser)]
#[command(version)]
pub struct Cli {
    #[arg(value_enum)]
    pub signal_type: SignalType,

    #[arg(value_enum)]
    pub protocol: Protocol,

    /// Address to send to
    pub url: String,

    /// Files to read from
    pub files: Vec<PathBuf>,
}

#[derive(Clone, ValueEnum)]
pub enum SignalType {
    Logs,
    Traces,
}

#[derive(Clone, ValueEnum)]
pub enum Protocol {
    Grpc,
    Http,
}
