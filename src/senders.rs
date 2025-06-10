use std::sync::Arc;

use anyhow::{Result, anyhow};
use opentelemetry_proto::tonic::{
    collector::{
        logs::v1::{
            ExportLogsServiceRequest, ExportLogsServiceResponse,
            logs_service_client::LogsServiceClient,
        },
        trace::v1::{
            ExportTraceServiceRequest, ExportTraceServiceResponse,
            trace_service_client::TraceServiceClient,
        },
    },
    logs::v1::LogsData,
    trace::v1::TracesData,
};
use prost::{Message, bytes::Bytes};
use reqwest::{IntoUrl, header::CONTENT_TYPE};
use rustls_platform_verifier::BuilderVerifierExt;
use tonic::transport::Channel;

#[derive(Clone)]
pub struct Sender(SenderInner);

#[derive(Clone)]
enum SenderInner {
    Grpc(GrpcSender),
    Http(HttpSender),
}

impl Sender {
    pub async fn new_grpc(url: &str) -> Result<Self> {
        Ok(Self(SenderInner::Grpc(GrpcSender::new(url).await?)))
    }

    pub fn new_http(url: &str) -> Result<Self> {
        Ok(Self(SenderInner::Http(HttpSender::new(url)?)))
    }

    pub async fn send_logs(&self, logs: LogsData) -> Result<()> {
        match &self.0 {
            SenderInner::Grpc(grpc_sender) => grpc_sender.send_logs(logs).await,
            SenderInner::Http(http_sender) => http_sender.send_logs(logs).await,
        }
    }

    pub async fn send_traces(&self, traces: TracesData) -> Result<()> {
        match &self.0 {
            SenderInner::Grpc(grpc_sender) => grpc_sender.send_traces(traces).await,
            SenderInner::Http(http_sender) => http_sender.send_traces(traces).await,
        }
    }
}

#[derive(Clone)]
struct GrpcSender {
    channel: Channel,
}

impl GrpcSender {
    async fn new(url: &str) -> Result<Self> {
        Ok(Self {
            channel: Channel::builder(url.parse()?).connect().await?,
        })
    }

    async fn send_traces(&self, traces: TracesData) -> Result<()> {
        let request = ExportTraceServiceRequest {
            resource_spans: traces.resource_spans,
        };
        let mut client = TraceServiceClient::new(self.channel.clone());
        handle_traces_response(client.export(request).await?.into_inner())
    }

    async fn send_logs(&self, logs: LogsData) -> Result<()> {
        let request = ExportLogsServiceRequest {
            resource_logs: logs.resource_logs,
        };
        let mut client = LogsServiceClient::new(self.channel.clone());
        handle_logs_response(client.export(request).await?.into_inner())
    }
}

#[derive(Clone)]
struct HttpSender {
    client: reqwest::Client,
    url: Arc<str>,
}

impl HttpSender {
    fn new(url: &str) -> Result<Self> {
        Ok(Self {
            client: reqwest::Client::builder()
                .use_preconfigured_tls(
                    rustls::ClientConfig::builder_with_provider(Arc::new(
                        rustls::crypto::ring::default_provider(),
                    ))
                    .with_safe_default_protocol_versions()?
                    .with_platform_verifier()?
                    .with_no_client_auth(),
                )
                .build()?,
            url: url.trim_end_matches("/").into(),
        })
    }

    async fn send_logs(&self, logs: LogsData) -> Result<()> {
        let response = self
            .send_body(
                format!("{}/v1/logs", self.url),
                (ExportLogsServiceRequest {
                    resource_logs: logs.resource_logs,
                })
                .encode_to_vec(),
            )
            .await?;

        handle_logs_response(ExportLogsServiceResponse::decode(response)?)
    }

    async fn send_traces(&self, traces: TracesData) -> Result<()> {
        let response = self
            .send_body(
                format!("{}/v1/traces", self.url),
                (ExportTraceServiceRequest {
                    resource_spans: traces.resource_spans,
                })
                .encode_to_vec(),
            )
            .await?;

        handle_traces_response(ExportTraceServiceResponse::decode(response)?)
    }

    async fn send_body(&self, url: impl IntoUrl, body: impl Into<reqwest::Body>) -> Result<Bytes> {
        let response = self
            .client
            .post(url)
            .header(CONTENT_TYPE, "application/x-protobuf")
            .body(body)
            .send()
            .await?;

        let status = response.status();
        if !status.is_success() {
            return Err(anyhow!("Server error {status}"));
        }
        Ok(response.bytes().await?)
    }
}

fn handle_logs_response(response: ExportLogsServiceResponse) -> Result<()> {
    if let Some(result) = response.partial_success {
        if result.rejected_log_records > 0 {
            return Err(anyhow!(
                "{} logs were rejected. Reason: {}",
                result.rejected_log_records,
                result.error_message
            ));
        }
    }
    Ok(())
}

fn handle_traces_response(response: ExportTraceServiceResponse) -> Result<()> {
    if let Some(result) = response.partial_success {
        if result.rejected_spans > 0 {
            return Err(anyhow!(
                "{} spans were rejected. Reason: {}",
                result.rejected_spans,
                result.error_message
            ));
        }
    }
    Ok(())
}
