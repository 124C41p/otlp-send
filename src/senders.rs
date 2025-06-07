use std::sync::Arc;

use anyhow::anyhow;
use either::Either::{Left, Right};
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
use reqwest::{Url, header::CONTENT_TYPE};
use rustls_platform_verifier::BuilderVerifierExt;
use tonic::transport::Channel;

pub struct Sender(SenderTypes);

enum SenderTypes {
    Grpc(GrpcSender),
    Http(HttpSender),
}

impl Sender {
    pub async fn new_grpc(url: &str) -> anyhow::Result<Self> {
        Ok(Self(SenderTypes::Grpc(GrpcSender::new(url).await?)))
    }

    pub fn new_http(url: &str) -> anyhow::Result<Self> {
        Ok(Self(SenderTypes::Http(HttpSender::new(url)?)))
    }

    pub fn send_logs(&self, logs: LogsData) -> impl Future<Output = anyhow::Result<()>> + 'static {
        let future = match &self.0 {
            SenderTypes::Grpc(grpc_sender) => Left(grpc_sender.send_logs(logs)),
            SenderTypes::Http(http_sender) => Right(http_sender.send_logs(logs)),
        };
        async move {
            future.await?;
            Ok(())
        }
    }

    pub fn send_traces(
        &self,
        traces: TracesData,
    ) -> impl Future<Output = anyhow::Result<()>> + 'static {
        let future = match &self.0 {
            SenderTypes::Grpc(grpc_sender) => Left(grpc_sender.send_traces(traces)),
            SenderTypes::Http(http_sender) => Right(http_sender.send_traces(traces)),
        };
        async move {
            future.await?;
            Ok(())
        }
    }
}

struct GrpcSender {
    channel: Channel,
}

impl GrpcSender {
    async fn new(url: &str) -> anyhow::Result<Self> {
        Ok(Self {
            channel: Channel::builder(url.parse()?).connect().await?,
        })
    }

    fn send_traces(
        &self,
        traces: TracesData,
    ) -> impl Future<Output = anyhow::Result<()>> + 'static {
        let request = ExportTraceServiceRequest {
            resource_spans: traces.resource_spans,
        };
        let mut client = TraceServiceClient::new(self.channel.clone());
        async move { handle_traces_response(client.export(request).await?.into_inner()) }
    }

    fn send_logs(&self, logs: LogsData) -> impl Future<Output = anyhow::Result<()>> + 'static {
        let request = ExportLogsServiceRequest {
            resource_logs: logs.resource_logs,
        };
        let mut client = LogsServiceClient::new(self.channel.clone());
        async move { handle_logs_response(client.export(request).await?.into_inner()) }
    }
}

struct HttpSender {
    client: reqwest::Client,
    logs_url: Url,
    traces_url: Url,
}

impl HttpSender {
    fn new(url: &str) -> anyhow::Result<Self> {
        let url = url.trim_end_matches("/");
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
            logs_url: Url::parse(&format!("{url}/v1/logs"))?,
            traces_url: Url::parse(&format!("{url}/v1/traces"))?,
        })
    }

    fn send_logs(&self, logs: LogsData) -> impl Future<Output = anyhow::Result<()>> + 'static {
        let future = self.send_body(
            self.logs_url.clone(),
            (ExportLogsServiceRequest {
                resource_logs: logs.resource_logs,
            })
            .encode_to_vec(),
        );

        async move { handle_logs_response(ExportLogsServiceResponse::decode(future.await?)?) }
    }

    fn send_traces(
        &self,
        traces: TracesData,
    ) -> impl Future<Output = anyhow::Result<()>> + 'static {
        let future = self.send_body(
            self.traces_url.clone(),
            (ExportTraceServiceRequest {
                resource_spans: traces.resource_spans,
            })
            .encode_to_vec(),
        );

        async move { handle_traces_response(ExportTraceServiceResponse::decode(future.await?)?) }
    }

    fn send_body(
        &self,
        url: Url,
        body: impl Into<reqwest::Body>,
    ) -> impl Future<Output = anyhow::Result<Bytes>> + 'static {
        let future = self
            .client
            .post(url)
            .header(CONTENT_TYPE, "application/x-protobuf")
            .body(body)
            .send();

        async move {
            let response = future.await?;
            let status = response.status();
            if !status.is_success() {
                return Err(anyhow!("Server error {status}"));
            }
            Ok(response.bytes().await?)
        }
    }
}

fn handle_logs_response(response: ExportLogsServiceResponse) -> anyhow::Result<()> {
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

fn handle_traces_response(response: ExportTraceServiceResponse) -> anyhow::Result<()> {
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
