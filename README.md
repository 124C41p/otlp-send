# otlp-send

This is a simple command line tool which reads logs and traces from zstd-compressed protobuf-encoded files written by the [OpenTelemetry File Exporter](https://github.com/open-telemetry/opentelemetry-collector-contrib/tree/main/exporter/fileexporter), and send those to any OTLP-compatible server.

```
Usage: otlp-send <SIGNAL_TYPE> <PROTOCOL> <URL> [FILES]...

Arguments:
  <SIGNAL_TYPE>  [possible values: logs, traces]
  <PROTOCOL>     [possible values: grpc, http]
  <URL>          Address to send to
  [FILES]...     Files to read from

Options:
  -h, --help     Print help
  -V, --version  Print version
```