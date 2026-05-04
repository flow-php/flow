# Telemetry OTLP Bridge

The OTLP (OpenTelemetry Protocol) Bridge provides serializers and transports for sending telemetry data to
OpenTelemetry-compatible backends.
It extends the [Flow Telemetry](/documentation/components/libs/telemetry.md) library with production-ready export
capabilities.

- [⬅️️ Back](/documentation/introduction.md)
- [📦Packagist](https://packagist.org/packages/flow-php/telemetry-otlp-bridge)
- [➡️ Installation](/documentation/installation/packages/telemetry-otlp-bridge.md)
- [🐙GitHub](https://github.com/flow-php/telemetry-otlp-bridge)
- [📚API Reference](/documentation/api/bridge/telemetry/otlp)
- [🗺DSL](/documentation/api/bridge/telemetry/otlp/namespaces/flow-bridge-telemetry-otlp-dsl.html)

[TOC]

## Installation

For detailed installation instructions, see the [installation page](/documentation/installation/packages/telemetry-otlp-bridge.md).

### Transports

| Transport | Required Extension | Supported Serializers |
|-----------|--------------------|-----------------------|
| **Curl**  | `ext-curl`         | JSON, Protobuf        |
| **HTTP**  | -                  | JSON, Protobuf        |
| **gRPC**  | `ext-grpc`         | Protobuf              |

### Serializers

| Serializer   | Required Packages  | Supported Transports |
|--------------|--------------------|----------------------|
| **JSON**     | -                  | Curl, HTTP           |
| **Protobuf** | `google/protobuf`  | Curl, HTTP, gRPC     |

To install Protobuf dependencies:

```
composer require google/protobuf
```

## Transports

The bridge provides three transport options for sending telemetry data to OTLP endpoints.

| Transport | Protocol     | Use Case                         | Requirements  |
|-----------|--------------|----------------------------------|---------------|
| **Curl**  | HTTP (async) | Production, low latency          | ext-curl      |
| **HTTP**  | HTTP (sync)  | Standard PSR-18 integration      | PSR-18 client |
| **gRPC**  | gRPC         | High-performance binary protocol | ext-grpc      |

### Curl Transport (Recommended)

The Curl transport uses `curl_multi` for non-blocking I/O, making it ideal for production environments where you want to
minimize latency impact on your application.

```php
<?php

use function Flow\Bridge\Telemetry\OTLP\DSL\{
    otlp_curl_transport,
    otlp_curl_options,
    otlp_json_serializer,
};

$transport = otlp_curl_transport(
    endpoint: 'http://localhost:4318',
    serializer: otlp_json_serializer(),
);

// With custom options
$transport = otlp_curl_transport(
    endpoint: 'https://otlp.example.com:4318',
    serializer: otlp_json_serializer(),
    options: otlp_curl_options()
        ->withTimeout(60)
        ->withConnectTimeout(15)
        ->withHeader('Authorization', 'Bearer your-token')
        ->withCompression(),
);
```

### HTTP Transport (PSR-18)

The HTTP transport uses any PSR-18 compatible HTTP client for synchronous requests. This is useful when you want to
integrate with an existing HTTP client in your application.

```php
<?php

use function Flow\Bridge\Telemetry\OTLP\DSL\{
    otlp_http_transport,
    otlp_json_serializer,
};

// Using any PSR-18 client (e.g., Symfony HttpClient, Guzzle)
$transport = otlp_http_transport(
    client: $httpClient,
    requestFactory: $psr17Factory,
    streamFactory: $psr17Factory,
    endpoint: 'http://localhost:4318',
    serializer: otlp_json_serializer(),
);

// With authentication headers
$transport = otlp_http_transport(
    client: $httpClient,
    requestFactory: $psr17Factory,
    streamFactory: $psr17Factory,
    endpoint: 'https://otlp.example.com:4318',
    serializer: otlp_json_serializer(),
    headers: [
        'Authorization' => 'Bearer your-token',
    ],
);
```

### gRPC Transport

The gRPC transport uses the native gRPC protocol for high-performance binary communication. It requires the `ext-grpc`
PHP extension.

```php
<?php

use function Flow\Bridge\Telemetry\OTLP\DSL\{
    otlp_grpc_transport,
    otlp_protobuf_serializer,
};

$transport = otlp_grpc_transport(
    endpoint: 'localhost:4317',
    serializer: otlp_protobuf_serializer(),
);

// With authentication
$transport = otlp_grpc_transport(
    endpoint: 'otlp.example.com:4317',
    serializer: otlp_protobuf_serializer(),
    headers: [
        'Authorization' => 'Bearer your-token',
    ],
    insecure: false, // Use TLS
);
```

## Serializers

The bridge provides two serialization formats for OTLP data.

| Serializer   | Format | Size    | Readability    | Dependencies      |
|--------------|--------|---------|----------------|-------------------|
| **JSON**     | Text   | Larger  | Human-readable | None              |
| **Protobuf** | Binary | Smaller | Not readable   | `google/protobuf` |

### JSON Serializer

The default serializer that produces human-readable JSON. Best for development and debugging.

```php
<?php

use function Flow\Bridge\Telemetry\OTLP\DSL\otlp_json_serializer;

$serializer = otlp_json_serializer();
```

### Protobuf Serializer

Binary serialization for smaller payloads and better performance. Recommended for production.

```php
<?php

use function Flow\Bridge\Telemetry\OTLP\DSL\otlp_protobuf_serializer;

$serializer = otlp_protobuf_serializer();
```

## Complete Setup

Here's a complete example showing how to set up telemetry with OTLP export.

### Production Setup with Curl Transport

```php
<?php

use function Flow\Telemetry\DSL\{
    telemetry,
    resource,
    tracer_provider,
    meter_provider,
    logger_provider,
    batching_span_processor,
    batching_metric_processor,
    batching_log_processor,
};
use function Flow\Bridge\Telemetry\OTLP\DSL\{
    otlp_curl_transport,
    otlp_curl_options,
    otlp_json_serializer,
    otlp_span_exporter,
    otlp_metric_exporter,
    otlp_log_exporter,
};

$resource = resource([
    'service.name' => 'order-service',
    'service.version' => '1.2.0',
    'deployment.environment' => 'production',
]);

$options = otlp_curl_options()
    ->withTimeout(30)
    ->withCompression();

$transport = otlp_curl_transport(
    endpoint: 'http://otel-collector:4318',
    serializer: otlp_json_serializer(),
    options: $options,
);

$telemetry = telemetry(
    $resource,
    tracer_provider(batching_span_processor(otlp_span_exporter($transport))),
    meter_provider(batching_metric_processor(otlp_metric_exporter($transport))),
    logger_provider(batching_log_processor(otlp_log_exporter($transport))),
);

// Register shutdown handler for graceful termination
$telemetry->registerShutdownFunction();

// Use telemetry
$tracer = $telemetry->tracer('http-handler');
$span = $tracer->span('handle-request');
$span->setAttribute('http.method', 'POST');
$span->setAttribute('http.url', '/api/orders');
$tracer->complete($span);
```

### Setup with Grafana Cloud

```php
<?php

use function Flow\Telemetry\DSL\{
    telemetry,
    resource,
    tracer_provider,
    batching_span_processor,
};
use function Flow\Bridge\Telemetry\OTLP\DSL\{
    otlp_curl_transport,
    otlp_curl_options,
    otlp_protobuf_serializer,
    otlp_span_exporter,
};

$transport = otlp_curl_transport(
    endpoint: 'https://otlp-gateway-prod-eu-west-0.grafana.net/otlp',
    serializer: otlp_protobuf_serializer(),
    options: otlp_curl_options()
        ->withHeader('Authorization', 'Basic ' . base64_encode('instance-id:api-key')),
);

$telemetry = telemetry(
    resource(['service.name' => 'my-app']),
    tracer_provider(batching_span_processor(otlp_span_exporter($transport))),
);
```

### Setup with Honeycomb

```php
<?php

use function Flow\Telemetry\DSL\{
    telemetry,
    resource,
    tracer_provider,
    batching_span_processor,
};
use function Flow\Bridge\Telemetry\OTLP\DSL\{
    otlp_curl_transport,
    otlp_curl_options,
    otlp_protobuf_serializer,
    otlp_span_exporter,
};

$transport = otlp_curl_transport(
    endpoint: 'https://api.honeycomb.io',
    serializer: otlp_protobuf_serializer(),
    options: otlp_curl_options()
        ->withHeader('x-honeycomb-team', 'your-api-key'),
);

$telemetry = telemetry(
    resource(['service.name' => 'my-app']),
    tracer_provider(batching_span_processor(otlp_span_exporter($transport))),
);
```

## OpenTelemetry Collector

The recommended production architecture is to deploy an [OpenTelemetry Collector](https://opentelemetry.io/docs/collector/) close to your application (same network, Kubernetes cluster, or sidecar container). This approach provides several benefits:

- **Decouple application from backends** - Your application sends telemetry to a single local endpoint, unaware of the final destinations
- **Change APM backends without code changes** - Switch from Jaeger to Grafana or add Datadog by updating collector configuration only
- **Fan-out to multiple backends** - Send the same telemetry data to multiple APM systems simultaneously
- **Offload processing** - Batching, retry logic, filtering, and sampling happen in the collector, not your application
- **Reduce network latency** - Local collector accepts data quickly; it handles slow or unreliable external connections

### Sample Collector Configuration

```yaml
# otel-collector-config.yaml
receivers:
  otlp:
    protocols:
      grpc:
        endpoint: 0.0.0.0:4317
      http:
        endpoint: 0.0.0.0:4318

processors:
  batch:
    timeout: 5s
    send_batch_size: 512

exporters:
  # Local Jaeger for development
  otlp/jaeger:
    endpoint: jaeger:4317
    tls:
      insecure: true

  # Grafana Cloud
  otlp/grafana:
    endpoint: otlp-gateway-prod-eu-west-0.grafana.net:443
    headers:
      Authorization: Basic ${GRAFANA_AUTH}

  # Honeycomb
  otlp/honeycomb:
    endpoint: api.honeycomb.io:443
    headers:
      x-honeycomb-team: ${HONEYCOMB_API_KEY}

service:
  pipelines:
    traces:
      receivers: [otlp]
      processors: [batch]
      exporters: [otlp/jaeger, otlp/grafana, otlp/honeycomb]
    metrics:
      receivers: [otlp]
      processors: [batch]
      exporters: [otlp/grafana]
    logs:
      receivers: [otlp]
      processors: [batch]
      exporters: [otlp/grafana]
```

### Docker Compose Example

```yaml
# compose.yaml
services:
  otel-collector:
    image: otel/opentelemetry-collector-contrib:latest
    command: ["--config=/etc/otel-collector-config.yaml"]
    volumes:
      - ./otel-collector-config.yaml:/etc/otel-collector-config.yaml
    ports:
      - "4317:4317"  # gRPC
      - "4318:4318"  # HTTP
    environment:
      - GRAFANA_AUTH=${GRAFANA_AUTH}
      - HONEYCOMB_API_KEY=${HONEYCOMB_API_KEY}

  app:
    build: .
    environment:
      - OTEL_EXPORTER_OTLP_ENDPOINT=http://otel-collector:4318
    depends_on:
      - otel-collector
```

With this setup, your PHP application only needs to know about `http://otel-collector:4318`. Adding or removing APM backends becomes a configuration change in the collector, requiring no application redeployment.

## Configuration Options

The `CurlTransportOptions` class provides a fluent interface for configuring the curl transport.

| Method                                                    | Description             | Default    |
|-----------------------------------------------------------|-------------------------|------------|
| `withTimeout(int $seconds)`                               | Request timeout         | 30         |
| `withConnectTimeout(int $seconds)`                        | Connection timeout      | 10         |
| `withHeader(string $name, string $value)`                 | Add a single header     | -          |
| `withHeaders(array $headers)`                             | Set all headers         | []         |
| `withCompression(bool $enabled)`                          | Enable gzip compression | false      |
| `withSslVerification(bool $verifyPeer, bool $verifyHost)` | SSL verification        | true, true |
| `withSslCertificate(string $certPath, ?string $keyPath)`  | Client certificate      | -          |
| `withCaInfo(string $caInfoPath)`                          | CA certificate bundle   | -          |
| `withProxy(string $proxy)`                                | Proxy server            | -          |
| `withFollowRedirects(bool $follow, int $maxRedirects)`    | Redirect behavior       | true, 3    |

**Example:**

```php
<?php

use function Flow\Bridge\Telemetry\OTLP\DSL\otlp_curl_options;

$options = otlp_curl_options()
    ->withTimeout(60)
    ->withConnectTimeout(15)
    ->withHeader('Authorization', 'Bearer token')
    ->withHeader('X-Custom-Header', 'value')
    ->withCompression()
    ->withSslVerification(verifyPeer: true, verifyHost: true)
    ->withProxy('http://proxy:8080');
```

## OTLP Endpoints

Standard OTLP endpoint paths:

| Signal  | HTTP Path     | gRPC Port |
|---------|---------------|-----------|
| Traces  | `/v1/traces`  | 4317      |
| Metrics | `/v1/metrics` | 4317      |
| Logs    | `/v1/logs`    | 4317      |

The transport automatically appends the correct path based on the signal type. You only need to provide the base
endpoint URL.

**HTTP endpoints:**

- OpenTelemetry Collector: `http://localhost:4318`
- Grafana Cloud: `https://otlp-gateway-prod-{region}.grafana.net/otlp`
- Honeycomb: `https://api.honeycomb.io`

**gRPC endpoints:**

- OpenTelemetry Collector: `localhost:4317`
- Jaeger: `localhost:4317`
