---
package: flow-php/telemetry-otlp-bridge
---

# Telemetry OTLP Bridge

The OTLP (OpenTelemetry Protocol) Bridge provides serializers and transports for sending telemetry data to
OpenTelemetry-compatible backends.
It extends the [Flow Telemetry](/documentation/components/libs/telemetry.md) library with production-ready export
capabilities.

[PACKAGE_NAV]

[TOC]

## Installation

For detailed installation instructions, see
the [installation page](/documentation/installation/packages/telemetry-otlp-bridge.md).

## Transports

The bridge provides three transport options for sending telemetry data to OTLP endpoints.

| Transport  | Protocol     | Use Case                                                                | Requirements |
|------------|--------------|-------------------------------------------------------------------------|--------------|
| **Curl**   | HTTP (async) | Production, low latency                                                 | ext-curl     |
| **gRPC**   | gRPC         | High-performance binary protocol                                        | ext-grpc     |
| **Stream** | JSONL        | Sidecar collectors, log shippers, FaaS / Kubernetes stdout/err scraping | None         |

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

// With custom options (timeouts in milliseconds — see "Timeouts" below)
$transport = otlp_curl_transport(
    endpoint: 'https://otlp.example.com:4318',
    serializer: otlp_json_serializer(),
    options: otlp_curl_options()
        ->withTimeout(2000)
        ->withConnectTimeout(500)
        ->withHeader('Authorization', 'Bearer your-token')
        ->withCompression(),
);
```

### gRPC Transport

The gRPC transport uses the native gRPC protocol for high-performance binary communication. It requires the `ext-grpc`
PHP extension. OTLP/gRPC mandates Protobuf, so the transport instantiates the protobuf request factory internally —
no serializer parameter.

```php
<?php

use function Flow\Bridge\Telemetry\OTLP\DSL\otlp_grpc_transport;

$transport = otlp_grpc_transport(endpoint: 'localhost:4317');

// With authentication and a tighter call deadline (in milliseconds)
$transport = otlp_grpc_transport(
    endpoint: 'otlp.example.com:4317',
    headers: [
        'Authorization' => 'Bearer your-token',
    ],
    insecure: false, // Use TLS
    timeoutMs: 2000, // call deadline: connect + send + receive
);
```

> **Note**: gRPC has no separate connect timeout — `timeoutMs` is the per-call deadline that bounds DNS, connect, send,
> and receive together. See [Timeouts](#timeouts).

### Stream Transport

The Stream transport implements
the [OTLP File Exporter spec](https://opentelemetry.io/docs/specs/otel/protocol/file-exporter/).
It writes JSONL to either an absolute file path or a `php://` stream wrapper. The handle is opened once in
the constructor and reused across `send()` calls; each call appends one JSON Line under `LOCK_EX` so concurrent
writers interleave at line boundaries.

Only JSON encoding is supported per the spec — the transport hard-codes the JSON serializer; there is no
serializer parameter.

```php
<?php

use function Flow\Bridge\Telemetry\OTLP\DSL\otlp_stream_transport;

// Append to a file (creates parent directories by default, chmod 0644 on first create)
$transport = otlp_stream_transport('/var/log/otel/logs.jsonl');

// Stream wrapper destinations for FaaS / Kubernetes log scraping
$transport = otlp_stream_transport('php://stdout');
$transport = otlp_stream_transport('php://stderr');

// Tweak file mode and disable directory creation
$transport = otlp_stream_transport(
    destination: '/var/log/otel/logs.jsonl',
    filePermissions: 0o640,
    createDirectories: false,
);
```

`filePermissions` and `createDirectories` apply only when the destination is a file path; they are ignored for
`php://` URIs. For production deployments.

One transport instance writes to one destination. To split logs, metrics, and traces across multiple files,
build three transports and wire them to three exporters. To keep all signals in one file (the OpenTelemetry
Collector handles mixed JSONL just fine), reuse the same destination across exporters.

## Wire Encoding

The OTLP spec defines fixed encodings per transport. The bridge enforces them.

| Transport | JSON | Protobuf | Notes                                                                      |
|-----------|:----:|:--------:|----------------------------------------------------------------------------|
| Curl      |  ✅   |    ✅     | OTLP/HTTP supports both; pick a serializer when constructing the transport |
| gRPC      |  ❌   |    ✅     | OTLP/gRPC mandates Protobuf; the transport builds it internally            |
| Stream    |  ✅   |    ❌     | OTLP File Exporter spec only supports JSON                                 |

For curl, pass `otlp_json_serializer()` (default) or `otlp_protobuf_serializer()`:

```php
<?php

use function Flow\Bridge\Telemetry\OTLP\DSL\{otlp_curl_transport, otlp_json_serializer, otlp_protobuf_serializer};

// JSON over HTTP — default; great for development and debugging
$transport = otlp_curl_transport('http://localhost:4318');

// Protobuf over HTTP — smaller payloads; recommended for production
$transport = otlp_curl_transport('http://localhost:4318', otlp_protobuf_serializer());
```

The Protobuf serializer requires the `google/protobuf` package.

## Timeouts

The production recommendation is to run an OpenTelemetry Collector close to the application (loopback, UDS, or
sidecar), so the roundtrip is sub-millisecond and a stuck collector never freezes your PHP process at shutdown.

The curl transport is **synchronous**: each `send()` blocks up to `timeout_ms`, then returns or throws. Keeping export
off the hot path is the batching processor's job — it flushes only every `batch_size` signals (or on age / flush /
shutdown). gRPC progresses in the background, so its per-call deadline stays at **250 ms**.

| Transport   | Setting                 | Default | Unit         | Bounds                                                           |
|-------------|-------------------------|--------:|--------------|------------------------------------------------------------------|
| Curl (sync) | `withTimeout()`         |   10000 | milliseconds | Per-request: connect + send + receive (max time `send()` blocks) |
| Curl (sync) | `withConnectTimeout()`  |     250 | milliseconds | TCP/TLS connection establishment only                            |
| Curl (sync) | `withShutdownTimeout()` |    5000 | milliseconds | Reserved for the failover drain budget at shutdown               |
| Async curl  | `withTimeout()`         |    5000 | milliseconds | Per-request wall-clock; must span the gap between pumps          |
| Async curl  | `withConnectTimeout()`  |    1500 | milliseconds | TCP/TLS connect; larger default tolerates infrequent pumping     |
| Async curl  | `withPumpTimeout()`     |     100 | milliseconds | Per-`tick()` bounded drive budget (`0` = single exec round)      |
| Async curl  | `withShutdownTimeout()` |    5000 | milliseconds | Wall-clock budget for draining pending requests at shutdown      |
| gRPC        | `timeoutMs`             |     250 | milliseconds | Per-call deadline (no separate connect bound)                    |
| gRPC        | `shutdownTimeoutMs`     |    5000 | milliseconds | Wall-clock budget for draining pending calls at shutdown         |

Against a local collector each send is sub-millisecond, so the defaults are only ceilings. On failure `send()` throws
synchronously — `TransportException`, or `FailoverTransportException` once the batch is forwarded to the failover. Async
curl is tuned differently — see [Asynchronous transport](#async-curl-transport).

```php
<?php

use function Flow\Bridge\Telemetry\OTLP\DSL\{otlp_curl_options, otlp_curl_transport, otlp_grpc_transport};

// Local collector — defaults are usually fine
$curl = otlp_curl_transport('http://localhost:4318');
$grpc = otlp_grpc_transport('localhost:4317');

// Remote collector — raise both to suit your network
$remoteCurl = otlp_curl_transport(
    endpoint: 'https://otlp.example.com:4318',
    options: otlp_curl_options()
        ->withTimeout(5000)
        ->withConnectTimeout(1000),
);

$remoteGrpc = otlp_grpc_transport(
    endpoint: 'otlp.example.com:4317',
    insecure: false,
    timeoutMs: 5000,
);
```

> **Why milliseconds**: telemetry exports to a local collector complete in microseconds; the second-granularity API in
> earlier versions could not express tight, realistic deadlines. A negative value to `withTimeout()` /
> `withConnectTimeout()` raises `\InvalidArgumentException`.

## Long-running workers {#long-running-workers}

The synchronous curl transport completes each request before `send()` returns, so nothing ages out between messages.
Keeping export off the worker's hot path is the **batching processor** — it flushes per `batch_size` signals or age
limit. In a Symfony Messenger worker the bundle flushes after each message and on stop (see the
[Symfony telemetry bundle](/documentation/components/bridges/symfony-telemetry-bundle.md)); keep a local Collector so
each flush stays sub-millisecond. For non-blocking dispatch, see [Asynchronous transport](#async-curl-transport).

## Asynchronous transport (`AsyncCurlTransport`) {#async-curl-transport}

`AsyncCurlTransport` uses `curl_multi` for non-blocking I/O: `send()` queues the request and returns without waiting.
Opt in with `otlp_async_curl_transport()` or the bundle's `transport.type: 'async_curl'`.

A queued request only advances while the host pumps the handle — on the next `send()`, `shutdown()`, or `tick()`.
`tick()` is bounded and select-driven: it drives pending requests for up to `pump_timeout_ms` (default **100 ms**), so a
local backend completes within a single tick. `0` falls back to one non-blocking exec round (rarely enough — prefer the
default).

```php
use function Flow\Bridge\Telemetry\OTLP\DSL\otlp_async_curl_transport;

$transport = otlp_async_curl_transport('http://localhost:4318');

// Pump once per worker loop so in-flight requests complete:
$transport->tick();
```

In a Symfony Messenger worker with `messenger` instrumentation enabled, the bundle pumps every `async_curl` transport on
`WorkerRunningEvent` (~1s when idle) — the cadence the **1500 ms** `connect_timeout_ms` default is sized for. Elsewhere
you must call `tick()` yourself.

Failures surface on a later `send()`/`tick()`/`shutdown()` (no caller on the stack), so they go to the failover
transport or, without one, the injected `ErrorHandler` (5th constructor arg, default `ErrorLogHandler`) — which is why
the async transport takes an error handler and the synchronous one just throws.

Prefer the synchronous `curl` transport unless you need non-blocking dispatch and can guarantee a pump cadence.

## Failover Transport

Both `CurlTransport` and `GrpcTransport` accept an optional `Transport $failover` argument. When set, batches that
failed on the primary are forwarded to the failover transport so telemetry is preserved even when the primary backend
is unreachable. A typical pairing is **gRPC primary → Stream (JSONL on disk) failover** so a downed collector still
leaves recoverable data the operator can replay later.

### How it works

The transport defers error handling to the *next* `send()` or `shutdown()`. When the next call drains the prior
in-flight request:

1. **Primary OK** — the batch is dropped from the pending list. Nothing else happens.
2. **Primary failed, failover accepted** — the prior batch is forwarded to `failover->send()`; data preserved. The
   transport still raises a `FailoverTransportException` so the operator is informed primary is degraded.
3. **Primary failed, failover also failed** — the prior batch is lost. The exception carries both throwables in the
   `failures` list.

In every case the **current** request is dispatched to the primary *before* the exception is raised, so the new batch
is never lost due to a prior failure.

On `shutdown()`, the primary drains pending requests, applies the same forwarding logic, then calls
`failover->shutdown()` (cascade). The failover lifecycle is owned by the primary — you do not need to shut it down
separately.

### Behavior matrix

| Primary | Failover send | Outcome                         | Exception                                                |
|---------|---------------|---------------------------------|----------------------------------------------------------|
| OK      | —             | Data delivered                  | none                                                     |
| Failed  | OK            | Data preserved via failover     | `FailoverTransportException` (1 entry, `failover: null`) |
| Failed  | Failed        | Data lost; both errors surfaced | `FailoverTransportException` (1 entry, both errors)      |

`FailoverTransportException` extends `TransportException`, so existing `catch (TransportException $e)` blocks in
exporters keep working. The structured `$exception->failures` list is available when you want per-batch detail:

```php
foreach ($exception->failures as $failure) {
    $failure['primary'];   // \Throwable — the primary error
    $failure['failover'];  // \Throwable|null — null means failover absorbed the batch
}
```

### Examples

```php
<?php

use function Flow\Bridge\Telemetry\OTLP\DSL\{otlp_curl_transport, otlp_grpc_transport, otlp_protobuf_serializer, otlp_stream_transport};

// gRPC primary → JSONL on disk failover (recommended for production)
$transport = otlp_grpc_transport(
    endpoint: 'localhost:4317',
    failover: otlp_stream_transport('/var/log/otel/failed.jsonl'),
);

// Curl primary → Stream (php://stderr) failover for FaaS / Kubernetes log scraping
$transport = otlp_curl_transport(
    endpoint: 'http://localhost:4318',
    serializer: otlp_protobuf_serializer(),
    failover: otlp_stream_transport('php://stderr'),
);
```

### Edge case: blocking on shutdown

The drain logic uses each backend's native non-blocking primitive where it exists. **gRPC's `UnaryCall::wait()` is
blocking by design** — when the second flush arrives before the prior call resolved, it waits for resolution before
forwarding to failover. This is mitigated by the per-call `timeoutMs` deadline; configure it tight enough that a stuck
collector cannot hang shutdown indefinitely. See [Timeouts](#timeouts).

### Caveats

- The failover is **single-level**: a failover transport cannot itself declare another failover. Compose multiple
  primaries instead if you need cascading destinations.
- Forwarding to the failover happens on the *next* flush, not at the moment of failure. A single batch lost between
  the first send and process exit is surfaced from `shutdown()`.
- The failover transport receives the original `Signals` instance — it sees the same content the primary did.

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
    otlp_exporter,
};

$resource = resource([
    'service.name' => 'order-service',
    'service.version' => '1.2.0',
    'deployment.environment' => 'production',
]);

$options = otlp_curl_options()
    ->withTimeout(2000)
    ->withCompression();

$transport = otlp_curl_transport(
    endpoint: 'http://otel-collector:4318',
    serializer: otlp_json_serializer(),
    options: $options,
);

$exporter = otlp_exporter($transport);

$telemetry = telemetry(
    $resource,
    tracer_provider(batching_span_processor($exporter)),
    meter_provider(batching_metric_processor($exporter)),
    logger_provider(batching_log_processor($exporter)),
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
    otlp_exporter,
};

$transport = otlp_curl_transport(
    endpoint: 'https://otlp-gateway-prod-eu-west-0.grafana.net/otlp',
    serializer: otlp_protobuf_serializer(),
    options: otlp_curl_options()
        ->withHeader('Authorization', 'Basic ' . base64_encode('instance-id:api-key')),
);

$telemetry = telemetry(
    resource(['service.name' => 'my-app']),
    tracer_provider(batching_span_processor(otlp_exporter($transport))),
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
    otlp_exporter,
};

$transport = otlp_curl_transport(
    endpoint: 'https://api.honeycomb.io',
    serializer: otlp_protobuf_serializer(),
    options: otlp_curl_options()
        ->withHeader('x-honeycomb-team', 'your-api-key'),
);

$telemetry = telemetry(
    resource(['service.name' => 'my-app']),
    tracer_provider(batching_span_processor(otlp_exporter($transport))),
);
```

## OpenTelemetry Collector

The recommended production architecture is to deploy
an [OpenTelemetry Collector](https://opentelemetry.io/docs/collector/) close to your application (same network,
Kubernetes cluster, or sidecar container). This approach provides several benefits:

- **Decouple application from backends** - Your application sends telemetry to a single local endpoint, unaware of the
  final destinations
- **Change APM backends without code changes** - Switch from Jaeger to Grafana or add Datadog by updating collector
  configuration only
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
      receivers: [ otlp ]
      processors: [ batch ]
      exporters: [ otlp/jaeger, otlp/grafana, otlp/honeycomb ]
    metrics:
      receivers: [ otlp ]
      processors: [ batch ]
      exporters: [ otlp/grafana ]
    logs:
      receivers: [ otlp ]
      processors: [ batch ]
      exporters: [ otlp/grafana ]
```

### Docker Compose Example

```yaml
# compose.yaml
services:
  otel-collector:
    image: otel/opentelemetry-collector-contrib:latest
    command: [ "--config=/etc/otel-collector-config.yaml" ]
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

With this setup, your PHP application only needs to know about `http://otel-collector:4318`. Adding or removing APM
backends becomes a configuration change in the collector, requiring no application redeployment.

## Configuration Options

The `CurlTransportOptions` class provides a fluent interface for configuring the curl transport.

| Method                                                    | Description                         | Default    |
|-----------------------------------------------------------|-------------------------------------|------------|
| `withTimeout(int $milliseconds)`                          | Per-request total timeout (ms)      | 5000       |
| `withConnectTimeout(int $milliseconds)`                   | TCP/TLS connect timeout (ms)        | 250        |
| `withShutdownTimeout(int $milliseconds)`                  | Wall-clock drain budget at shutdown | 5000       |
| `withHeader(string $name, string $value)`                 | Add a single header                 | -          |
| `withHeaders(array $headers)`                             | Set all headers                     | []         |
| `withCompression(bool $enabled)`                          | Enable gzip compression             | false      |
| `withSslVerification(bool $verifyPeer, bool $verifyHost)` | SSL verification                    | true, true |
| `withSslCertificate(string $certPath, ?string $keyPath)`  | Client certificate                  | -          |
| `withCaInfo(string $caInfoPath)`                          | CA certificate bundle               | -          |
| `withProxy(string $proxy)`                                | Proxy server                        | -          |
| `withFollowRedirects(bool $follow, int $maxRedirects)`    | Redirect behavior                   | true, 3    |

See [Timeouts](#timeouts) for guidance on the default values.

**Example:**

```php
<?php

use function Flow\Bridge\Telemetry\OTLP\DSL\otlp_curl_options;

$options = otlp_curl_options()
    ->withTimeout(2000)
    ->withConnectTimeout(500)
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
