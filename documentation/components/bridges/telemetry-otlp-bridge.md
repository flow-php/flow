---
package: flow-php/telemetry-otlp-bridge
---

# Telemetry OTLP Bridge

The OTLP (OpenTelemetry Protocol) Bridge has serializers and transports that send telemetry data to
OpenTelemetry-compatible backends. It adds production-ready export to the
[Flow Telemetry](/documentation/components/libs/telemetry.md) library.

[PACKAGE_NAV]

[TOC]

## Installation

For the installation instructions, read
the [installation page](/documentation/installation/packages/telemetry-otlp-bridge.md).

## Transports

The bridge has three transports that send telemetry data to OTLP endpoints.

| Transport | Protocol    | Use Case                                                                | Requirements |
|-----------|-------------|-------------------------------------------------------------------------|--------------|
| Curl      | HTTP (sync) | Production, low latency                                                 | ext-curl     |
| gRPC      | gRPC        | High-performance binary protocol                                        | ext-grpc     |
| Stream    | JSONL       | Sidecar collectors, log shippers, FaaS / Kubernetes stdout/err scraping | None         |

### Curl Transport (Recommended)

The Curl transport sends each request synchronously with `curl_exec`. With a local collector, each request is fast,
so the effect on the latency of your application stays small.

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

// With custom options (timeouts in milliseconds - see "Timeouts" below)
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

The gRPC transport uses the native gRPC protocol for fast binary communication. It needs the `ext-grpc` PHP
extension. OTLP/gRPC requires Protobuf, so the transport creates the protobuf request factory internally. It has no
serializer parameter.

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

> Note: gRPC has no separate connect timeout. `timeoutMs` is the deadline of each call, for DNS, connect, send,
> and receive together. For more information, read [Timeouts](#timeouts).

### Stream Transport

The Stream transport implements
the [OTLP File Exporter spec](https://opentelemetry.io/docs/specs/otel/protocol/file-exporter/). It writes JSONL to
an absolute file path or to a `php://` stream wrapper. The constructor opens the handle once, and all `send()` calls
use it. Each call appends one JSON Line under `LOCK_EX`, so concurrent writers interleave only at line boundaries.

The specification supports only JSON encoding. The transport always uses the JSON serializer, and there is no
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

`filePermissions` and `createDirectories` apply only to a file path destination. They are ignored for
`php://` URIs. For production deployments.

One transport instance writes to one destination. To split logs, metrics, and traces across multiple files, build three
transports and connect them to three exporters. To keep all signals in one file, use the same destination in all
exporters. The OpenTelemetry Collector handles mixed JSONL correctly.

## Wire Encoding

The OTLP specification defines fixed encodings for each transport. The bridge enforces them.

| Transport | JSON | Protobuf | Notes                                                                       |
|-----------|------|----------|-----------------------------------------------------------------------------|
| Curl      | yes  | yes      | OTLP/HTTP supports both. The transport constructor takes the serializer     |
| gRPC      | no   | yes      | OTLP/gRPC requires Protobuf. The transport builds it internally             |
| Stream    | yes  | no       | The OTLP File Exporter spec supports only JSON                              |

For curl, give `otlp_json_serializer()` (default) or `otlp_protobuf_serializer()`:

```php
<?php

use function Flow\Bridge\Telemetry\OTLP\DSL\{otlp_curl_transport, otlp_json_serializer, otlp_protobuf_serializer};

// JSON over HTTP - default; great for development and debugging
$transport = otlp_curl_transport('http://localhost:4318');

// Protobuf over HTTP - smaller payloads; recommended for production
$transport = otlp_curl_transport('http://localhost:4318', otlp_protobuf_serializer());
```

The Protobuf serializer needs the `google/protobuf` package.

## Timeouts

For production, run an OpenTelemetry Collector close to the application (loopback, UDS, or sidecar). The roundtrip
is then below one millisecond, and a stuck collector never freezes your PHP process at shutdown.

The curl transport is synchronous. Each `send()` blocks for up to `timeout_ms`, and then returns or throws. The
batching processor keeps the export off the hot path. It flushes only after each `batch_size` signals (or on age /
flush / shutdown). gRPC progresses in the background, so its deadline for each call stays at 250 ms.

| Transport   | Setting                 | Default | Unit         | Bounds                                                           |
|-------------|-------------------------|--------:|--------------|------------------------------------------------------------------|
| Curl (sync) | `withTimeout()`         |   10000 | milliseconds | Per-request: connect + send + receive (max time `send()` blocks) |
| Curl (sync) | `withConnectTimeout()`  |     250 | milliseconds | TCP/TLS connection establishment only                            |
| Curl (sync) | `withShutdownTimeout()` |    5000 | milliseconds | Reserved for the failover drain budget at shutdown               |
| Async curl  | `withTimeout()`         |    5000 | milliseconds | Per-request wall-clock. Must cover the gap between pumps         |
| Async curl  | `withConnectTimeout()`  |    1500 | milliseconds | TCP/TLS connect. A larger default allows infrequent pumping      |
| Async curl  | `withPumpTimeout()`     |     100 | milliseconds | Per-`tick()` bounded drive budget (`0` = single exec round)      |
| Async curl  | `withShutdownTimeout()` |    5000 | milliseconds | Wall-clock budget for draining pending requests at shutdown      |
| gRPC        | `timeoutMs`             |     250 | milliseconds | Per-call deadline (no separate connect bound)                    |
| gRPC        | `shutdownTimeoutMs`     |    5000 | milliseconds | Wall-clock budget for draining pending calls at shutdown         |

With a local collector, each send takes less than one millisecond, so the defaults are only upper limits. On failure,
`send()` throws synchronously: `TransportException`, or `FailoverTransportException` after the batch goes to the
failover. Async curl uses different values. For more information, read [Asynchronous transport](#async-curl-transport).

```php
<?php

use function Flow\Bridge\Telemetry\OTLP\DSL\{otlp_curl_options, otlp_curl_transport, otlp_grpc_transport};

// Local collector - defaults are usually fine
$curl = otlp_curl_transport('http://localhost:4318');
$grpc = otlp_grpc_transport('localhost:4317');

// Remote collector - raise both to suit your network
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

> Why milliseconds: telemetry exports to a local collector take microseconds. The API with seconds in earlier
> versions did not allow tight, realistic deadlines. A negative value for `withTimeout()` /
> `withConnectTimeout()` throws `\InvalidArgumentException`.

## Long-running workers {#long-running-workers}

The synchronous curl transport completes each request before `send()` returns, so nothing ages out between messages.
The batching processor keeps the export off the hot path of the worker. It flushes after each `batch_size` signals or
at the age limit. In a Symfony Messenger worker, the bundle flushes after each message and on stop. For more
information, read the [Symfony telemetry bundle](/documentation/components/bridges/symfony-telemetry-bundle.md) page.
Keep a local Collector, so that each flush takes less than one millisecond. For non-blocking dispatch, read
[Asynchronous transport](#async-curl-transport).

## Asynchronous transport (`AsyncCurlTransport`) {#async-curl-transport}

`AsyncCurlTransport` uses `curl_multi` for non-blocking I/O. `send()` queues the request and returns without waiting.
To use it, call `otlp_async_curl_transport()`, or set `transport.type: 'async_curl'` in the bundle.

A queued request moves forward only while the host pumps the handle: on the next `send()`, `shutdown()`, or
`tick()`. `tick()` is bounded and select-driven. It drives pending requests for up to `pump_timeout_ms` (default
100 ms), so a local backend completes within one tick. `0` falls back to one non-blocking exec round. This is rarely
sufficient, so use the default.

```php
use function Flow\Bridge\Telemetry\OTLP\DSL\otlp_async_curl_transport;

$transport = otlp_async_curl_transport('http://localhost:4318');

// Pump once per worker loop so in-flight requests complete:
$transport->tick();
```

The `messenger` instrumentation of the bundle pumps each `async_curl` transport on
`WorkerRunningEvent` (~1s for an idle worker) in a Symfony Messenger worker. The 1500 ms default of `connect_timeout_ms` is sized for this cadence. In other
places, you must call `tick()` yourself.

Failures show on a later `send()`/`tick()`/`shutdown()`. At that time, no caller is on the stack. They therefore go to the
failover transport. Without a failover, they go to the injected `ErrorHandler` (5th constructor arg, default
`ErrorLogHandler`). For this reason, the async transport takes an error handler, and the synchronous one only throws.

Use the synchronous `curl` transport by default. Use the async transport only for non-blocking dispatch with a guaranteed pump cadence.

## Failover Transport

`CurlTransport` and `GrpcTransport` both accept an optional `Transport $failover` argument. With it, batches that
failed on the primary go to the failover transport. Telemetry is then kept, also for an unreachable primary
backend. A typical pair is a gRPC primary → Stream (JSONL on disk) failover. A collector that is down then still
leaves data that the operator can replay later.

### How it works

The transport delays error handling to the next `send()` or `shutdown()`. The next call drains the previous
in-flight request, with these results:

1. Primary OK: the batch is dropped from the pending list. Nothing else happens.
2. Primary failed, failover accepted: the previous batch goes to `failover->send()`, and the data is kept. The
   transport still throws a `FailoverTransportException`, so the operator knows that the primary is degraded.
3. Primary failed, failover also failed: the previous batch is lost. The exception carries both throwables in the
   `failures` list.

In each case, the current request goes to the primary before the exception is thrown. A previous failure therefore
never loses the new batch.

On `shutdown()`, the primary drains the pending requests and applies the same forwarding logic. Then it calls
`failover->shutdown()` (cascade). The primary owns the failover lifecycle. You do not have to shut down the failover
separately.

### Behavior matrix

| Primary | Failover send | Outcome                         | Exception                                                |
|---------|---------------|---------------------------------|----------------------------------------------------------|
| OK      | -             | Data delivered                  | none                                                     |
| Failed  | OK            | Data preserved via failover     | `FailoverTransportException` (1 entry, `failover: null`) |
| Failed  | Failed        | Data lost, both errors reported | `FailoverTransportException` (1 entry, both errors)      |

`FailoverTransportException` extends `TransportException`, so existing `catch (TransportException $e)` blocks in
exporters still work. For details of each batch, use the structured `$exception->failures` list:

```php
foreach ($exception->failures as $failure) {
    $failure['primary'];   // \Throwable - the primary error
    $failure['failover'];  // \Throwable|null - null means failover absorbed the batch
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

The drain logic uses the native non-blocking primitive of each backend that has one. The `UnaryCall::wait()` of
gRPC is blocking by design. If the second flush arrives before the previous call resolved, it waits for the result
before it forwards to the failover. The `timeoutMs` deadline of each call limits this wait. Configure it tight, so that
a stuck collector cannot block the shutdown forever. For more information, read [Timeouts](#timeouts).

### Caveats

- The failover has one level only. A failover transport cannot declare another failover itself. For cascading
  destinations, compose multiple primaries instead.
- The forward to the failover happens on the next flush, not at the moment of the failure. `shutdown()` reports a
  single batch that was lost between the first send and the process exit.
- The failover transport receives the original `Signals` instance. It sees the same content as the primary.

## Complete Setup

This complete example sets up telemetry with OTLP export.

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

The recommended production architecture has an [OpenTelemetry Collector](https://opentelemetry.io/docs/collector/)
close to your application (same network, Kubernetes cluster, or sidecar container). This approach has these
benefits:

- The application does not depend on the backends: your application sends telemetry to one local endpoint, and it
  does not know the final destinations
- APM backends change without code changes: to switch from Jaeger to Grafana or to add Datadog, change only the
  collector configuration
- Fan-out to multiple backends: send the same telemetry data to multiple APM systems at the same time
- Processing moves out of the application: batching, retry logic, filtering, and sampling happen in the collector
- Lower network latency: the local collector accepts data quickly, and it handles slow or unreliable external
  connections

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

With this setup, your PHP application needs to know only `http://otel-collector:4318`. To add or remove APM
backends, change the configuration of the collector. The application needs no new deployment.

## Configuration Options

The `CurlTransportOptions` class has a fluent interface for the configuration of the curl transport.

| Method                                                    | Description                         | Default    |
|-----------------------------------------------------------|-------------------------------------|------------|
| `withTimeout(int $milliseconds)`                          | Per-request total timeout (ms)      | 10000      |
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

For guidance on the default values, read [Timeouts](#timeouts).

Example:

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

The transport appends the correct path for the signal type automatically. Give only the base endpoint URL.

HTTP endpoints:

- OpenTelemetry Collector: `http://localhost:4318`
- Grafana Cloud: `https://otlp-gateway-prod-{region}.grafana.net/otlp`
- Honeycomb: `https://api.honeycomb.io`

gRPC endpoints:

- OpenTelemetry Collector: `localhost:4317`
- Jaeger: `localhost:4317`
