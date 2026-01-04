# Flow PHP - Telemetry OTLP Bridge

OTLP (OpenTelemetry Protocol) exporter bridge for Flow PHP Telemetry library.

Enables exporting traces, metrics, and logs to any OTLP-compatible backend:
- SigNoz
- Jaeger
- Grafana (Tempo, Mimir, Loki)
- Sentry
- Datadog
- Honeycomb
- New Relic
- Any OTLP-compatible collector

> [!IMPORTANT]
> This repository is a subtree split from our monorepo. If you'd like to contribute,
> please visit our main monorepo [flow-php/flow](https://github.com/flow-php/flow).

## Installation

```bash
composer require flow-php/telemetry-otlp-bridge
```

You also need a PSR-18 HTTP client:

```bash
composer require symfony/http-client nyholm/psr7
```

## Usage

```php
<?php

use function Flow\Bridge\Telemetry\OTLP\DSL\otlp_telemetry;
use function Flow\Bridge\Telemetry\OTLP\DSL\otlp_http_transport;
use Nyholm\Psr7\Factory\Psr17Factory;
use Symfony\Component\HttpClient\Psr18Client;

$psr17Factory = new Psr17Factory();
$httpClient = new Psr18Client();

$telemetry = otlp_telemetry(
    transport: otlp_http_transport(
        endpoint: 'http://localhost:4318',
        httpClient: $httpClient,
        requestFactory: $psr17Factory,
        streamFactory: $psr17Factory,
    ),
    serviceName: 'my-etl-pipeline',
);

$tracer = $telemetry->tracer('my-component');
$span = $tracer->startSpan('process-batch');
// ... work ...
$span->end();

$telemetry->flush();
```

## Resources

- [Documentation](https://flow-php.com/documentation/components/bridges/telemetry-otlp/)
- [Installation](https://flow-php.com/documentation/installation/)
- [Contributing](https://flow-php.com/documentation/contributing/)
- [Upgrading](https://flow-php.com/documentation/upgrading/)
