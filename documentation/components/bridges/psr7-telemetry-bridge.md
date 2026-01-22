# PSR-7 Telemetry Bridge

Flow PSR-7 Telemetry Bridge provides carriers for propagators that can pass and read telemetry context
and baggage via PSR-7 request and response objects.

- [Back](/documentation/introduction.md)
- [Packagist](https://packagist.org/packages/flow-php/psr7-telemetry-bridge)
- [GitHub](https://github.com/flow-php/psr7-telemetry-bridge)

[TOC]

## Installation

```
composer require flow-php/psr7-telemetry-bridge:~--FLOW_PHP_VERSION--
```

## Overview

This bridge connects Flow PHP's Telemetry library with PSR-7 compliant HTTP message implementations, enabling telemetry
context propagation in HTTP-based applications. It provides carriers that allow propagators to inject and extract
telemetry context (trace context, baggage) through HTTP headers in PSR-7 Request and Response objects.

This is particularly useful for:

- Distributed tracing across microservices
- Propagating correlation IDs through HTTP calls
- Passing baggage data (key-value pairs) across service boundaries
- Integration with OpenTelemetry-based observability stacks
- Framework-agnostic telemetry integration

> **Note:** This bridge works with any PSR-7 compliant HTTP message implementation (Nyholm/PSR-7, Guzzle, Laminas, etc.).
> Due to PSR-7's immutable nature, use `unwrap()` on the `ResponseCarrier` to retrieve the modified response after
> injecting headers.

## Requirements

- PHP 8.3+
- flow-php/telemetry
- psr/http-message ^1.1 || ^2.0

## Usage

### Extracting context from incoming requests

```php
use Flow\Bridge\Psr7\Telemetry\RequestCarrier;
use Flow\Telemetry\Propagation\CompositePropagator;
use Flow\Telemetry\Propagation\W3CTraceContext;
use Flow\Telemetry\Propagation\W3CBaggage;

// Create propagator
$propagator = new CompositePropagator([
    new W3CTraceContext(),
    new W3CBaggage(),
]);

// $request is a PSR-7 ServerRequestInterface
$carrier = new RequestCarrier($request);
$ctx = $propagator->extract($carrier);

// Use extracted context
if ($ctx->spanContext !== null) {
    $traceId = $ctx->spanContext->traceId->toHex();
    // ...
}
```

### Injecting context into outgoing responses

```php
use Flow\Bridge\Psr7\Telemetry\ResponseCarrier;
use Flow\Telemetry\Propagation\CompositePropagator;
use Flow\Telemetry\Propagation\W3CTraceContext;
use Flow\Telemetry\Propagation\W3CBaggage;
use Flow\Telemetry\Propagation\PropagationContext;

// Create propagator
$propagator = new CompositePropagator([
    new W3CTraceContext(),
    new W3CBaggage(),
]);

// $response is a PSR-7 ResponseInterface
$carrier = new ResponseCarrier($response);
$propagator->inject($ctx, $carrier);

// Get the modified response (PSR-7 responses are immutable)
$response = $carrier->unwrap();
```

### DSL Functions

The bridge provides convenient DSL functions:

```php
use function Flow\Bridge\Psr7\Telemetry\DSL\psr7_request_carrier;
use function Flow\Bridge\Psr7\Telemetry\DSL\psr7_response_carrier;

$requestCarrier = psr7_request_carrier($request);
$responseCarrier = psr7_response_carrier($response);
```
