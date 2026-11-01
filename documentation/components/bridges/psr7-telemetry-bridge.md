---
package: flow-php/psr7-telemetry-bridge
---

# PSR-7 Telemetry Bridge

This bridge has carriers for propagators. With them, propagators pass and read telemetry context and baggage
through PSR-7 request and response objects.

[PACKAGE_NAV]

[TOC]

## Installation

For the installation instructions, read the [installation page](/documentation/installation/packages/psr7-telemetry-bridge.md).

## Overview

This bridge connects the Telemetry library of Flow PHP with PSR-7 compliant HTTP message implementations.
HTTP-based applications can then propagate telemetry context. Its carriers let propagators inject and extract
telemetry context (trace context, baggage) through HTTP headers in PSR-7 Request and Response objects.

Use cases:

- Distributed tracing across microservices
- Correlation IDs that go through HTTP calls
- Baggage data (key-value pairs) that goes across service boundaries
- Integration with OpenTelemetry-based observability stacks
- Framework-agnostic telemetry integration

> Note: This bridge works with each PSR-7 compliant HTTP message implementation, for example Nyholm/PSR-7, Guzzle or
> Laminas. PSR-7 objects are immutable. After you inject headers, use `unwrap()` on the `ResponseCarrier` to get the
> changed response.

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

The bridge has DSL functions:

```php
use function Flow\Bridge\Psr7\Telemetry\DSL\psr7_request_carrier;
use function Flow\Bridge\Psr7\Telemetry\DSL\psr7_response_carrier;

$requestCarrier = psr7_request_carrier($request);
$responseCarrier = psr7_response_carrier($response);
```
