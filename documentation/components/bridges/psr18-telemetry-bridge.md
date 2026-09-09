---
package: flow-php/psr18-telemetry-bridge
---

# PSR-18 Telemetry Bridge

Flow PSR-18 Telemetry Bridge provides a traceable HTTP client wrapper for PSR-18 compliant HTTP clients, enabling automatic span creation and telemetry context propagation for outgoing HTTP requests.

[PACKAGE_NAV]

[TOC]

## Installation

For detailed installation instructions, see the [installation page](/documentation/installation/packages/psr18-telemetry-bridge.md).

## Overview

This bridge connects Flow PHP's Telemetry library with PSR-18 compliant HTTP clients, providing automatic tracing for outgoing HTTP requests. It wraps any PSR-18 `ClientInterface` implementation to create spans with HTTP request/response metadata.

This is particularly useful for:

- Automatic tracing of outgoing HTTP requests
- Recording HTTP method, URL, status code, and other attributes
- Error tracking for failed requests (4xx/5xx responses)
- Exception recording when HTTP clients throw errors
- Integration with OpenTelemetry-based observability stacks
- Framework-agnostic HTTP client instrumentation

> **Note:** This bridge works with any PSR-18 compliant HTTP client implementation (Guzzle, Symfony HttpClient with PSR-18 adapter, Buzz, etc.).

## Requirements

- PHP 8.3+
- flow-php/telemetry
- psr/http-client ^1.0
- psr/http-message ^1.0 || ^2.0

## Usage

### Wrapping a PSR-18 HTTP Client

```php ignore
use Flow\Bridge\Psr18\Telemetry\PSR18TraceableClient;
use Flow\Telemetry\Telemetry;
use Symfony\Component\HttpClient\Psr18Client;

// Create your telemetry instance
$telemetry = /* ... your Telemetry setup ... */;

// Wrap any PSR-18 client
$httpClient = new Psr18Client();
$traceableClient = new PSR18TraceableClient($httpClient, $telemetry);

// Use it like any other PSR-18 client
$request = new Request('GET', 'https://api.example.com/users');
$response = $traceableClient->sendRequest($request);

// A span is automatically created with:
// - Name: "GET api.example.com"
// - Kind: CLIENT
// - Attributes: http.method, http.url, http.scheme, http.host, http.status_code
// - Status: OK for 2xx/3xx, ERROR for 4xx/5xx
```

### Span Attributes

The traceable client automatically records the following span attributes:

| Attribute | Description | Example |
|-----------|-------------|---------|
| `http.method` | HTTP method | `GET`, `POST` |
| `http.url` | Full request URL | `https://api.example.com/users` |
| `http.scheme` | URL scheme | `https` |
| `http.host` | Target host | `api.example.com` |
| `http.status_code` | Response status code | `200`, `404`, `500` |

### Error Handling

The client handles errors in two ways:

1. **HTTP Error Responses (4xx/5xx)**: The span status is set to ERROR with the message "HTTP {status_code}"
2. **Exceptions**: If the underlying client throws an exception, it is recorded on the span and re-thrown

```php
try {
    $response = $traceableClient->sendRequest($request);
} catch (\Throwable $e) {
    // Exception is recorded on the span with:
    // - exception.type
    // - exception.message
    // - exception.stacktrace
    // The span status is set to ERROR
    throw $e;
}
```

### DSL Functions

The bridge provides a convenient DSL function:

```php
use function Flow\Bridge\Psr18\Telemetry\DSL\psr18_traceable_client;

$traceableClient = psr18_traceable_client($httpClient, $telemetry);
```
