---
package: flow-php/psr18-telemetry-bridge
---

# PSR-18 Telemetry Bridge

This bridge has a traceable HTTP client wrapper for PSR-18 compliant HTTP clients. It creates spans and propagates telemetry context for outgoing HTTP requests automatically.

[PACKAGE_NAV]

[TOC]

## Installation

For the installation instructions, read the [installation page](/documentation/installation/packages/psr18-telemetry-bridge.md).

## Overview

This bridge connects the Telemetry library of Flow PHP with PSR-18 compliant HTTP clients. It traces outgoing HTTP requests automatically. It wraps each PSR-18 `ClientInterface` implementation and creates spans with HTTP request/response metadata.

Use cases:

- Automatic tracing of outgoing HTTP requests
- Record the HTTP method, URL, status code, and other attributes
- Error tracking for failed requests (4xx/5xx responses)
- Exception records for errors that HTTP clients throw
- Integration with OpenTelemetry-based observability stacks
- Framework-agnostic HTTP client instrumentation

> Note: This bridge works with each PSR-18 compliant HTTP client implementation, for example Guzzle, Symfony HttpClient with the PSR-18 adapter, or Buzz.

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
// - Name: "GET"
// - Kind: CLIENT
// - Attributes: http.request.method, url.full, url.scheme, server.address, server.port, http.response.status_code
// - Status: unset for 1xx-3xx, ERROR for 4xx/5xx
```

### Span Attributes

The traceable client records these span attributes automatically:

| Attribute                   | Description                                               | Example                                      |
|-----------------------------|-----------------------------------------------------------|----------------------------------------------|
| `http.request.method`       | HTTP method                                               | `GET`, `POST`                                |
| `url.full`                  | Full request URL, redacted                                | `https://api.example.com/users?sig=REDACTED` |
| `url.scheme`                | URL scheme                                                | `https`                                      |
| `server.address`            | Target host                                               | `api.example.com`                            |
| `server.port`               | Target port, or the scheme default for a URL without port | `443`                                        |
| `http.response.status_code` | Response status code                                      | `200`, `404`, `500`                          |
| `error.type`                | Status code of a 4xx/5xx response, or the exception class | `404`, `RuntimeException`                    |

In `url.full` and `exception.message`, the userinfo and the values of sensitive query parameters of the URL become
`REDACTED`. For more information, read [URL sanitization](/documentation/components/libs/telemetry.md#url-sanitization):

```php
use Flow\Telemetry\Sanitizer\UrlSanitizer;

use function Flow\Bridge\Psr18\Telemetry\DSL\psr18_traceable_client;
use function Flow\Telemetry\DSL\url_sanitizer;

$traceableClient = psr18_traceable_client(
    $httpClient,
    $telemetry,
    url_sanitizer([...UrlSanitizer::DEFAULT_SENSITIVE_QUERY_PARAMETERS, 'fb_exchange_token']),
);
```

### Error Handling

The client handles errors in two ways:

1. HTTP Error Responses (4xx/5xx): the span status becomes ERROR, with the message "HTTP {status_code}".
2. Exceptions: if the wrapped client throws an exception, the span records it, and the exception is thrown again.

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

The bridge has a DSL function:

```php
use function Flow\Bridge\Psr18\Telemetry\DSL\psr18_traceable_client;

$traceableClient = psr18_traceable_client($httpClient, $telemetry);
```
