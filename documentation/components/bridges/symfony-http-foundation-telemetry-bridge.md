---
package: flow-php/symfony-http-foundation-telemetry-bridge
---

# Symfony HttpFoundation Telemetry Bridge

This bridge has carriers for propagators. With them, propagators pass and read telemetry context and baggage
through Symfony HttpFoundation request and response objects.

[PACKAGE_NAV]

[TOC]

## Installation

For the installation instructions, read the [installation page](/documentation/installation/packages/symfony-http-foundation-telemetry-bridge.md).

## Overview

This bridge connects the Telemetry library of Flow PHP with the HttpFoundation component of Symfony. HTTP-based
applications can then propagate telemetry context. Its carriers let propagators inject and extract telemetry context
(trace context, baggage) through HTTP headers in Symfony Request and Response objects.

Use cases:

- Distributed tracing across microservices
- Correlation IDs that go through HTTP calls
- Baggage data (key-value pairs) that goes across service boundaries
- Integration with OpenTelemetry-based observability stacks

> Note: This bridge is separate from the `flow-php/symfony-http-foundation-bridge`, which is for ETL operations.
> This bridge is only for telemetry integration. Users who need only telemetry support therefore do not get the full
> ETL framework as a dependency.

## Requirements

- PHP 8.3+
- flow-php/telemetry
- symfony/http-foundation ^6.4 || ^7.3 || ^8.0
