# Symfony HttpFoundation Telemetry Bridge

Flow Symfony HttpFoundation Telemetry Bridge provides carriers for propagators that can pass and read telemetry context
and baggage via Symfony HttpFoundation request and response objects.

- [⬅️️ Back](/documentation/introduction.md)
- [📦Packagist](https://packagist.org/packages/flow-php/symfony-http-foundation-telemetry-bridge)
- [➡️ Installation](/documentation/installation/packages/symfony-http-foundation-telemetry-bridge.md)
- [🐙GitHub](https://github.com/flow-php/symfony-http-foundation-telemetry-bridge)

[TOC]

## Installation

For detailed installation instructions, see the [installation page](/documentation/installation/packages/symfony-http-foundation-telemetry-bridge.md).

## Overview

This bridge connects Flow PHP's Telemetry library with Symfony's HttpFoundation component, enabling telemetry context
propagation in HTTP-based applications. It provides carriers that allow propagators to inject and extract telemetry
context (trace context, baggage) through HTTP headers in Symfony Request and Response objects.

This is particularly useful for:

- Distributed tracing across microservices
- Propagating correlation IDs through HTTP calls
- Passing baggage data (key-value pairs) across service boundaries
- Integration with OpenTelemetry-based observability stacks

> **Note:** This bridge is separate from the `flow-php/symfony-http-foundation-bridge` which is designed for ETL
> operations. This bridge focuses specifically on telemetry integration, allowing users who only need telemetry
> support to avoid bringing in the entire ETL framework as a dependency.

## Requirements

- PHP 8.3+
- flow-php/telemetry
- symfony/http-foundation ^6.4 || ^7.3 || ^8.0
