# Symfony Telemetry Bundle

Flow Symfony Telemetry Bundle provides automatic telemetry integration for Symfony applications, including HTTP request/response tracing, console command instrumentation, and configurable exporters.

- [⬅️️ Back](/documentation/introduction.md)
- [📦Packagist](https://packagist.org/packages/flow-php/symfony-telemetry-bundle)
- [🐙GitHub](https://github.com/flow-php/symfony-telemetry-bundle)

[TOC]

## Installation

```
composer require flow-php/symfony-telemetry-bundle:~--FLOW_PHP_VERSION--
```

## Overview

This bundle integrates Flow PHP's Telemetry library with Symfony applications, providing:

- Automatic HTTP request/response span creation
- Console command tracing
- Context propagation via W3C Trace Context and Baggage
- Configurable exporters (console, memory, void, OTLP)
- Full configuration through Symfony's config system

## Requirements

- PHP 8.3+
- flow-php/telemetry
- flow-php/symfony-http-foundation-telemetry-bridge
- symfony/http-kernel ^6.4 || ^7.3 || ^8.0
- symfony/dependency-injection ^6.4 || ^7.3 || ^8.0
- symfony/config ^6.4 || ^7.3 || ^8.0
- symfony/console ^6.4 || ^7.3 || ^8.0

## Configuration

```yaml
# config/packages/flow_telemetry.yaml
flow_telemetry:
    service_name: 'my-app'
    service_version: '1.0.0'
    environment: '%kernel.environment%'

    exporter: 'console'  # console|void|memory|otlp

    tracing:
        enabled: true
        sampler: 'always_on'

    metrics:
        enabled: true

    logging:
        enabled: true

    http:
        enabled: true

    console:
        enabled: true
```
