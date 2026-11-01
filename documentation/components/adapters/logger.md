---
package: flow-php/etl-adapter-logger
---

# ETL Adapter: Logger

[PACKAGE_NAV]

[TOC]

An ETL Adapter that adds PSR Logger support to ETL.

## Installation

For the installation instructions, read the [installation page](/documentation/installation/packages/etl-adapter-logger.md).

## Loader - PsrLoggerLoader

Logs each row as one record. The values of the row are the log context. `TestLogger` below comes from
`composer require --dev fig/log-test`.

```php
<?php

use Flow\ETL\Adapter\Logger\IO\Loader\PsrLoggerLoader;
use Psr\Log\LogLevel;
use Psr\Log\Test\TestLogger;

use function Flow\ETL\DSL\{data_frame, from_array};

$logger = new TestLogger();

data_frame()
    ->read(from_array([['id' => 12345, 'name' => 'norbert']]))
    ->write(new PsrLoggerLoader($logger, 'row log', LogLevel::ERROR))
    ->run();

$logger->hasError(['message' => 'row log', 'context' => ['id' => 12345, 'name' => 'norbert']]); // true
```
