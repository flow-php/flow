---
package: flow-php/etl-adapter-logger
---

# ETL Adapter: Logger

[PACKAGE_NAV]

[TOC]

ETL Adapter that provides PSR Logger support for ETL.

## Installation

For detailed installation instructions, see the [installation page](/documentation/installation/packages/etl-adapter-logger.md).

## Loader - PsrLogger

Load each row into PsrLoggerInterface implementation. To get `TestLogger` mock class first run:

```
composer require fig/log-test
```

```php
<?php

$logger = new TestLogger();

$loader = new PsrLoggerLoader($logger, 'row log', LogLevel::ERROR);

$loader->load(rows(
    schema(int_schema('id'), str_schema('name')),
    row(['id' => 12345, 'name' => 'norbert']),
));

$this->assertTrue($logger->hasErrorRecords());
$this->assertTrue($logger->hasError('row log'));
```